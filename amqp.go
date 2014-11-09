package main

import (
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const consumerTag = "delayd-"

// Shutdown is a base class that is inheritable by any other class that may want
// to send a shutdown signal over a channel
type Shutdown struct {
	shutdown chan bool
}

// AMQPBase is the base class for amqp senders and recievers, containing the
// startup and shutdown logic.
type AMQPBase struct {
	Shutdown

	connection *amqp.Connection
	channel    *amqp.Channel
}

// Close the connection to amqp gracefully. Subclasses should ensure they finish
// all in-flight processing.
func (a AMQPBase) Close() {
	a.channel.Close()
	a.connection.Close()
}

// Connect to AMQP, and open a communication channel.
func (a *AMQPBase) dial(amqpURL string) (err error) {
	a.connection, err = amqp.Dial(amqpURL)
	if err != nil {
		Error("Could not connect to AMQP: ", err)
		return
	}

	a.channel, err = a.connection.Channel()
	if err != nil {
		Error("Could not open AMQP channel: ", err)
		return
	}

	return
}

// AMQPReceiver receives delayd commands over amqp
type AMQPReceiver struct {
	AMQPBase
	C            <-chan EntryWrapper
	metaMessages chan (<-chan amqp.Delivery)
	paused       bool

	tagCount uint

	ac AMQPConfig
	m  *sync.Mutex
}

// Close overwrites the base class close because we need to do some additional
// work for the receiver (signalling the shutdown channel)
func (a AMQPReceiver) Close() {
	a.Pause()
	a.shutdown <- true
	a.AMQPBase.Close()
}

// NewAMQPReceiver creates a new AMQPReceiver based on the provided AMQPConfig,
// and starts it listening for commands.
func NewAMQPReceiver(ac AMQPConfig) (receiver *AMQPReceiver, err error) {
	receiver = new(AMQPReceiver)
	receiver.ac = ac
	receiver.paused = true
	receiver.tagCount = 0
	receiver.m = new(sync.Mutex)

	err = receiver.dial(ac.URL)
	if err != nil {
		return
	}

	Debug("Setting channel QoS to", ac.Qos)
	err = receiver.channel.Qos(ac.Qos, 0, false)
	if err != nil {
		return
	}

	err = receiver.channel.ExchangeDeclare(ac.Exchange.Name, ac.Exchange.Kind, ac.Exchange.Durable, ac.Exchange.AutoDelete, ac.Exchange.Internal, ac.Exchange.NoWait, nil)
	if err != nil {
		Error("Could not declare AMQP Exchange: ", err)
		return
	}

	queue, err := receiver.channel.QueueDeclare(ac.Queue.Name, ac.Queue.Durable, ac.Queue.AutoDelete, ac.Queue.Exclusive, ac.Queue.NoWait, nil)
	if err != nil {
		Error("Could not declare AMQP Queue: ", err)
		return
	}

	for _, exch := range ac.Queue.Bind {
		Debugf("Binding queue %s to exchange %s", queue.Name, exch)
		err = receiver.channel.QueueBind(queue.Name, "delayd", exch, ac.Queue.NoWait, nil)
		if err != nil {
			// XXX un-fatal this like the other errors
			Fatalf("Error binding queue %s to Exchange %s", queue.Name, exch)
		}
	}

	c := make(chan EntryWrapper)
	receiver.C = c
	receiver.shutdown = make(chan bool)

	// messages is a proxy that wraps the 'real' channel, which will be closed
	// and opened on pause/resume
	messages := make(chan amqp.Delivery, ac.Qos)
	receiver.metaMessages = make(chan (<-chan amqp.Delivery))
	go func() {
		var realMessages <-chan amqp.Delivery
		for {
			select {
			case m := <-receiver.metaMessages:
				// we have a new source of 'real' messages. swap it in.
				Debug("Installing new amqp channel")
				realMessages = m
			case msg, ok := <-realMessages:
				// Don't propagate any channel close messages
				if ok {
					messages <- msg
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-receiver.shutdown:
				Debug("received signal to quit reading amqp, exiting goroutine")
				return
			case msg := <-messages:
				entry := Entry{}

				eWrapper := EntryWrapper{Msg: msg}

				var delay int64
				switch val := msg.Headers["delayd-delay"].(type) {
				case int32:
					delay = int64(val)
				case int64:
					delay = val
				default:
					Warn(msg)
					Warn("Bad/missing delay. discarding message")
					eWrapper.Done(true)
				}
				entry.SendAt = time.Now().Add(time.Duration(delay) * time.Millisecond)

				var ok bool
				entry.Target, ok = msg.Headers["delayd-target"].(string)
				if !ok {
					Warn("Bad/missing target. discarding message")
					eWrapper.Done(true)
					continue
				}

				// optional key value for overwrite
				h, ok := msg.Headers["delayd-key"].(string)
				if ok {
					entry.Key = h
				}

				// optional headers that will be relayed
				entry.ContentType = msg.ContentType
				entry.ContentEncoding = msg.ContentEncoding
				entry.CorrelationID = msg.CorrelationId

				entry.Body = msg.Body
				eWrapper.Entry = entry

				c <- eWrapper
			}
		}
	}()

	return
}

// Start or restart listening for messages on the queue
func (a AMQPReceiver) Start() (err error) {
	a.m.Lock()
	defer a.m.Unlock()
	if !a.paused {
		return
	}

	a.paused = false
	a.tagCount++
	m, err := a.channel.Consume(a.ac.Queue.Name, consumerTag+string(a.tagCount), a.ac.Queue.AutoAck, a.ac.Queue.Exclusive, a.ac.Queue.NoLocal, a.ac.Queue.NoWait, nil)
	a.metaMessages <- m
	return
}

// Pause listening for messages on the queue
func (a AMQPReceiver) Pause() error {
	a.m.Lock()
	defer a.m.Unlock()
	if a.paused {
		return nil
	}
	a.paused = true
	return a.channel.Cancel(consumerTag+string(a.tagCount), false)
}

// AMQPSender sends delayd entries over amqp after their timeout
type AMQPSender struct {
	AMQPBase

	C chan<- EntryWrapper
}

// NewAMQPSender creates a new AMQPSender connected to the given AMQP URL.
func NewAMQPSender(amqpURL string) (sender *AMQPSender, err error) {
	sender = new(AMQPSender)

	err = sender.dial(amqpURL)
	if err != nil {
		return
	}

	return
}

// Send sends a delayd entry over AMQP, using the entry's Target as the publish
// exchange.
func (s AMQPSender) Send(e Entry) (err error) {
	msg := amqp.Publishing{
		DeliveryMode:    amqp.Persistent,
		Timestamp:       time.Now(),
		ContentType:     e.ContentType,
		ContentEncoding: e.ContentEncoding,
		CorrelationId:   e.CorrelationID,
		Body:            e.Body,
	}

	err = s.channel.Publish(e.Target, "", true, false, msg)
	return
}
