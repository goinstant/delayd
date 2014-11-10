package delayd

import (
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const consumerTag = "delayd-"

// AMQP manages amqp a connection and channel.
type AMQP struct {
	Channel    *amqp.Channel
	Connection *amqp.Connection
}

// Close the connection to amqp gracefully. A caller should ensure they finish
// all in-flight processing.
func (a *AMQP) Close() {
	a.Channel.Close()
	a.Connection.Close()
}

// Dial connects to , and open a communication channel.
func (a *AMQP) Dial(url string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		Error("Could not connect to : ", err)
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		Error("Could not open  channel: ", err)
		return err
	}
	a.Connection, a.Channel = conn, ch
	return nil
}

// AMQPReceiver receives delayd commands over amqp
type AMQPReceiver struct {
	AMQP

	c            chan Message
	shutdown     chan bool
	metaMessages chan (<-chan amqp.Delivery)
	messages     chan amqp.Delivery
	paused       bool
	tagCount     uint
	ac           AMQPConfig
	mu           sync.Mutex
}

// NewAMQPReceiver creates a new Receiver based on the provided Config,
// and starts it listening for commands.
func NewAMQPReceiver(ac AMQPConfig) (*AMQPReceiver, error) {
	receiver := &AMQPReceiver{
		c: make(chan Message),

		messages:     make(chan amqp.Delivery, ac.Qos),
		metaMessages: make(chan (<-chan amqp.Delivery)),
		ac:           ac,
		paused:       true,
		shutdown:     make(chan bool),
	}

	if err := receiver.Dial(ac.URL); err != nil {
		return nil, err
	}

	Debug("Setting channel QoS to", ac.Qos)
	if err := receiver.Channel.Qos(ac.Qos, 0, false); err != nil {
		return nil, err
	}

	if err := receiver.Channel.ExchangeDeclare(
		ac.Exchange.Name,
		ac.Exchange.Kind,
		ac.Exchange.Durable,
		ac.Exchange.AutoDelete,
		ac.Exchange.Internal,
		ac.Exchange.NoWait,
		nil,
	); err != nil {
		Error("Could not declare  Exchange: ", err)
		return nil, err
	}

	queue, err := receiver.Channel.QueueDeclare(
		ac.Queue.Name,
		ac.Queue.Durable,
		ac.Queue.AutoDelete,
		ac.Queue.Exclusive,
		ac.Queue.NoWait,
		nil,
	)
	if err != nil {
		Error("Could not declare  Queue: ", err)
		return nil, err
	}

	for _, exch := range ac.Queue.Bind {
		Debugf("Binding queue %s to exchange %s", queue.Name, exch)
		if err := receiver.Channel.QueueBind(queue.Name, "delayd", exch, ac.Queue.NoWait, nil); err != nil {
			// XXX un-fatal this like the other errors
			Fatalf("Error binding queue %s to Exchange %s", queue.Name, exch)
		}
	}

	go receiver.monitorChannel()
	go receiver.messageLoop()

	return receiver, nil
}

// monitorChannel monitors metaMessages channel.
// It installs new channel when metaMessages channel returns a channel
func (a *AMQPReceiver) monitorChannel() {
	var realMessages <-chan amqp.Delivery
	for {
		select {
		case <-a.shutdown:
			Debug("received signal to quit reading amqp, exiting goroutine")
			return
		case m := <-a.metaMessages:
			// we have a new source of 'real' messages. swap it in.
			Debug("Installing new amqp channel")
			realMessages = m
		case msg, ok := <-realMessages:
			// Don't propagate any channel close messages
			if ok {
				a.messages <- msg
			}
		}
	}
}

func (a *AMQPReceiver) messageLoop() {
	for {
		select {
		case <-a.shutdown:
			Debug("received signal to quit reading amqp, exiting goroutine")
			return
		case delivery := <-a.messages:
			deliverer := &AMQPDeliverer{Delivery: delivery}

			var delay int64
			switch val := delivery.Headers["delayd-delay"].(type) {
			case int32:
				delay = int64(val)
			case int64:
				delay = val
			default:
				Warn(delivery)
				Warn("Bad/missing delay. discarding message")
				deliverer.Ack()
			}

			entry := Entry{
				SendAt: time.Now().Add(time.Duration(delay) * time.Millisecond),
			}

			target, found := delivery.Headers["delayd-target"].(string)
			if !found {
				Warn("Bad/missing target. discarding message")
				deliverer.Ack()
				continue
			}
			entry.Target = target

			// optional key value for overwrite
			if k, ok := delivery.Headers["delayd-key"].(string); ok {
				entry.Key = k
			}

			// optional headers that will be relayed
			entry.AMQP = &AMQPMessage{
				ContentType:     delivery.ContentType,
				ContentEncoding: delivery.ContentEncoding,
				CorrelationID:   delivery.CorrelationId,
			}

			entry.Body = delivery.Body
			msg := Message{
				Entry: entry,
				MessageDeliverer: &AMQPDeliverer{
					Delivery: delivery,
				},
			}
			a.c <- msg
		}
	}
}

// MessageCh returns a receiving-only channel returns Message
func (a *AMQPReceiver) MessageCh() <-chan Message {
	return a.c
}

// Close pauses the receiver and signals the shutdown channel.
// Finally, it closes AMQP the channel and connection.
func (a *AMQPReceiver) Close() {
	a.Pause()
	a.shutdown <- true
	a.AMQP.Close()
}

// Start or restart listening for messages on the queue
func (a *AMQPReceiver) Start() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.paused {
		// FIXME: already started?
		return nil
	}

	m, err := a.Channel.Consume(
		a.ac.Queue.Name,
		consumerTag+string(a.tagCount),
		a.ac.Queue.AutoAck,
		a.ac.Queue.Exclusive,
		a.ac.Queue.NoLocal,
		a.ac.Queue.NoWait,
		nil,
	)
	if err != nil {
		return err
	}

	a.paused = false
	a.tagCount++

	// Install new channel
	a.metaMessages <- m
	return err
}

// Pause listening for messages on the queue
func (a *AMQPReceiver) Pause() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.paused {
		return nil
	}
	a.paused = true
	return a.Channel.Cancel(consumerTag+string(a.tagCount), false)
}

// AMQPSender sends delayd entries over amqp after their timeout
type AMQPSender struct {
	AMQP
}

// NewAMQPSender creates a new Sender connected to the given  URL.
func NewAMQPSender(amqpURL string) (*AMQPSender, error) {
	sender := &AMQPSender{}
	err := sender.Dial(amqpURL)
	if err != nil {
		return nil, err
	}

	return sender, nil
}

// Send sends a delayd entry over , using the entry's Target as the publish
// exchange.
func (s *AMQPSender) Send(e Entry) error {
	msg := amqp.Publishing{
		DeliveryMode:    amqp.Persistent,
		Timestamp:       time.Now(),
		ContentType:     e.AMQP.ContentType,
		ContentEncoding: e.AMQP.ContentEncoding,
		CorrelationId:   e.AMQP.CorrelationID,
		Body:            e.Body,
	}
	return s.Channel.Publish(e.Target, "", true, false, msg)
}
