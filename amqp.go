package main

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

// Shutdown is a base class that is inheritable by any other class that may want
// to send a shutdown signal over a channel
type Shutdown struct {
	shutdown chan bool
}

// AmqpBase is the base class for amqp senders and recievers, containing the
// startup and shutdown logic.
type AmqpBase struct {
	Shutdown

	connection *amqp.Connection
	channel    *amqp.Channel
}

// Close the connection to amqp gracefully. Subclasses should ensure they finish
// all in-flight processing.
func (a AmqpBase) Close() {
	a.channel.Close()
	a.connection.Close()
}

// Connect to AMQP, and open a communication channel.
func (a *AmqpBase) dial(amqpURL string) (err error) {
	a.connection, err = amqp.Dial(amqpURL)
	if err != nil {
		log.Println("Could not connect to AMQP: ", err)
		return
	}

	a.channel, err = a.connection.Channel()
	if err != nil {
		log.Println("Could not open AMQP channel: ", err)
		return
	}

	return
}

// AmqpReceiver receives delayd commands over amqp
type AmqpReceiver struct {
	AmqpBase
	C    <-chan EntryWrapper
	rawC <-chan amqp.Delivery
}

// Close overwrites the base class close because we need to do some additional
// work for the receiver (signalling the shutdown channel)
func (a AmqpReceiver) Close() {
	a.shutdown <- true
	a.AmqpBase.Close()
}

// NewAmqpReceiver creates a new AmqpReceiver based on the provided AmqpConfig,
// and starts it listening for commands.
func NewAmqpReceiver(ac AmqpConfig) (receiver *AmqpReceiver, err error) {
	receiver = new(AmqpReceiver)

	err = receiver.dial(ac.URL)
	if err != nil {
		return
	}

	err = receiver.channel.ExchangeDeclare(ac.Exchange.Name, ac.Exchange.Kind, ac.Exchange.Durable, ac.Exchange.AutoDelete, ac.Exchange.Internal, ac.Exchange.NoWait, nil)
	if err != nil {
		log.Println("Could not declare AMQP Exchange: ", err)
		return
	}

	queue, err := receiver.channel.QueueDeclare(ac.Queue.Name, ac.Queue.Durable, ac.Queue.AutoDelete, ac.Queue.Exclusive, ac.Queue.NoWait, nil)
	if err != nil {
		log.Println("Could not declare AMQP Queue: ", err)
		return
	}

	for _, exch := range ac.Queue.Bind {
		log.Printf("Binding queue %s to exchange %s", queue.Name, exch)
		err = receiver.channel.QueueBind(queue.Name, "delayd", exch, ac.Queue.NoWait, nil)
		if err != nil {
			log.Fatalf("Error binding queue %s to Exchange %s", queue.Name, exch)
		}
	}

	//XXX set Qos here to match incoming concurrency
	messages, err := receiver.channel.Consume(queue.Name, "delayd", ac.Queue.AutoAck, ac.Queue.Exclusive, ac.Queue.NoLocal, ac.Queue.NoWait, nil)
	if err != nil {
		log.Println("Could not set up queue consume: ", err)
		return
	}

	c := make(chan EntryWrapper)
	receiver.C = c
	receiver.shutdown = make(chan bool)

	go func() {
		for {
			select {
			case _ = <-receiver.shutdown:
				log.Println("received signal to quit reading amqp, exiting goroutine")
				return
			case msg, ok := <-messages:
				entry := Entry{}

				eWrapper := EntryWrapper{Msg: msg}

				// channel was closed. exit
				if !ok {
					return
				}

				delay, ok := msg.Headers["delayd-delay"].(int64)
				if !ok {
					log.Println("Bad/missing delay. discarding message")
					eWrapper.Done(false)
					continue
				}
				entry.SendAt = time.Now().Add(time.Duration(delay) * time.Millisecond)

				entry.Target, ok = msg.Headers["delayd-target"].(string)
				if !ok {
					log.Println("Bad/missing target. discarding message")
					eWrapper.Done(false)
					continue
				}

				// optional key value for overwrite
				h, ok := msg.Headers["delayd-key"].(string)
				if ok {
					entry.Key = h
				}

				// optional headers that will be relayed
				h, ok = msg.Headers["content-type"].(string)
				if ok {
					entry.ContentType = h
				}

				h, ok = msg.Headers["content-encoding"].(string)
				if ok {
					entry.ContentEncoding = h
				}

				h, ok = msg.Headers["correlation-id"].(string)
				if ok {
					entry.CorrelationID = h
				}

				entry.Body = msg.Body
				eWrapper.Entry = entry

				c <- eWrapper
			}
		}
	}()

	return
}

// AmqpSender sends delayd entries over amqp after their timeout
type AmqpSender struct {
	AmqpBase

	C chan<- EntryWrapper
}

// NewAmqpSender creates a new AmqpSender connected to the given AMQP URL.
func NewAmqpSender(amqpURL string) (sender *AmqpSender, err error) {
	sender = new(AmqpSender)

	err = sender.dial(amqpURL)
	if err != nil {
		return
	}

	return
}

// Send sends a delayd entry over AMQP, using the entry's Target as the publish
// exchange.
func (s AmqpSender) Send(e Entry) (err error) {
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
