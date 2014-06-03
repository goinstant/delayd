package main

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

type Connection interface {
	Close() error
	Channel() (c *amqp.Channel, e error)
}

type Channel interface {
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (q amqp.Queue, e error)
	Consume(name string, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (c <-chan amqp.Delivery, e error)
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
  Close() error
}

type AmqpBase struct {
	connection Connection
	channel    Channel
}

func (a AmqpBase) Close() {
  chanError := a.channel.Close()
  connectionError := a.connection.Close()

  if chanError != nil || connectionError != nil {
    log.Println("Error closing amqp, chanError:", chanError, ", connectionError:", connectionError)
  }
}

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

type AmqpReceiver struct {
	AmqpBase
	C    <-chan Entry
	rawC <-chan amqp.Delivery
}

func NewAmqpReceiver(amqpURL string, amqpQueue string) (receiver *AmqpReceiver, err error) {
	receiver = new(AmqpReceiver)

	err = receiver.dial(amqpURL)
	if err != nil {
		return
	}

	// XXX take queue options from config?
	// XXX declare exchange too?
	queue, err := receiver.channel.QueueDeclare(amqpQueue, true, false, false, false, nil)
	if err != nil {
		log.Println("Could not declare AMQP Queue: ", err)
		return
	}

	//XXX set Qos here to match incoming concurrency
	//XXX make the true (autoAck) false, and ack after done.
	messages, err := receiver.channel.Consume(queue.Name, "delayd", true, false, false, false, nil)
	if err != nil {
		log.Println("Could not set up queue consume: ", err)
		return
	}

	c := make(chan Entry)
	receiver.C = c

	go func() {
		for {
			msg, ok := <-messages
			entry := Entry{}

			// channel was closed. exit
			if !ok {
				return
			}

			delay, ok := msg.Headers["delayd-delay"].(int64)
			if !ok {
				log.Println("Bad/missing delay. discarding message")
				continue
			}
			entry.SendAt = time.Now().Add(time.Duration(delay) * time.Millisecond)

			entry.Target, ok = msg.Headers["delayd-target"].(string)
			if !ok {
				log.Println("Bad/missing target. discarding message")
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

			c <- entry
		}
	}()

	return
}

type AmqpSender struct {
	AmqpBase

	C chan<- Entry
}

func NewAmqpSender(amqpURL string) (sender *AmqpSender, err error) {
	sender = new(AmqpSender)

	err = sender.dial(amqpURL)
	if err != nil {
		return
	}

	// XXX take exchange options from config?
	// XXX declare exchange too?

	return
}

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
