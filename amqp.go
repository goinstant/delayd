package main

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

type amqpBase struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

func (a amqpBase) Close() {
	a.connection.Close()
}

func (a *amqpBase) dial(amqpURL string) (err error) {
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
	amqpBase
	C    <-chan Entry
	rawC <-chan amqp.Delivery
}

func NewAmqpReceiver(amqpURL string, amqpQueue string) (receiver AmqpReceiver, err error) {
	receiver = AmqpReceiver{}

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

			// XXX cleanup needed here before exit
			if !ok {
				close(c)
			}

			delay, ok := msg.Headers["delayd-delay"].(int64)
			if !ok {
				log.Println("Bad/missing delay. discarding message")
				continue
			}

			entry.SendAt = time.Now().Add(time.Duration(delay) * time.Millisecond)
			entry.Body = msg.Body

			c <- entry
		}
	}()

	return
}

type AmqpSender struct {
	amqpBase

	C chan<- Entry
}

func NewAmqpSender(amqpURL string, amqpExchange string) (sender AmqpSender, err error) {
	sender = AmqpSender{}

	err = sender.dial(amqpURL)
	if err != nil {
		return
	}

	// XXX take exchange options from config?
	// XXX declare exchange too?

	c := make(chan Entry)
	sender.C = c

	go func() {
		for {
			entry := <-c

			msg := amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Now(),
				ContentType:  "text/plain",
				Body:         entry.Body,
			}

			err = sender.channel.Publish(amqpExchange, "delayd", true, false, msg)
			if err != nil {
				// XXX proper cleanup
				log.Fatal("publish failed: ", err)
			}
		}
	}()

	return
}
