package main

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

type AmqpReceiver struct {
	C chan struct {
		Entry
		bool
	}
	rawC <-chan amqp.Delivery

	connection *amqp.Connection
	channel    *amqp.Channel
}

func (a AmqpReceiver) Close() {
	a.channel.Close()
}

func NewAmqpReceiver(amqpUrl string, amqpQueue string) (receiver AmqpReceiver, err error) {
	receiver = AmqpReceiver{}

	receiver.connection, err = amqp.Dial(amqpUrl)
	if err != nil {
		log.Println("Could not connect to AMQP: ", err)
		return
	}

	receiver.channel, err = receiver.connection.Channel()
	if err != nil {
		log.Println("Could not open AMQP channel: ", err)
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
	messages, err := receiver.channel.Consume(queue.Name, "vulliamy", true, false, false, false, nil)
	if err != nil {
		log.Println("Could not set up queue consume: ", err)
		return
	}

	receiver.C = make(chan struct {
		Entry
		bool
	})

	go func() {
		for {
			msg, ok := <-messages
			entry := Entry{}

			// XXX cleanup needed here before exit
			if !ok {
				receiver.C <- struct {
					Entry
					bool
				}{entry, ok}
			}

			delay, ok := msg.Headers["vulliamy-delay"].(int64)
			if !ok {
				log.Println("Bad/missing delay. discarding message")
				continue
			}

			entry.SendAt = time.Now().Add(time.Duration(delay) * time.Millisecond)
			entry.Body = msg.Body

			receiver.C <- struct {
				Entry
				bool
			}{entry, ok}
		}
	}()

	return
}
