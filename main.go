package main

import (
	"log"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/streadway/amqp"
)

type AmqpConfig struct {
	Url   string
	Queue string
}

type Config struct {
	Amqp AmqpConfig
}

func main() {
	log.Println("Starting vulliamy")

	var config Config
	if _, err := toml.DecodeFile("vulliamy.toml", &config); err != nil {
		log.Fatal("Unable to read config file: ", err)
	}

	connection, err := amqp.Dial(config.Amqp.Url)
	if err != nil {
		log.Fatal("Could not connect to AMQP: ", err)
	}

	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		log.Fatal("Could not open AMQP channel: ", err)
	}

	queue, err := channel.QueueDeclare(config.Amqp.Queue, true, false, false, false, nil)
	if err != nil {
		log.Fatal("Could not declare AMQP Queue: ", err)
	}

	// XXX set Qos here to match incoming concurrency
	// XXX make the true (autoAck) false, and ack after done.
	messages, err := channel.Consume(queue.Name, "vulliamy", true, false, false, false, nil)
	if err != nil {
		log.Fatal("Could not set up queue consume: ", err)
	}

	log.Println("Connected to AMQP")

	for {
		msg, ok := <-messages
		// XXX cleanup needed here before exit
		if !ok {
			log.Fatal("AMQP Consumption failed!")
		}

		entry := Entry{}

		delay := msg.Headers["vulliamy-delay"].(int64)
		if !ok {
			log.Println("Bad/missing delay. discarding message")
			continue
		}

		entry.SendAt = time.Now().Add(time.Duration(delay) * time.Millisecond)
		entry.Body = msg.Body
		log.Println("Got entry: ", entry)

		go func() {
			timer := time.NewTimer(time.Duration(delay) * time.Millisecond)
			_ = <-timer.C
			log.Println("Sending entry: ", entry)
		}()
	}
}
