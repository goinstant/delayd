package main

import (
	"github.com/streadway/amqp"
	"log"
	"time"
)

// Client is the delayd client.  It can relay messages to the server for easy
// testing
type Client struct {
	amqpBase *AmqpBase

	// XXX -- for response
	// messages <- chan amqp.Delivery

	stdin chan []byte
}

func (c *Client) send(msg []byte, conf Config) error {
	log.Println("SENDING:", string(msg[:]))

	pub := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Headers: amqp.Table{
			"delayd-delay":  int64(4000),
			"delayd-target": "TODO",
			"delayd-key":    "TODO",
		},
		Body: msg,
	}

	exchange := conf.Amqp.Exchange.Name
	queue := conf.Amqp.Queue.Name
	err := c.amqpBase.channel.Publish(exchange, queue, true, false, pub)
	if err != nil {
		return err
	}

	log.Println("SENT")

	return nil
}

// Run is called to set up amqp options and then relay messages to the server
// over AMQP.
func (c *Client) Run(conf Config) error {
	amqpBase := new(AmqpBase)

	conn, err := amqp.Dial(conf.Amqp.URL)
	if err != nil {
		return nil
	}

	amqpBase.connection = conn
	ch, err := amqpBase.connection.Channel()
	if err != nil {
		return nil
	}

	amqpBase.channel = ch
	c.amqpBase = amqpBase
	for {
		msg := <-c.stdin
		err := c.send(msg, conf)

		if err != nil {
			log.Println("Got Err:", err)
		}
	}
}
