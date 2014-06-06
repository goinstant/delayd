package delayd

import (
	"github.com/streadway/amqp"
	"log"
	"time"
)

// Client is the delayd client.  It can relay messages to the server for easy
// testing
type Client struct {
	amqpBase *AmqpBase

	exchange string
	key      string

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
			"delayd-target": c.exchange,
			"delayd-key":    c.key,
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

// Listen is responsible for hearing messages back from the server
func (c *Client) Listen(messages <-chan amqp.Delivery) {
	go func() {
		for {
			// XXX -- It might be worth putting in a shutdown message here in the
			// future, but likely the client will not need to be fail-proof.
			msg := <-messages
			log.Println("got a message", string(msg.Body[:]))
		}
	}()
}

// Run is called to set up amqp options and then relay messages to the server
// over AMQP.
func (c *Client) Run(conf Config) error {
	amqpBase := new(AmqpBase)

	conn, err := amqp.Dial(conf.Amqp.URL)
	if err != nil {
		log.Fatal("Unable to dial AMQP:", err)
	}

	amqpBase.connection = conn
	ch, err := amqpBase.connection.Channel()
	if err != nil {
		log.Fatal("Unable to create amqp channel:", err)
	}

	amqpBase.channel = ch
	c.amqpBase = amqpBase

	exch := conf.Amqp.Exchange
	log.Println("declaring exchange:", c.exchange)
	err = ch.ExchangeDeclare(c.exchange, exch.Kind, exch.Durable, exch.AutoDelete, exch.Internal, exch.NoWait, nil)
	if err != nil {
		log.Fatal("Unable to declare exchange:", err)
	}

	q := conf.Amqp.Queue
	queue, err := ch.QueueDeclare("", q.Durable, q.AutoDelete, q.Exclusive, q.NoWait, nil)
	if err != nil {
		log.Fatal("Unable to declare queue:", err)
	}

	err = ch.QueueBind(queue.Name, "", c.exchange, q.NoWait, nil)
	if err != nil {
		log.Fatal("Unable to bind queue to exchange:", err)
	}

	messages, err := ch.Consume(queue.Name, "delayd", true, q.Exclusive, q.NoLocal, q.NoLocal, nil)

	c.Listen(messages)

	for {
		msg := <-c.stdin
		err := c.send(msg, conf)

		if err != nil {
			log.Println("Got Err:", err)
		}
	}
}
