package main

import (
	"bufio"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/codegangsta/cli"
	"github.com/streadway/amqp"
)

// Client is the delayd client.  It can relay messages to the server for easy
// testing
type Client struct {
	Shutdown
	amqpBase *AmqpBase

	exchange       string
	key            string
	file           string
	outFile        string
	delay          int64
	repl           bool
	outFileDefined bool
	noWait         bool
	lock           sync.WaitGroup

	stdin chan []byte
}

// NewClient creates and returns a Client instance
func NewClient(c *cli.Context) (cli *Client, err error) {
	cli = new(Client)
	cli.exchange = c.String("exchange")
	cli.key = c.String("key")
	cli.repl = c.Bool("repl")
	cli.delay = int64(c.Int("delay"))
	cli.file = c.String("file")
	cli.outFile = c.String("out")
	cli.outFileDefined = cli.outFile != ""
	cli.shutdown = make(chan bool)
	cli.noWait = c.Bool("no-wait")

	cli.stdin = make(chan []byte)

	return cli, nil
}

func (c *Client) send(msg []byte, conf Config) error {
	log.Println("SENDING:", string(msg[:]))

	pub := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Headers: amqp.Table{
			"delayd-delay":  c.delay,
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

	c.lock.Add(1)
	log.Println("SENT")

	return nil
}

func (c *Client) listenInput() {
	if c.repl {
		log.Println("Waiting for STDIN")
		bio := bufio.NewReader(os.Stdin)
		for {
			line, err := bio.ReadBytes('\n')
			if err != nil {
				log.Fatal("Error reading from STDIN")
			}

			c.stdin <- line
		}
	}

	f := c.file
	dat, err := ioutil.ReadFile(f)
	if err != nil {
		log.Fatalf("Error reading from file: %s, got error: %s", f, err)
	}

	c.stdin <- dat
	c.shutdown <- true
}

func (c *Client) listenResponse(messages <-chan amqp.Delivery) {
	for {
		// XXX -- It might be worth putting in a shutdown message here in the
		// future, but likely the client will not need to be fail-proof.
		msg := <-messages
		if c.outFileDefined {
			f, err := os.OpenFile(c.outFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				panic(err)
			}

			defer f.Close()
			_, err = f.Write(msg.Body)
			if err != nil {
				panic(err)
			}

			log.Println("Wrote response to", c.outFile)
		} else {
			log.Println("MSG RECEIVED:", string(msg.Body[:]))
		}

		defer func() {
			// XXX We are seeing panics from the waitgroup lock value going below 0.
			// This shouldn't happen.  Channel is receiving two messages
			// (the second message is always empty) .. null byte from EOF read?
			_ = recover()
		}()

		c.lock.Done()
	}
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

	go c.listenInput()
	go c.listenResponse(messages)

	for {
		select {
		case <-c.shutdown:
			return nil
		case msg := <-c.stdin:
			err := c.send(msg, conf)

			if err != nil {
				log.Println("Got Err:", err)
			}
		}
	}
}

// Stop is resonsible for shutting down all services used by the Client
func (c *Client) Stop() {
	log.Println("Shutting down gracefully")
	log.Println("waiting for AMQP responses to arrive")

	// this is a double negative which sucks, but it makes more sense from the cli
	// point of view to specifiy --no-wait when you don't want to wait, rather than
	// defaulting to not waiting
	if !c.noWait {
		c.lock.Wait()
	}

	c.amqpBase.Close()
}
