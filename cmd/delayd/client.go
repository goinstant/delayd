package main

import (
	"bufio"
	"os"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/streadway/amqp"

	"github.com/nabeken/delayd"
)

const (
	delay int = iota
	exchange
	key
)

// AMQPClient is the delayd client.  It can relay messages to the server for easy
// testing
type AMQPClient struct {
	AMQP *delayd.AMQP

	shutdown       chan bool
	exchange       string
	key            string
	file           string
	outFile        string
	delay          int64
	repl           bool
	outFileDefined bool
	noWait         bool
	lock           sync.WaitGroup
	stdin          chan ClientMessages
}

// NewClient creates and returns a Client instance
func NewClient(c Context) (cli *AMQPClient, err error) {

	cli = new(AMQPClient)
	cli.exchange = c.String("exchange")
	cli.key = c.String("key")
	cli.repl = c.Bool("repl")
	cli.delay = int64(c.Int("delay"))
	cli.file = c.String("file")
	cli.outFile = c.String("out")
	cli.outFileDefined = cli.outFile != ""
	cli.shutdown = make(chan bool)
	cli.noWait = c.Bool("no-wait")

	cli.stdin = make(chan ClientMessages)

	return cli, nil
}

func (c *AMQPClient) send(msgs ClientMessages, conf delayd.Config, params ...int) error {

	for _, msg := range msgs.Message {
		delayd.Debug("SENDING:", msg.Value)

		pub := amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			ContentType:  "text/plain",
			Headers: amqp.Table{
				"delayd-delay":  msg.Delay,
				"delayd-target": c.exchange,
				"delayd-key":    msg.Key,
			},
			Body: []byte(msg.Value),
		}

		exchange := conf.AMQP.Exchange.Name
		queue := conf.AMQP.Queue.Name
		err := c.AMQP.Channel.Publish(exchange, queue, true, false, pub)
		if err != nil {
			return err
		}

		c.lock.Add(1)
	}

	delayd.Debug("ALL MESSAGES SENT")

	return nil
}

func (c *AMQPClient) listenInput() {
	if c.repl {
		delayd.Info("Waiting for STDIN")
		bio := bufio.NewReader(os.Stdin)
		for {
			line, err := bio.ReadBytes('\n')
			if err != nil {
				delayd.Fatal("Error reading from STDIN")
			}

			cliMessages := ClientMessages{
				Message: []Message{
					{
						Value: string(line[:]),
						Key:   c.key,
						Delay: c.delay,
					},
				},
			}

			c.stdin <- cliMessages
		}
	}

	cliMessages, err := loadMessages(c.file)
	if err != nil {
		delayd.Fatalf("Error reading from file: %s, got error: %s", c.file, err)
	}

	c.stdin <- cliMessages
	c.shutdown <- true
}

func (c *AMQPClient) listenResponse(messages <-chan amqp.Delivery) {
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
			// concat a newline for readability
			_, err = f.Write(append(msg.Body, []byte("\n")...))
			if err != nil {
				panic(err)
			}

			delayd.Info("Wrote response to", c.outFile)
		} else {
			delayd.Info("MSG RECEIVED:", string(msg.Body[:]))
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
func (c *AMQPClient) Run(conf delayd.Config) error {
	c.AMQP = &delayd.AMQP{}
	if err := c.AMQP.Dial(conf.AMQP.URL); err != nil {
		delayd.Fatal("Unable to dial AMQP:", err)
	}

	exch := conf.AMQP.Exchange
	delayd.Debug("declaring exchange:", c.exchange)
	err := c.AMQP.Channel.ExchangeDeclare(c.exchange, exch.Kind, exch.Durable, exch.AutoDelete, exch.Internal, exch.NoWait, nil)
	if err != nil {
		delayd.Fatal("Unable to declare exchange:", err)
	}

	q := conf.AMQP.Queue
	queue, err := c.AMQP.Channel.QueueDeclare("", q.Durable, q.AutoDelete, q.Exclusive, q.NoWait, nil)
	if err != nil {
		delayd.Fatal("Unable to declare queue:", err)
	}

	err = c.AMQP.Channel.QueueBind(queue.Name, "", c.exchange, q.NoWait, nil)
	if err != nil {
		delayd.Fatal("Unable to bind queue to exchange:", err)
	}

	messages, err := c.AMQP.Channel.Consume(queue.Name, "delayd", true, q.Exclusive, q.NoLocal, q.NoLocal, nil)

	go c.listenInput()
	go c.listenResponse(messages)

	for {
		select {
		case <-c.shutdown:
			return nil
		case msg := <-c.stdin:
			err := c.send(msg, conf)

			if err != nil {
				delayd.Warn("Got Err:", err)
			}
		}
	}
}

// Stop is resonsible for shutting down all services used by the Client
func (c *AMQPClient) Stop() {
	delayd.Info("Shutting down gracefully")
	delayd.Info("waiting for AMQP responses to arrive")

	// this is a double negative which sucks, but it makes more sense from the cli
	// point of view to specifiy --no-wait when you don't want to wait, rather than
	// defaulting to not waiting
	if !c.noWait {
		c.lock.Wait()
	}

	c.AMQP.Close()
}

func loadMessages(path string) (config ClientMessages, err error) {
	delayd.Debug("reading", path)
	_, err = toml.DecodeFile(path, &config)
	return config, err
}

// Message holds a message to send to delayd server
type Message struct {
	Value string
	Key   string
	Delay int64
}

// ClientMessages holds delayd client config options
type ClientMessages struct {
	Message []Message
}
