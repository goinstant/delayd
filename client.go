package delayd

import (
	"bufio"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/codegangsta/cli"
	"github.com/streadway/amqp"
)

// Client is the delayd client.  It can relay messages to the server for easy
// testing
type Client struct {
	amqpBase *AmqpBase

	exchange string
	key      string
	file     string
	delay    int64
	repl     bool

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
}

func (c *Client) listenResponse(messages <-chan amqp.Delivery) {
	for {
		// XXX -- It might be worth putting in a shutdown message here in the
		// future, but likely the client will not need to be fail-proof.
		msg := <-messages
		log.Println("got a message", string(msg.Body[:]))
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
		msg := <-c.stdin
		err := c.send(msg, conf)

		if err != nil {
			log.Println("Got Err:", err)
		}
	}
}

// Stop is resonsible for shutting down all services used by the Client
func (c *Client) Stop() {
	log.Println("Shutting down gracefully.")
	c.amqpBase.Close()
}
