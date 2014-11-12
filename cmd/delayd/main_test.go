package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"

	"github.com/nabeken/delayd"
)

const (
	unknownAsk = "unknown option asked for: "
)

type MockClientContext struct{}

func (m MockClientContext) String(ask string) string {
	switch ask {
	case "target":
		return "delayd-test"
	case "key":
		return "delayd-key"
	case "file":
		return "testdata/in.toml"
	case "out":
		return "testdata/out.txt"
	case "config":
		return "delayd.toml"
	}

	panic(unknownAsk + ask)
}

func (m MockClientContext) Bool(ask string) bool {
	panic(unknownAsk + ask)
}

func (m MockClientContext) Int(ask string) int {
	switch ask {
	case "delay":
		return 50
	}

	panic(unknownAsk + ask)
}

func getFileAsString(path string) string {
	dat, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	return string(dat[:])
}

// TestAMQPClient is the delayd client.
// It can relay messages to the server for easy testing.
type TestAMQPClient struct {
	*delayd.AMQPConsumer

	delaydEx   delayd.AMQPConfig
	deliveryCh <-chan amqp.Delivery
	out        io.Writer
}

// NewClient creates and returns a Client instance
func NewTestClient(targetEx, delaydEx delayd.AMQPConfig, out io.Writer) (*TestAMQPClient, error) {
	a, err := delayd.NewAMQPConsumer(targetEx, "")
	if err != nil {
		return nil, err
	}

	q := targetEx.Queue
	deliveryCh, err := a.Channel.Consume(
		a.Queue.Name,
		"delayd", // consumer
		true,     // autoAck
		q.Exclusive,
		q.NoLocal,
		q.NoWait,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &TestAMQPClient{
		AMQPConsumer: a,

		delaydEx:   delaydEx,
		deliveryCh: deliveryCh,
		out:        out,
	}, nil
}

func (c *TestAMQPClient) SendMessages(msgs []Message) error {
	for _, msg := range msgs {
		pm := amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			ContentType:  "text/plain",
			Headers: amqp.Table{
				"delayd-delay":  msg.Delay,
				"delayd-target": c.Config.Exchange.Name,
				"delayd-key":    msg.Key,
			},
			Body: []byte(msg.Value),
		}
		if err := c.Channel.Publish(
			c.delaydEx.Exchange.Name,
			c.delaydEx.Queue.Name,
			true,  // mandatory
			false, // immediate
			pm,
		); err != nil {
			return err
		}
	}
	return nil
}

// messageLoop processes messages reading from deliveryCh until quit is closed.
// it also send a notification via done channel when processing is done.
func (c *TestAMQPClient) RecvLoop(quit <-chan struct{}) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-quit:
				delayd.Info("receiving quit. existing.")
				return
			case msg := <-c.deliveryCh:
				fmt.Fprintf(c.out, "%s\n", msg.Body)
				delayd.Infof("client: written %s", string(msg.Body))
				done <- struct{}{}
			}
		}
	}()
	return done
}

func loadMessages(path string) (msgs []Message, err error) {
	messages := struct {
		Message []Message
	}{}
	delayd.Debug("reading", path)
	_, err = toml.DecodeFile(path, &messages)
	return messages.Message, err
}

// Message holds a message to send to delayd server
type Message struct {
	Value string
	Key   string
	Delay int64
}

func TestInAndOut(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test")
	}

	assert := assert.New(t)

	m := MockClientContext{}
	conf, err := loadConfig(m.String("config"))

	// create an ephemeral location for data storage during tests
	conf.DataDir, err = ioutil.TempDir("", "delayd-testint")
	assert.NoError(err)
	defer os.Remove(conf.DataDir)

	// Use stdout instead of file
	conf.LogDir = ""

	s := &delayd.Server{}
	go s.Run(conf)
	defer s.Stop()

	out, err := os.OpenFile(m.String("out"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	targetCfn := conf.AMQP
	targetCfn.Queue.Name = ""
	targetCfn.Queue.Bind = []string{m.String("target")}
	targetCfn.Exchange.Name = m.String("target")
	c, err := NewTestClient(targetCfn, conf.AMQP, out)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Send messages to delayd exchange
	msgs, err := loadMessages("testdata/in.toml")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SendMessages(msgs); err != nil {
		t.Fatal(err)
	}

	quit := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(msgs))
	go func() {
		for _ = range c.RecvLoop(quit) {
			wg.Done()
		}
	}()

	// Wait for messages to be processed
	wg.Wait()

	// shutdown recvloop
	close(quit)

	// remove all whitespace for a more reliable compare
	f1 := strings.Trim(getFileAsString("testdata/expected.txt"), "\n ")
	f2 := strings.Trim(getFileAsString("testdata/out.txt"), "\n ")

	assert.Equal(f1, f2)
	if err := os.Remove("testdata/out.txt"); err != nil {
		t.Fatal(t)
	}
}
