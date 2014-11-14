package delayd

import (
	"errors"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const consumerTag = "delayd-"

// AMQP manages amqp a connection and channel.
type AMQP struct {
	Channel    *amqp.Channel
	Connection *amqp.Connection
}

// NewAMQP connects to url and opens a communication channel and  it returns
// initialized AMQP instance.
func NewAMQP(url string) (*AMQP, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &AMQP{
		Connection: conn,
		Channel:    ch,
	}, nil
}

// Close the connection to amqp gracefully. A caller should ensure they finish
// all in-flight processing.
func (a *AMQP) Close() {
	a.Channel.Close()
	a.Connection.Close()
}

// AMQPConsumer represents general AMQP consumer.
type AMQPConsumer struct {
	*AMQP

	Config AMQPConfig
	Queue  amqp.Queue
}

// NewAMQPConsumer creates a consumer for AMQP and returns a AMQPConsumer instance.
func NewAMQPConsumer(config AMQPConfig, rk string) (*AMQPConsumer, error) {
	a, err := NewAMQP(config.URL)
	if err != nil {
		return nil, err
	}

	Debug("amqp: setting channel QoS to", config.Qos)
	if err := a.Channel.Qos(config.Qos, 0, false); err != nil {
		return nil, err
	}

	e := config.Exchange
	if err := a.Channel.ExchangeDeclare(
		e.Name,
		e.Kind,
		e.Durable,
		e.AutoDelete,
		e.Internal,
		e.NoWait,
		nil,
	); err != nil {
		return nil, err
	}

	q := config.Queue
	queue, err := a.Channel.QueueDeclare(
		q.Name,
		q.Durable,
		q.AutoDelete,
		q.Exclusive,
		q.NoWait,
		nil,
	)
	if err != nil {
		return nil, err
	}

	for _, exch := range config.Queue.Bind {
		if err := a.Channel.QueueBind(queue.Name, rk, exch, q.NoWait, nil); err != nil {
			return nil, err
		}
		Debugf("amqp: binded queue %s to exchange %s with routing key %s", queue.Name, exch, rk)
	}

	return &AMQPConsumer{
		AMQP:   a,
		Config: config,
		Queue:  queue,
	}, nil
}

// AMQPReceiver receives delayd commands over amqp
type AMQPReceiver struct {
	*AMQPConsumer

	c            chan Message
	shutdown     chan bool
	metaMessages chan (<-chan amqp.Delivery)
	messages     chan amqp.Delivery
	paused       bool
	tagCount     uint
	ac           AMQPConfig
	mu           sync.Mutex
}

// NewAMQPReceiver creates a new Receiver based on the provided Config,
// and starts it listening for commands.
func NewAMQPReceiver(ac AMQPConfig, routingKey string) (*AMQPReceiver, error) {
	a, err := NewAMQPConsumer(ac, routingKey)
	if err != nil {
		return nil, err
	}

	receiver := &AMQPReceiver{
		AMQPConsumer: a,

		c:            make(chan Message),
		messages:     make(chan amqp.Delivery, ac.Qos),
		metaMessages: make(chan (<-chan amqp.Delivery)),
		ac:           ac,
		paused:       true,
		shutdown:     make(chan bool),
	}

	go receiver.monitorChannel()
	go receiver.messageLoop()

	return receiver, nil
}

// monitorChannel monitors metaMessages channel.
// It installs new channel when metaMessages channel returns a channel
func (a *AMQPReceiver) monitorChannel() {
	var realMessages <-chan amqp.Delivery
	for {
		select {
		case <-a.shutdown:
			Debug("amqp: received signal to quit reading amqp, exiting goroutine")
			return
		case m := <-a.metaMessages:
			// we have a new source of 'real' messages. swap it in.
			realMessages = m
			Debug("amqp: new amqp channel is installed")
		case msg, ok := <-realMessages:
			// Don't propagate any channel close messages
			if ok {
				a.messages <- msg
			}
		}
	}
}

func (a *AMQPReceiver) messageLoop() {
	for {
		select {
		case <-a.shutdown:
			Debug("amqp: received signal to quit reading amqp, exiting goroutine")
			return
		case delivery := <-a.messages:
			deliverer := &AMQPDeliverer{Delivery: delivery}

			var delay int64
			switch val := delivery.Headers["delayd-delay"].(type) {
			case int32:
				delay = int64(val)
			case int64:
				delay = val
			default:
				Warn(delivery)
				Warn("amqp: bad/missing delay. discarding message")
				deliverer.Ack()
				continue
			}

			entry := Entry{
				SendAt: time.Now().Add(time.Duration(delay) * time.Millisecond),
			}

			target, found := delivery.Headers["delayd-target"].(string)
			if !found {
				Warn("amqp: bad/missing target. discarding message")
				deliverer.Ack()
				continue
			}
			entry.Target = target

			// optional key value for overwrite
			if k, ok := delivery.Headers["delayd-key"].(string); ok {
				entry.Key = k
			}

			// optional headers that will be relayed
			entry.AMQP = &AMQPMessage{
				ContentType:     delivery.ContentType,
				ContentEncoding: delivery.ContentEncoding,
				CorrelationID:   delivery.CorrelationId,
			}

			entry.Body = delivery.Body
			msg := Message{
				Entry: entry,
				MessageDeliverer: &AMQPDeliverer{
					Delivery: delivery,
				},
			}
			a.c <- msg
		}
	}
}

// MessageCh returns a receiving-only channel returns Message
func (a *AMQPReceiver) MessageCh() <-chan Message {
	return a.c
}

// Close pauses the receiver and signals the shutdown channel.
// Finally, it closes AMQP the channel and connection.
// FIXME: Is this still restartable?
func (a *AMQPReceiver) Close() {
	a.Pause()
	close(a.shutdown)
	close(a.c)
	a.AMQP.Close()
}

// Start or restart listening for messages on the queue
func (a *AMQPReceiver) Start() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.paused {
		// FIXME: already started?
		return nil
	}

	m, err := a.Channel.Consume(
		a.ac.Queue.Name,
		consumerTag+string(a.tagCount),
		a.ac.Queue.AutoAck,
		a.ac.Queue.Exclusive,
		a.ac.Queue.NoLocal,
		a.ac.Queue.NoWait,
		nil,
	)
	if err != nil {
		return err
	}

	a.paused = false
	a.tagCount++

	// Install new channel
	a.metaMessages <- m
	return err
}

// Pause listening for messages on the queue
func (a *AMQPReceiver) Pause() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.paused {
		return nil
	}
	a.paused = true
	return a.Channel.Cancel(consumerTag+string(a.tagCount), false)
}

// AMQPSender sends delayd entries over amqp after their timeout
type AMQPSender struct {
	*AMQP
}

// NewAMQPSender creates a new Sender connected to the given  URL.
func NewAMQPSender(amqpURL string) (*AMQPSender, error) {
	a, err := NewAMQP(amqpURL)
	if err != nil {
		return nil, err
	}

	return &AMQPSender{AMQP: a}, nil
}

// Send sends a delayd entry over , using the entry's Target as the publish
// exchange.
func (s *AMQPSender) Send(e Entry) error {
	if e.AMQP == nil {
		return errors.New("amqp: invalid entry")
	}

	msg := amqp.Publishing{
		DeliveryMode:    amqp.Persistent,
		Timestamp:       time.Now(),
		ContentType:     e.AMQP.ContentType,
		ContentEncoding: e.AMQP.ContentEncoding,
		CorrelationId:   e.AMQP.CorrelationID,
		Body:            e.Body,
	}
	return s.Channel.Publish(e.Target, "", true, false, msg)
}
