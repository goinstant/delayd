package delayd

import (
	"time"

	"github.com/streadway/amqp"
	"github.com/ugorji/go/codec"
)

var mh codec.MsgpackHandle

// Message represents a message with a broker-agnostic way.
// It holds Entry object and also a broker-specific message manager.
type Message struct {
	Entry
	MessageDeliverer
}

// MessageDeliverer is an interface for a broker-spefici message
// acknowledgment.
type MessageDeliverer interface {
	Ack() error
	Nack() error
}

// AMQPDeliverer implements MessageDeliverer for AMQP.
type AMQPDeliverer struct {
	amqp.Delivery
}

// Ack acknowledgs a message via AMQP.
func (d *AMQPDeliverer) Ack() error {
	return d.Delivery.Ack(false)
}

// Nack returns negative an acknowledgement via AMQP.
func (d *AMQPDeliverer) Nack() error {
	return d.Delivery.Nack(false, false)
}

// Entry represents a delayed message.
type Entry struct {
	// Required
	SendAt time.Time
	Target string
	Body   []byte

	// Optional
	Key string

	// AMQP specific message
	AMQP *AMQPMessage
}

// AMQPMessage represents AMQP specific message.
type AMQPMessage struct {
	ContentEncoding string
	ContentType     string
	CorrelationID   string
}

// entryFromBytes creates a new Entry based on the MessagePack encoded byte slice b.
func entryFromBytes(b []byte) (e Entry, err error) {
	dec := codec.NewDecoderBytes(b, &mh)
	err = dec.Decode(&e)
	return
}

// ToBytes encodes an Entry to a byte slice, encoding with MessagePack
func (e Entry) ToBytes() (b []byte, err error) {
	enc := codec.NewEncoderBytes(&b, &mh)
	err = enc.Encode(e)
	return
}
