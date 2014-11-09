package delayd

import (
	"time"

	"github.com/streadway/amqp"
	"github.com/ugorji/go/codec"
)

var mh codec.MsgpackHandle

// EntryWrapper wraps the Entry object and also attaches the amqp.Delivery
// object so that we can call Done whenever we want to ACK or NACK the message
type EntryWrapper struct {
	Entry Entry
	Msg   amqp.Delivery
}

// Done is called whenever the user wants to ACK/NACK a message via amqp.
// a boolean is passed indicated whether we should ACK or NACK
func (e *EntryWrapper) Done(succ bool) {
	// If we cannot ACK a message then we are pretty much stuck, we won't
	// be able to receive any additional messages
	pan := func() {
		Panic("Unable to ACK/NACK amqp message")
	}

	if succ {
		err := e.Msg.Ack(false)
		if err != nil {
			pan()
		}

		return
	}

	err := e.Msg.Nack(false, false)
	if err != nil {
		pan()
	}
}

// Entry represents a delayed message.
type Entry struct {
	// Required
	SendAt time.Time
	Target string
	Body   []byte

	// Optional
	Key string

	// XXX amqp specific
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

// Sender describes the interface for sending delayd entries after their time
// has lapsed.
type Sender interface {
	Send(e Entry) (err error)
	Close()
}
