package main

import (
	"time"

	"github.com/ugorji/go/codec"
)

var mh codec.MsgpackHandle

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
