package main

import (
	"time"

	"github.com/ugorji/go/codec"
)

var mh codec.MsgpackHandle

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

func entryFromBytes(b []byte) (e Entry, err error) {
	dec := codec.NewDecoderBytes(b, &mh)
	err = dec.Decode(&e)
	return
}

func (e Entry) ToBytes() (b []byte, err error) {
	enc := codec.NewEncoderBytes(&b, &mh)
	err = enc.Encode(e)
	return
}
