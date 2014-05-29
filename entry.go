package main

import (
	"bytes"
	"encoding/gob"
	"time"
)

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
	CorrelationId   string
}

func entryFromGob(b []byte) (e Entry, err error) {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	err = dec.Decode(&e)
	return
}
