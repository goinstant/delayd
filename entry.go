package main

import "time"

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
