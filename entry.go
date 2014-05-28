package main

import "time"

type Entry struct {
	SendAt time.Time
	Body   []byte

	// XXX amqp specific
	ContentEncoding string
	ContentType     string
	CorrelationId   string
}
