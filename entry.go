package main

import "time"

type Entry struct {
	SendAt time.Time
	Body   []byte
}
