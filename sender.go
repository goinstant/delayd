package main

// Sender describes the interface for sending delayd entries after their time
// has lapsed.
type Sender interface {
	Send(e Entry) (err error)
}
