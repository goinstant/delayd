package main

type Sender interface {
	Send(e Entry) (err error)
}
