package delayd

// Sender is an interface for sending a message after their time has lapsed.
type Sender interface {
	Send(e *Entry) (err error)
	Close()
}
