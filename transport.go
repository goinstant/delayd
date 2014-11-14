package delayd

// Closer is an interface for closing its underlying transport.
type Closer interface {
	Close()
}

// Sender is an interface for sending a message after their time has lapsed.
type Sender interface {
	Send(e *Entry) (err error)
	Closer
}

// Receiver is an interface for receiving a message from a broker via channel.
type Receiver interface {
	Start() error
	Pause() error
	MessageCh() <-chan Message
	Closer
}
