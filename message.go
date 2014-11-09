package delayd

// Message holds a message to send to delayd server
type Message struct {
	Value string
	Key   string
	Delay int64
}

// ClientMessages holds delayd client config options
type ClientMessages struct {
	Message []Message
}
