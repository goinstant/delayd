package delayd

// AMQPQueue holds configuration for the queue used by the AMQPReceiver
type AMQPQueue struct {
	Name       string   `toml:"name"`
	Bind       []string `toml:"bind"`
	Durable    bool     `toml:"durable"`
	AutoDelete bool     `toml:"auto_delete"`
	AutoAck    bool     `toml:"auto_ack"`
	Exclusive  bool     `toml:"exclusive"`
	NoLocal    bool     `toml:"no_local"`
	NoWait     bool     `toml:"no_wait"`
}

// AMQPExchange holds configuration for the exchange used by the AMQPReceiver
type AMQPExchange struct {
	Name string `toml:"name"`
	Kind string `toml:"kind"`

	AutoDelete                bool `toml:"auto_delete"`
	Durable, Internal, NoWait bool
}

// AMQPConfig holds configuration for AMQP senders and receivers.
type AMQPConfig struct {
	URL      string       `toml:"url"`
	Exchange AMQPExchange `toml:"exchange"`
	Qos      int          `toml:"qos"`
	Queue    AMQPQueue    `toml:"queue"`
}

// RaftConfig holds configuration for Raft concensus
type RaftConfig struct {
	Single    bool `toml:"single_node"`
	Peers     []string
	Listen    string
	Advertise *string
}

// Config holds delayd configuration
type Config struct {
	AMQP    AMQPConfig `toml:"amqp"`
	DataDir string     `toml:"data_dir"`
	LogDir  string     `toml:"log_dir"`
	Raft    RaftConfig
}
