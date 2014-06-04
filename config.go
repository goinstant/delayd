package main

import (
	"github.com/BurntSushi/toml"
	"github.com/codegangsta/cli"
)

// AmqpQueue holds configuration for the queue used by the AmqpReceiver
type AmqpQueue struct {
	Name string   `toml:"name"`
	Bind []string `toml:"bind"`

	AutoDelete                 bool `toml:"auto_delete"`
	Durable, Exclusive, NoWait bool
}

// AmqpExchange holds configuration for the exchange used by the AmqpReceiver
type AmqpExchange struct {
	Name string `toml:"name"`
	Kind string `toml:"kind"`

	AutoDelete                bool `toml:"auto_delete"`
	Durable, Internal, NoWait bool
}

// AmqpConfig holds configuration for AMQP senders and receivers.
type AmqpConfig struct {
	URL      string       `toml:"url"`
	Exchange AmqpExchange `toml:"exchange"`
	Queue    AmqpQueue    `toml:"queue"`
}

// Config holds delayd configuration
type Config struct {
	Amqp    AmqpConfig `toml:"amqp"`
	DataDir string     `toml:"data_dir"`
}

// loadConfig load's delayd's toml configuration, based on the command-line
// provided location, or the default (/etc/delayd.toml)
func loadConfig(c *cli.Context) (config Config, err error) {
	_, err = toml.DecodeFile(c.String("config"), &config)

	return
}
