package main

import (
	"github.com/BurntSushi/toml"
	"github.com/codegangsta/cli"
)

type AmqpQueue struct {
	Name string   `toml:"name"`
	Bind []string `toml:"bind"`

	AutoDelete                 bool `toml:"auto_delete"`
	Durable, Exclusive, NoWait bool
}

type AmqpExchange struct {
	Name string `toml:"name"`
	Kind string `toml:"kind"`

	AutoDelete                bool `toml:"auto_delete"`
	Durable, Internal, NoWait bool
}

type AmqpConfig struct {
	URL      string       `toml:"url"`
	Exchange AmqpExchange `toml:"exchange"`
	Queue    AmqpQueue    `toml:"queue"`
}

type Config struct {
	Amqp    AmqpConfig `toml:"amqp"`
	DataDir string     `toml:"data_dir"`
}

func loadConfig(c *cli.Context) (config Config, err error) {
	_, err = toml.DecodeFile(c.String("config"), &config)

	return
}
