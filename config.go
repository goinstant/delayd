package main

import (
	"github.com/BurntSushi/toml"
	"github.com/codegangsta/cli"
)

type Config struct {
	Amqp    AmqpConfig
	DataDir string `toml:"data_dir"`
}

type AmqpConfig struct {
	URL   string
	Queue string
}

func loadConfig(c *cli.Context) (config Config, err error) {
	_, err = toml.DecodeFile(c.String("config"), &config)

	return
}
