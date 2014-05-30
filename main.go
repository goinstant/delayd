package main

import (
	"log"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/codegangsta/cli"
)

type AmqpConfig struct {
	URL   string
	Queue string
}

type Config struct {
	Amqp AmqpConfig
}

func execute(c *cli.Context) {
	log.Println("Starting delayd")

	var config Config
	if _, err := toml.DecodeFile(c.String("config"), &config); err != nil {
		log.Fatal("Unable to read config file: ", err)
	}

	s := Server{}
	s.Run(config)
}

func main() {
	app := cli.NewApp()
	app.Name = "delayd"
	app.Usage = "available setTimeout()"
	app.Action = execute
	app.Flags = []cli.Flag{
		cli.StringFlag{"config, c", "/etc/delayd.toml", "config file"},
	}
	app.Run(os.Args)
}
