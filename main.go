package main

import (
	"log"
	"os"
	"time"

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

	r, err := NewAmqpReceiver(config.Amqp.URL, config.Amqp.Queue)
	if err != nil {
		log.Fatal("Could not initialize receiver: ", err)
	}

	defer r.Close()

	s, err := NewAmqpSender(config.Amqp.URL)
	if err != nil {
		log.Fatal("Could not initialize sender: ", err)
	}

	// XXX read storage dir from config
	storage, err := NewStorage("delayd-data", s)
	if err != nil {
		log.Fatal("Could not initialize storage backend: ", err)
	}

	raft, err := configureRaft(storage)
	if err != nil {
		log.Fatal("Could not initialize raft: ", err)
	}

	for {
		entry, ok := <-r.C
		// XXX cleanup needed here before exit
		if !ok {
			log.Fatal("Receiver Consumption failed!")
		}

		log.Println("Got entry: ", entry)
		b, err := entry.ToBytes()
		if err != nil {
			log.Println("Error encoding entry", err)
			continue
		}

		// a generous 60 seconds to apply this command
		raft.Apply(b, time.Duration(60)*time.Second)
	}
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
