package main

import (
	"log"
	"time"

	"github.com/BurntSushi/toml"
)

type AmqpConfig struct {
	Url   string
	Queue string
}

type Config struct {
	Amqp AmqpConfig
}

func main() {
	log.Println("Starting vulliamy")

	var config Config
	if _, err := toml.DecodeFile("vulliamy.toml", &config); err != nil {
		log.Fatal("Unable to read config file: ", err)
	}

	r, err := NewAmqpReceiver(config.Amqp.Url, config.Amqp.Queue)
	if err != nil {
		log.Fatal("Could not initialize receiver: ", err)
	}

	defer r.Close()

	for {
		entry, ok := <-r.C
		// XXX cleanup needed here before exit
		if !ok {
			log.Fatal("Receiver Consumption failed!")
		}

		log.Println("Got entry: ", entry)

		go func() {
			timer := time.NewTimer(entry.SendAt.Sub(time.Now()))
			_ = <-timer.C
			log.Println("Sending entry: ", entry)
		}()
	}
}
