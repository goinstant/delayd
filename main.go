package main

import (
	"log"
	"time"

	"github.com/BurntSushi/toml"
)

type AmqpConfig struct {
	URL      string
	Queue    string
	Exchange string
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

	r, err := NewAmqpReceiver(config.Amqp.URL, config.Amqp.Queue)
	if err != nil {
		log.Fatal("Could not initialize receiver: ", err)
	}

	defer r.Close()

	s, err := NewAmqpSender(config.Amqp.URL, config.Amqp.Exchange)
	if err != nil {
		log.Fatal("Could not initialize sender: ", err)
	}

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
			s.C <- entry
		}()
	}
}
