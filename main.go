package main

import (
	"bytes"
	"encoding/gob"
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
	log.Println("Starting delayd")

	var config Config
	if _, err := toml.DecodeFile("delayd.toml", &config); err != nil {
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

	storage, err := NewStorage()
	if err != nil {
		log.Fatal("Could not initialize storage backend: ", err)
	}

	raft, err := configureRaft(s, storage)
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
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(entry)
		if err != nil {
			log.Println("Error encoding entry", err)
			continue
		}

		// a generous 60 seconds to apply this command
		raft.Apply(buf.Bytes(), time.Duration(60)*time.Second)
	}
}
