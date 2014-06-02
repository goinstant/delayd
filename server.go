package main

import (
	"log"
	"os"
	"time"
)

type Server struct {
}

func (s *Server) Run(c Config) {
	log.Println("Starting delayd")

	log.Println("Creating data dir: ", c.DataDir)
	err := os.MkdirAll(c.DataDir, 0755)

	if err != nil {
		log.Fatal("Error creating data dir: ", err)
	}

	r, err := NewAmqpReceiver(c.Amqp.URL, c.Amqp.Queue)
	if err != nil {
		log.Fatal("Could not initialize receiver: ", err)
	}

	defer r.Close()

	sender, err := NewAmqpSender(c.Amqp.URL)
	if err != nil {
		log.Fatal("Could not initialize sender: ", err)
	}

	storage, err := NewStorage(c.DataDir, sender)
	if err != nil {
		log.Fatal("Could not initialize storage backend: ", err)
	}

	raft, err := configureRaft(c.DataDir, storage)
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
