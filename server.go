package main

import (
	"log"
	"os"
	"time"
)

type Server struct {
	sender   *AmqpSender
	receiver *AmqpReceiver
	storage  *Storage
	raft     *Raft
}

func (s *Server) Run(c Config) {
	log.Println("Starting delayd")

	log.Println("Creating data dir: ", c.DataDir)
	err := os.MkdirAll(c.DataDir, 0755)

	if err != nil {
		log.Fatal("Error creating data dir: ", err)
	}

	s.receiver, err = NewAmqpReceiver(c.Amqp.URL, c.Amqp.Queue)
	if err != nil {
		log.Fatal("Could not initialize receiver: ", err)
	}

	s.sender, err = NewAmqpSender(c.Amqp.URL)
	if err != nil {
		log.Fatal("Could not initialize sender: ", err)
	}

	s.storage, err = NewStorage(c.DataDir, s.sender)
	if err != nil {
		log.Fatal("Could not initialize storage backend: ", err)
	}

	s.raft, err = NewRaft(c.DataDir, s.storage)
	if err != nil {
		log.Fatal("Could not initialize raft: ", err)
	}

	for {
		entry, ok := <-s.receiver.C
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
		s.raft.Apply(b, time.Duration(60)*time.Second)
	}
}

func (s *Server) Stop() {
	log.Println("Shutting down gracefully.")

	s.receiver.Close()
	s.storage.Close()
	s.raft.Close()
	s.sender.Close()

	log.Println("Terminated.")
}
