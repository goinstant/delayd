package main

import (
	"github.com/BurntSushi/toml"
	"log"
)

// Message holds a message to send to delayd server
type Message struct {
	Value string
	Key   string
	Delay int64
}

// ClientMessages holds delayd client config options
type ClientMessages struct {
	Message []Message
}

func loadMessages(path string) (config ClientMessages, err error) {
	log.Println("reading", path)
	_, err = toml.DecodeFile(path, &config)
	return config, err
}
