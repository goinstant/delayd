package main

import (
	"log"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

func configureRaft() (r *raft.Raft, err error) {
	fss, err := raft.NewFileSnapshotStore("raft", 1, nil)
	if err != nil {
		log.Println("Could not initialize raft snapshot store: ", err)
		return
	}

	advertise, err := net.ResolveTCPAddr("tcp", ":9998")
	if err != nil {
		log.Println("Could not lookup raft advertise address: ", err)
		return
	}

	trans, err := raft.NewTCPTransport("0.0.0.0:9999", advertise, 3, 10*time.Second, nil)
	if err != nil {
		log.Println("Could not create raft transport: ", err)
		return
	}

	peers := raft.NewJSONPeers("peers", trans)

	config := raft.DefaultConfig()
	config.EnableSingleNode = true

	r, err = raft.NewRaft(config, nil, raft.NewInmemStore(), raft.NewInmemStore(), fss, peers, trans)
	if err != nil {
		log.Println("Could not initialize raft: ", err)
		return
	}

	return
}
