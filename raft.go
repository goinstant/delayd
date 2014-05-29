package main

import (
	"io"
	"log"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

type FSM struct {
	store *Storage
}

func (fsm *FSM) Apply(l *raft.Log) interface{} {
	log.Println("Applying log ", l)

	entry, err := entryFromBytes(l.Data)
	if err != nil {
		log.Println("Error decoding entry", err)
		return nil
	}

	err = fsm.store.Add(entry, l.Data)
	if err != nil {
		log.Println("Error storing entry", err)
		return nil
	}

	return nil
}

func (*FSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (*FSM) Restore(snap io.ReadCloser) error {
	return nil
}

func configureRaft(storage *Storage) (r *raft.Raft, err error) {
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

	fsm := FSM{storage}

	config := raft.DefaultConfig()
	config.EnableSingleNode = true

	r, err = raft.NewRaft(config, &fsm, raft.NewInmemStore(), raft.NewInmemStore(), fss, peers, trans)
	if err != nil {
		log.Println("Could not initialize raft: ", err)
		return
	}

	return
}
