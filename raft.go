package main

import (
	"bytes"
	"io"
	"log"
	"net"
	"time"

	"encoding/gob"

	"github.com/hashicorp/raft"
)

type FSM struct {
	s AmqpSender
}

func (fsm *FSM) Apply(l *raft.Log) interface{} {
	log.Println("Applying log ", l)

	entry := Entry{}

	dec := gob.NewDecoder(bytes.NewBuffer(l.Data))
	err := dec.Decode(&entry)
	if err != nil {
		log.Println("Error decoding entry", err)
		return nil
	}

	go func() {
		timer := time.NewTimer(entry.SendAt.Sub(time.Now()))
		_ = <-timer.C
		log.Println("Sending entry: ", entry)
		fsm.s.C <- entry
	}()

	return nil
}

func (*FSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (*FSM) Restore(snap io.ReadCloser) error {
	return nil
}

func configureRaft(sender AmqpSender) (r *raft.Raft, err error) {
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

	fsm := FSM{sender}

	config := raft.DefaultConfig()
	config.EnableSingleNode = true

	r, err = raft.NewRaft(config, &fsm, raft.NewInmemStore(), raft.NewInmemStore(), fss, peers, trans)
	if err != nil {
		log.Println("Could not initialize raft: ", err)
		return
	}

	return
}
