package main

import (
	"io"
	"log"
	"net"
	"os"
	"path"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-mdb"
)

type FSM struct {
	store    *Storage
	mdbStore *raftmdb.MDBStore
}

func (fsm *FSM) Apply(l *raft.Log) interface{} {
	log.Println("Applying log ", l)

	entry, err := entryFromBytes(l.Data)
	if err != nil {
		log.Println("Error decoding entry", err)
		return nil
	}

	err = fsm.store.Add(entry)
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

type Raft struct {
	transport *raft.NetworkTransport
	mdb       *raftmdb.MDBStore
	raft      *raft.Raft
}

func NewRaft(prefix string, storage *Storage) (r *Raft, err error) {
	r = new(Raft)
	raftDir := path.Join(prefix, "raft")

	err = os.MkdirAll(raftDir, 0755)
	if err != nil {
		log.Fatal("Could not create raft storage dir: ", err)
	}

	fss, err := raft.NewFileSnapshotStore(raftDir, 1, nil)
	if err != nil {
		log.Println("Could not initialize raft snapshot store: ", err)
		return
	}

	advertise, err := net.ResolveTCPAddr("tcp", ":9998")
	if err != nil {
		log.Println("Could not lookup raft advertise address: ", err)
		return
	}

	r.transport, err = raft.NewTCPTransport("0.0.0.0:9999", advertise, 3, 10*time.Second, nil)
	if err != nil {
		log.Println("Could not create raft transport: ", err)
		return
	}

	peers := raft.NewJSONPeers(raftDir, r.transport)

	r.mdb, err = raftmdb.NewMDBStore(raftDir)
	if err != nil {
		log.Println("Could not create raft store: ", err)
		return
	}

	fsm := FSM{storage, r.mdb}

	config := raft.DefaultConfig()
	config.EnableSingleNode = true

	r.raft, err = raft.NewRaft(config, &fsm, r.mdb, r.mdb, fss, peers, r.transport)
	if err != nil {
		log.Println("Could not initialize raft: ", err)
		return
	}

	return
}

func (r *Raft) Close() {
	r.transport.Close()
	future := r.raft.Shutdown()
	if err := future.Error(); err != nil {
		log.Printf("Error shutting down raft: ", err)
	}
	r.mdb.Close()
}

func (r *Raft) Apply(cmd []byte, timeout time.Duration) {
	r.raft.Apply(cmd, timeout)
}
