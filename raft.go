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

// FSM wraps the Storage instance in the raft.FSM interface, allowing raft to apply commands.
type FSM struct {
	store *Storage
}

// Apply a raft.Log to our Storage instance.
func (fsm *FSM) Apply(l *raft.Log) interface{} {
	log.Println("Applying log ", l)

	entry, err := entryFromBytes(l.Data)
	if err != nil {
		log.Println("Error decoding entry: ", err)
		return nil
	}

	version, err := fsm.store.Version()
	if err != nil {
		log.Println("Error reading version: ", err)
		return nil
	}

	// this doesn't strictly check for version + 1 as raft has internal commands
	// that go on the log, too.
	if l.Index < version {
		log.Printf("Skipping apply for old version (did you restart?) existing=%d new=%d\n", version, l.Index)
		return nil
	}

	err = fsm.store.Add(entry, l.Index)
	if err != nil {
		log.Println("Error storing entry: ", err)
		return nil
	}

	return nil
}

// Snapshot creates a raft snapshot for fast restore.
func (*FSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

// Restore from a raft snapshot
func (*FSM) Restore(snap io.ReadCloser) error {
	return nil
}

// Raft encapsulates the raft specific logic for startup and shutdown.
type Raft struct {
	transport *raft.NetworkTransport
	mdb       *raftmdb.MDBStore
	raft      *raft.Raft
}

// NewRaft creates a new Raft instance. raft data is stored under the raft dir in prefix.
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

	fsm := FSM{storage}

	config := raft.DefaultConfig()
	config.EnableSingleNode = true

	r.raft, err = raft.NewRaft(config, &fsm, r.mdb, r.mdb, fss, peers, r.transport)
	if err != nil {
		log.Println("Could not initialize raft: ", err)
		return
	}

	return
}

// Close cleanly shutsdown the raft instance.
func (r *Raft) Close() {
	r.transport.Close()
	future := r.raft.Shutdown()
	if err := future.Error(); err != nil {
		log.Println("Error shutting down raft: ", err)
	}
	r.mdb.Close()
}

// Apply wraps the internal raft Apply, for encapsulation!
func (r *Raft) Apply(cmd []byte, timeout time.Duration) {
	r.raft.Apply(cmd, timeout)
}
