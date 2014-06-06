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

// Command is the command type as stored in the raft log
type Command uint8

const (
	addCmd Command = iota
	rmCmd
)

const logSchemaVersion = 0x0

// FSM wraps the Storage instance in the raft.FSM interface, allowing raft to apply commands.
type FSM struct {
	store *Storage
}

// Apply a raft.Log to our Storage instance.
// Before passing the command to Storage, Apply ensures that both the
// schema version and command type are present and understood. It panics
// if not.
func (fsm *FSM) Apply(l *raft.Log) interface{} {
	logVer := l.Data[0]
	if logVer != logSchemaVersion {
		log.Printf("Unknown log schema version seen. version=%d", logVer)
		panic("Unknown log schema version!")
	}

	version, err := fsm.store.Version()
	if err != nil {
		log.Println("Error reading version: ", err)
		return nil
	}

	// this doesn't strictly check for version + 1 as raft has internal commands
	// that go on the log, too.
	if l.Index <= version {
		log.Printf("Skipping apply for old version (did you restart?) existing=%d new=%d\n", version, l.Index)
		return nil
	}

	cmdType := l.Data[1]
	switch cmdType {
	case byte(addCmd):
		log.Println("Applying add command")
		entry, err := entryFromBytes(l.Data[2:])
		if err != nil {
			log.Println("Error decoding entry: ", err)
			panic("Could not decode entry!")
		}

		_, err = fsm.store.Add(entry, l.Index)
		if err != nil {
			log.Println("Error storing entry: ", err)
		}
	case byte(rmCmd):
		log.Println("Applying rm command")
		err = fsm.store.Remove(l.Data[2:], l.Index)
		if err != nil {
			log.Println("Error removing entry: ", err)
		}
	default:
		log.Printf("Unknown command type seen. type=%d", cmdType)
		panic("Unknown command type!")
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

// Add wraps the internal raft Apply, for encapsulation!
// Commands sent to raft are prefixed with a header containing two bytes of
// additional data:
// - the first byte indicates the schema version of the log entry
// - the second byte indicates the command type
func (r *Raft) Add(cmd []byte, timeout time.Duration) error {
	h := []byte{logSchemaVersion, byte(addCmd)}
	future := r.raft.Apply(append(h, cmd...), timeout)
	return future.Error()
}

// Remove enqueues a remove command in raft. Like Add, it prefixes version and
// command type.
func (r *Raft) Remove(cmd []byte, timeout time.Duration) error {
	h := []byte{logSchemaVersion, byte(rmCmd)}
	future := r.raft.Apply(append(h, cmd...), timeout)
	return future.Error()
}

// SyncAll Ensures that all raft nodes are up to date with the latest state for
// their FSM
func (r *Raft) SyncAll() error {
	future := r.raft.Barrier(0)
	return future.Error()
}
