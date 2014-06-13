package main

import (
	"errors"
	"io"
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

	uuidOffset  = 2
	entryOffset = 18

	logSchemaVersion  = 0x0
	snapSchemaVersion = 0x0
)

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
		Panicf("Unknown log schema version seen. version=%d", logVer)
	}

	cmdType := l.Data[1]
	switch cmdType {
	case byte(addCmd):
		Debug("Applying add command")

		uuid := l.Data[uuidOffset:entryOffset]

		entry, err := entryFromBytes(l.Data[entryOffset:])
		if err != nil {
			Panic("Error decoding entry: ", err)
		}

		err = fsm.store.Add(uuid, entry)
		if err != nil {
			Error("Error storing entry: ", err)
		}
	case byte(rmCmd):
		Debug("Applying rm command")
		err := fsm.store.Remove(l.Data[2:])
		if err != nil {
			Error("Error removing entry: ", err)
		}
	default:
		Panicf("Unknown command type seen. type=%d", cmdType)
	}

	return nil
}

// Snapshot creates a raft snapshot for fast restore.
func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	uuids, entries, err := fsm.store.GetAll()
	snapshot := &Snapshot{uuids, entries}
	return snapshot, err
}

// Restore from a raft snapshot
func (fsm *FSM) Restore(snap io.ReadCloser) error {
	defer snap.Close()

	s, err := NewStorage()
	if err != nil {
		return err
	}

	// swap in the restored storage, with time emission channels.
	s.c = fsm.store.c
	s.C = fsm.store.C
	fsm.store.Close()
	fsm.store = s

	b := make([]byte, 1)
	_, err = snap.Read(b)
	if b[0] != byte(snapSchemaVersion) {
		msg := "Unknown snapshot schema version"
		Error(msg)
		return errors.New(msg)
	}

	uuid := make([]byte, 16)
	size := make([]byte, 4)

	count := 0
	for {
		_, err = snap.Read(uuid)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		_, err = snap.Read(size)
		if err != nil {
			return err
		}

		eb := make([]byte, bytesToUint32(size))
		_, err = snap.Read(eb)
		if err != nil {
			return err
		}

		e, err := entryFromBytes(eb)
		if err != nil {
			return err
		}

		err = s.Add(uuid, e)
		if err != nil {
			return err
		}

		count++
	}

	Debugf("Restored snapshot. entries=%d", count)
	return nil
}

// Snapshot holds the data needed to serialize storage
type Snapshot struct {
	uuids   [][]byte
	entries []Entry
}

// Persist writes a snapshot to a file. We just serialize all active entries.
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write([]byte{snapSchemaVersion})
	if err != nil {
		sink.Cancel()
		return err
	}

	for i, e := range s.entries {
		_, err = sink.Write(s.uuids[i])
		if err != nil {
			sink.Cancel()
			return err
		}

		b, err := e.ToBytes()
		if err != nil {
			sink.Cancel()
			return err
		}

		_, err = sink.Write(uint32ToBytes(uint32(len(b))))
		if err != nil {
			sink.Cancel()
			return err
		}

		_, err = sink.Write(b)
		if err != nil {
			sink.Cancel()
			return err
		}
	}

	return sink.Close()
}

// Release cleans up a snapshot. We don't need to do anything.
func (s *Snapshot) Release() {
}

// Raft encapsulates the raft specific logic for startup and shutdown.
type Raft struct {
	transport *raft.NetworkTransport
	mdb       *raftmdb.MDBStore
	raft      *raft.Raft
	fsm       *FSM
}

// NewRaft creates a new Raft instance. raft data is stored under the raft dir in prefix.
func NewRaft(c RaftConfig, prefix string) (r *Raft, err error) {
	r = new(Raft)
	raftDir := path.Join(prefix, "raft")

	err = os.MkdirAll(raftDir, 0755)
	if err != nil {
		Fatal("Could not create raft storage dir: ", err)
	}

	fss, err := raft.NewFileSnapshotStore(raftDir, 1, nil)
	if err != nil {
		Error("Could not initialize raft snapshot store: ", err)
		return
	}

	// this should be our externally visible address. If not provided in the
	// config as 'advertise', we use the address of the listen config.
	if c.Advertise == nil {
		c.Advertise = &c.Listen
	}

	a, err := net.ResolveTCPAddr("tcp", *c.Advertise)
	if err != nil {
		Error("Could not lookup raft advertise address: ", err)
		return
	}

	r.transport, err = raft.NewTCPTransport(c.Listen, a, 3, 10*time.Second, nil)
	if err != nil {
		Error("Could not create raft transport: ", err)
		return
	}

	peerStore := raft.NewJSONPeers(raftDir, r.transport)

	config := raft.DefaultConfig()
	config.EnableSingleNode = c.Single

	if !c.Single {
		var peers []net.Addr
		peers, err = peerStore.Peers()
		if err != nil {
			return
		}

		for _, peerStr := range c.Peers {
			peer, err := net.ResolveTCPAddr("tcp", peerStr)
			if err != nil {
				Fatal("Bad peer:", err)
			}

			if !raft.PeerContained(peers, peer) {
				peerStore.SetPeers(raft.AddUniquePeer(peers, peer))
			}
		}
	} else {
		Warn("Running in single node permitted mode. Only use this for testing!")
	}

	r.mdb, err = raftmdb.NewMDBStore(raftDir)
	if err != nil {
		Error("Could not create raft store:", err)
		return
	}

	storage, err := NewStorage()
	if err != nil {
		Error("Could not create storage:", err)
		return
	}
	r.fsm = &FSM{storage}

	r.raft, err = raft.NewRaft(config, r.fsm, r.mdb, r.mdb, fss, peerStore, r.transport)
	if err != nil {
		Error("Could not initialize raft: ", err)
		return
	}

	return
}

// Close cleanly shutsdown the raft instance.
func (r *Raft) Close() {
	r.transport.Close()
	future := r.raft.Shutdown()
	if err := future.Error(); err != nil {
		Error("Error shutting down raft: ", err)
	}
	r.mdb.Close()
	r.fsm.store.Close()
}

// Add wraps the internal raft Apply, for encapsulation!
// Commands sent to raft are prefixed with a header containing two bytes of
// additional data:
// - the first byte indicates the schema version of the log entry
// - the second byte indicates the command type
// - Add includes 16 bytes after this for the entry UUID
//
// Add panics if it cannot create a UUID
func (r *Raft) Add(cmd []byte, timeout time.Duration) error {
	uuid, err := newUUID()
	if err != nil {
		Panic("Could not generate entry UUID")
	}

	h := append([]byte{logSchemaVersion, byte(addCmd)}, uuid...)
	Debug(h)
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

// LeaderCh just wraps the raft LeaderCh call
func (r *Raft) LeaderCh() <-chan bool {
	return r.raft.LeaderCh()
}
