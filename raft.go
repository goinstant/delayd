package delayd

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
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
		Panicf("raft: unknown log schema version seen. version=%d", logVer)
	}

	cmdType := l.Data[1]
	switch Command(cmdType) {
	case addCmd:
		Debug("raft: applying add command")

		uuid := l.Data[uuidOffset:entryOffset]
		entry, err := entryFromBytes(l.Data[entryOffset:])
		if err != nil {
			Panic("raft: error decoding entry:", err)
		}

		if err := fsm.store.Add(uuid, entry); err != nil {
			Error("raft: failed to add entry:", err)
			return err
		}
	case rmCmd:
		Debug("raft: applying rm command")
		if err := fsm.store.Remove(l.Data[2:]); err != nil {
			Error("raft: failed to rm entry:", err)
			return err
		}
	default:
		Panicf("raft: unknown command type seen. type=%d", cmdType)
	}

	return nil
}

// Snapshot creates a raft snapshot for fast restore.
func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	uuids, entries, err := fsm.store.GetAll()
	return &Snapshot{uuids, entries}, err
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
		return errors.New("raft: unknown snapshot schema version")
	}

	uuid := make([]byte, 16)
	size := make([]byte, 4)

	count := 0
	for {
		_, err = snap.Read(uuid)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if _, err := snap.Read(size); err != nil {
			return err
		}

		eb := make([]byte, bytesToUint32(size))
		if _, err := snap.Read(eb); err != nil {
			return err
		}

		e, err := entryFromBytes(eb)
		if err != nil {
			return err
		}

		if err := s.Add(uuid, e); err != nil {
			return err
		}

		count++
	}

	Debugf("raft: restored snapshot. entries=%d", count)
	return nil
}

// Snapshot holds the data needed to serialize storage
type Snapshot struct {
	uuids   [][]byte
	entries []*Entry
}

// Persist writes a snapshot to a file. We just serialize all active entries.
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write([]byte{snapSchemaVersion}); err != nil {
		sink.Cancel()
		return err
	}

	for i, e := range s.entries {
		if _, err := sink.Write(s.uuids[i]); err != nil {
			sink.Cancel()
			return err
		}

		b, err := e.ToBytes()
		if err != nil {
			sink.Cancel()
			return err
		}

		if _, err := sink.Write(uint32ToBytes(uint32(len(b)))); err != nil {
			sink.Cancel()
			return err
		}

		if _, err := sink.Write(b); err != nil {
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
func NewRaft(c RaftConfig, prefix string, logDir string) (*Raft, error) {
	config := raft.DefaultConfig()
	config.EnableSingleNode = c.Single

	if len(logDir) > 0 {
		logFile := filepath.Join(logDir, "raft.log")
		logOutput, err := os.OpenFile(logFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}

		config.LogOutput = logOutput
	}

	raftDir := filepath.Join(prefix, "raft")
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, fmt.Errorf("raft: could not create raft storage dir: %s", err)
	}

	fss, err := raft.NewFileSnapshotStore(raftDir, 1, nil)
	if err != nil {
		Error("raft: could not initialize raft snapshot store:", err)
		return nil, err
	}

	// this should be our externally visible address. If not provided in the
	// config as 'advertise', we use the address of the listen config.
	if c.Advertise == nil {
		c.Advertise = &c.Listen
	}

	a, err := net.ResolveTCPAddr("tcp", *c.Advertise)
	if err != nil {
		Error("raft: could not lookup raft advertise address:", err)
		return nil, err
	}

	transport, err := raft.NewTCPTransport(c.Listen, a, 3, 10*time.Second, nil)
	if err != nil {
		Error("raft: could not create raft transport:", err)
		return nil, err
	}

	peerStore := raft.NewJSONPeers(raftDir, transport)

	if !c.Single {
		peers, err := peerStore.Peers()
		if err != nil {
			return nil, err
		}

		for _, peerStr := range c.Peers {
			peer, err := net.ResolveTCPAddr("tcp", peerStr)
			if err != nil {
				Fatal("raft: bad peer:", err)
			}

			if !raft.PeerContained(peers, peer) {
				peerStore.SetPeers(raft.AddUniquePeer(peers, peer))
			}
		}
	} else {
		Warn("raft: uunning in single node permitted mode. only use this for testing!")
	}

	mdb, err := raftmdb.NewMDBStore(raftDir)
	if err != nil {
		Error("raft: could not create raft store:", err)
		return nil, err
	}

	storage, err := NewStorage()
	if err != nil {
		Error("raft: could not create storage:", err)
		return nil, err
	}
	fsm := &FSM{storage}

	raft, err := raft.NewRaft(config, fsm, mdb, mdb, fss, peerStore, transport)
	if err != nil {
		Error("raft: could not initialize raft:", err)
		return nil, err
	}

	return &Raft{
		transport: transport,
		mdb:       mdb,
		fsm:       fsm,
		raft:      raft,
	}, nil
}

// Close cleanly shutsdown the raft instance.
func (r *Raft) Close() {
	r.transport.Close()
	future := r.raft.Shutdown()
	if err := future.Error(); err != nil {
		Error("raft: error shutting down raft:", err)
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
		Panic("raft: could not generate entry UUID")
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
