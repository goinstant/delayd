package delayd

import (
	"bytes"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
)

const (
	invalidSchema  = 0xAB // A value far past what we recognize
	invalidCommand = 0xFE // A byte value that has no command associated
)

var (
	v0AddCmd = []byte{logSchemaVersion, byte(addCmd)}
	v0RmCmd  = []byte{logSchemaVersion, byte(rmCmd)}
)

func TestApplyPanicsOnBadSchemaVersion(t *testing.T) {
	l := raft.Log{Data: []byte{invalidSchema}}
	fsm := FSM{}

	assert.Panics(t, func() {
		fsm.Apply(&l)
	})
}

func TestApplyPanicsOnUnknownCommand(t *testing.T) {
	l := raft.Log{Data: []byte{logSchemaVersion, invalidCommand}}
	fsm := FSM{}

	assert.Panics(t, func() {
		fsm.Apply(&l)
	})
}

func TestApplyPanicsOnBadEntry(t *testing.T) {
	l := raft.Log{Data: []byte{logSchemaVersion, byte(addCmd), 0xDE, 0xAD, 0xBE, 0xEF}}
	fsm := FSM{}

	assert.Panics(t, func() {
		fsm.Apply(&l)
	})
}

func TestApplyAddsEntry(t *testing.T) {
	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}

	b, err := e.ToBytes()
	assert.Nil(t, err)

	fsm := FSM{s}

	l := raft.Log{Data: append(append(v0AddCmd, dummyUUID...), b...), Index: 1}
	fsm.Apply(&l)

	_, entries, _ := s.Get(e.SendAt)
	assert.Equal(t, len(entries), 1)
}

func TestApplyAddsMultipleEntries(t *testing.T) {
	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}

	b, err := e.ToBytes()
	assert.Nil(t, err)

	fsm := FSM{s}

	l := raft.Log{Data: append(append(v0AddCmd, dummyUUID...), b...), Index: 1}
	fsm.Apply(&l)

	l = raft.Log{Data: append(append(v0AddCmd, dummyUUID2...), b...), Index: 2}
	fsm.Apply(&l)

	_, entries, _ := s.Get(e.SendAt)
	assert.Equal(t, len(entries), 2)
}

func TestRemove(t *testing.T) {
	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}

	fsm := FSM{s}

	err = s.Add(dummyUUID, e)
	assert.Nil(t, err)

	l := raft.Log{Data: append(v0RmCmd, dummyUUID...), Index: 2}
	fsm.Apply(&l)

	_, entries, _ := s.Get(e.SendAt)
	assert.Equal(t, len(entries), 0)
}

type InmemSnapshotSink struct {
	bytes.Buffer
}

func (InmemSnapshotSink) Close() error {
	return nil
}

func (InmemSnapshotSink) ID() string {
	return "dummy"
}

func (InmemSnapshotSink) Cancel() error {
	return nil
}

func TestFSMSnapshotReapplies(t *testing.T) {
	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}

	fsm := FSM{s}

	err = s.Add(dummyUUID, e)
	assert.Nil(t, err)

	err = s.Add(dummyUUID2, e)
	assert.Nil(t, err)

	snap, err := fsm.Snapshot()
	assert.Nil(t, err)

	sink := InmemSnapshotSink{}

	snap.Persist(&sink)

	s2, err := NewStorage()
	assert.Nil(t, err)
	defer s2.Close()

	fsm2 := FSM{s2}

	fsm2.Restore(&sink)

	_, entries, _ := fsm.store.GetAll()
	assert.Equal(t, len(entries), 2)
}
