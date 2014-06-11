package main

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
)

const (
	invalidSchema  = 0xAB // A value far past what we recognize
	invalidCommand = 0xFE // A byte value that has no command associated
)

var v0AddCmd = []byte{logSchemaVersion, byte(addCmd)}
var v0RmCmd = []byte{logSchemaVersion, byte(rmCmd)}

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

	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}

	b, err := e.ToBytes()
	assert.Nil(t, err)

	fsm := FSM{s}

	l := raft.Log{Data: append(v0AddCmd, b...), Index: 1}
	fsm.Apply(&l)

	_, entries, _ := s.Get(e.SendAt)
	assert.Equal(t, len(entries), 1)
}

func TestApplyAddsMultipleEntries(t *testing.T) {
	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}

	b, err := e.ToBytes()
	assert.Nil(t, err)

	fsm := FSM{s}

	l := raft.Log{Data: append(v0AddCmd, b...), Index: 1}
	fsm.Apply(&l)

	l = raft.Log{Data: append(v0AddCmd, b...), Index: 2}
	fsm.Apply(&l)

	_, entries, _ := s.Get(e.SendAt)
	assert.Equal(t, len(entries), 2)
}

func TestRemove(t *testing.T) {
	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}

	fsm := FSM{s}

	uuid, err := s.Add(e)
	assert.Nil(t, err)

	l := raft.Log{Data: append(v0RmCmd, uuid...), Index: 2}
	fsm.Apply(&l)

	_, entries, _ := s.Get(e.SendAt)
	assert.Equal(t, len(entries), 0)
}
