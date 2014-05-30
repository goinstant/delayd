package main

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type StubSender struct {
}

func (s StubSender) Send(e Entry) error {
	return nil
}

func innerTestAdd(t *testing.T, e Entry) {
	dir, err := ioutil.TempDir("", "delayd-test")
	assert.Nil(t, err)
	defer os.Remove(dir)

	s, err := NewStorage(dir, StubSender{})
	assert.Nil(t, err)
	defer s.Close()

	err = s.Add(e)
	assert.Nil(t, err)

	entries, err := s.get(e.SendAt)
	assert.Nil(t, err)

	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0], e)

}

func TestAddNoKey(t *testing.T) {
	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}
	innerTestAdd(t, e)
}

func TestAddWithKey(t *testing.T) {
	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
		Key:    "user-key",
	}
	innerTestAdd(t, e)
}

func innerTestRemove(t *testing.T, e Entry) {
	dir, err := ioutil.TempDir("", "delayd-test")
	assert.Nil(t, err)
	defer os.Remove(dir)

	s, err := NewStorage(dir, StubSender{})
	assert.Nil(t, err)
	defer s.Close()

	err = s.Add(e)
	assert.Nil(t, err)

	err = s.remove(e)
	assert.Nil(t, err)

	entries, err := s.get(e.SendAt)
	assert.Nil(t, err)

	assert.Equal(t, len(entries), 0)
}

func TestRemoveNoKey(t *testing.T) {
	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}
	innerTestRemove(t, e)
}

func TestRemoveWithKey(t *testing.T) {
	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
		Key:    "user-key",
	}
	innerTestRemove(t, e)
}
