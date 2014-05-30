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

func TestAdd(t *testing.T) {
	dir, err := ioutil.TempDir("", "delayd-test")
	assert.Nil(t, err)
	defer os.Remove(dir)

	s, err := NewStorage(dir, StubSender{})
	assert.Nil(t, err)

	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}

	err = s.Add(e)
	assert.Nil(t, err)

	entries, err := s.get(e.SendAt)
	assert.Nil(t, err)

	assert.Equal(t, len(entries), 1)
}
