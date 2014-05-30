package main

import (
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
	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}

	s, err := NewStorage(StubSender{})
	assert.Nil(t, err)

	err = s.Add(e)
	assert.Nil(t, err)

	entries, err := s.get(e.SendAt)
	assert.Nil(t, err)

	assert.Equal(t, len(entries), 1)
}
