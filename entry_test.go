package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEntryToBytes(t *testing.T) {
	e := Entry{
		SendAt: time.Now(),
	}

	b, err := e.ToBytes()
	assert.Nil(t, err)

	oe, err := entryFromBytes(b)
	assert.Nil(t, err)

	assert.Equal(t, oe, e)
}
