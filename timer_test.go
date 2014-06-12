package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimerCallsSendFunc(t *testing.T) {
	ch := make(chan bool, 1)
	testFunc := func(callTime time.Time) {
		close(ch)
	}

	timer := NewTimer(testFunc)
	defer timer.Stop()

	timeout := time.After(time.Duration(500) * time.Millisecond)
	timer.Reset(time.Now().Add(time.Duration(2)*time.Millisecond), true)

	select {
	case <-ch:
		break
	case <-timeout:
		assert.Fail(t, "test timed out")
	}
}
