package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimerCallsSendFunc(t *testing.T) {
	ch := make(chan bool)
	testFunc := func(callTime time.Time) (time.Time, bool) {
		ch <- true
		return time.Now(), false
	}

	timer := NewTimer(testFunc)
	defer timer.Stop()

	timeout := time.After(time.Duration(3) * time.Millisecond)
	timer.Reset(time.Now().Add(time.Duration(1)*time.Millisecond), true)

	select {
	case _ = <-ch:
		return
	case _ = <-timeout:
		assert.Fail(t, "test timed out")
	}
}
