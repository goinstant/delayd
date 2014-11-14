package delayd

import (
	"sync"
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

func TestTimerReset(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)

	var lastTime time.Time
	testFunc := func(callTime time.Time) {
		lastTime = callTime
		wg.Done()
	}

	timer := NewTimer(testFunc)
	defer timer.Stop()

	n := time.Now()
	tt := []time.Time{
		n.Add(2 * time.Millisecond),
		n.Add(-2 * time.Millisecond),
		n.Add(2 * time.Millisecond),
	}

	go func() {
		// initialize
		timer.Reset(tt[0], true)

		// before now
		timer.Reset(tt[1], false)

		// normal
		timer.Reset(tt[2], false)
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
		if lastTime.Before(tt[2]) {
			t.Error("the last time should be after the last call")
		}
		t.Log("done!")
	case <-time.After(500 * time.Millisecond):
		t.Error("timeout")
	}
}
