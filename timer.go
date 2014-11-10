package delayd

import (
	"sync"
	"time"
)

const twentyFourHours = time.Duration(24) * time.Hour

// SendFunc is called by the timer every time the timer lapses.
type SendFunc func(time.Time)

// Timer handles triggering event emission, and coordination between Storage
// and the Sender
type Timer struct {
	shutdown     chan bool
	timerRunning bool
	timer        *time.Timer
	nextSend     time.Time
	m            *sync.Mutex
	sendFunc     SendFunc
}

// NewTimer creates a new timer instance, and starts its main loop
func NewTimer(sendFunc SendFunc) *Timer {
	t := &Timer{
		timerRunning: false,
		timer:        time.NewTimer(twentyFourHours),
		nextSend:     time.Now().Add(twentyFourHours),
		m:            &sync.Mutex{},
		sendFunc:     sendFunc,
		shutdown:     make(chan bool),
	}

	go t.timerLoop()

	return t
}

// Stop gracefully stops the timer, ensuring any running processing is complete.
func (t *Timer) Stop() {
	close(t.shutdown)
	t.Pause()
}

// Reset resets the timer to nextSend, if the timer is not running, or if nextSend is before
// the current scheduled send.
func (t *Timer) Reset(nextSend time.Time, force bool) {
	t.m.Lock()
	defer t.m.Unlock()

	if nextSend.Before(time.Now()) {
		nextSend = time.Now()
	}

	if !force && t.timerRunning && nextSend.After(t.nextSend) {
		return
	}

	Debug("Timer reset to", nextSend)
	t.timerRunning = true
	t.timer.Reset(nextSend.Sub(time.Now()))
	t.nextSend = nextSend
}

// Pause the timer, stopping any existing timeouts
func (t *Timer) Pause() {
	t.m.Lock()
	defer t.m.Unlock()
	t.timerRunning = false
	t.timer.Stop()
}

func (t *Timer) timerLoop() {
	for {
		select {
		case <-t.shutdown:
			return
		case sendTime := <-t.timer.C:
			t.Pause()
			t.sendFunc(sendTime)
		}
	}
}
