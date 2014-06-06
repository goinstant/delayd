package main

import (
	"log"
	"sync"
	"time"
)

const twentyFourHours = time.Duration(24) * time.Hour

// SendFunc is called by the timer every time the timer lapses. It must return
// the next send time, and a bool indicating if there is a next send time or not.
type SendFunc func(time.Time) (time.Time, bool)

// Timer handles triggering event emission, and coordination between Storage
// and the Sender
type Timer struct {
	timerRunning bool
	timer        *time.Timer
	nextSend     time.Time
	m            *sync.Mutex

	sendFunc SendFunc
}

// NewTimer creates a new timer instance, and starts its main loop
func NewTimer(sendFunc SendFunc) (t *Timer) {
	t = new(Timer)

	t.timerRunning = false
	t.timer = time.NewTimer(twentyFourHours)
	t.nextSend = time.Now().Add(twentyFourHours)
	t.m = new(sync.Mutex)

	t.sendFunc = sendFunc

	go t.timerLoop()

	return
}

// Stop gracefully stops the timer, ensuring any running processing is complete.
// XXX implement graceful stop.
func (t *Timer) Stop() {
	t.m.Lock()
	defer t.m.Unlock()
	t.timer.Stop()
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

	log.Println("Setting next timer for: ", t)
	t.timerRunning = true
	t.timer.Reset(nextSend.Sub(time.Now()))
	t.nextSend = nextSend
}

func (t *Timer) pause() {
	t.m.Lock()
	defer t.m.Unlock()
	t.timerRunning = false
}

func (t *Timer) timerLoop() {
	for sendTime := range t.timer.C {
		nextSend, ok := t.sendFunc(sendTime)

		if ok {
			t.Reset(nextSend, true)
		} else {
			t.pause()
		}
	}
}
