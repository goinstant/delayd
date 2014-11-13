package delayd

import (
	"sync"
	"time"
)

const tickDuration = 3 * time.Second
const twentyFourHours = time.Duration(24) * time.Hour

// SendFunc is called by the timer every time the timer lapses.
type SendFunc func(time.Time)

// Timer handles triggering event emission, and coordination between Storage
// and the Sender
type Timer struct {
	shutdown     chan bool
	tickCh       chan time.Time
	timerRunning bool
	timer        *time.Timer
	nextSend     time.Time
	mu           sync.Mutex
	sendFunc     SendFunc
}

// NewTimer creates a new timer instance, and starts its main loop
func NewTimer(sendFunc SendFunc) *Timer {
	t := &Timer{
		timer:    time.NewTimer(twentyFourHours),
		tickCh:   make(chan time.Time),
		nextSend: time.Now().Add(twentyFourHours),
		sendFunc: sendFunc,
		shutdown: make(chan bool),
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
func (t *Timer) Reset(resetTo time.Time, force bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	n := time.Now()

	nextSend := resetTo
	if resetTo.Before(n) {
		nextSend = n
	}

	Debugf("timer: next: %s, t.next: %s, now: %s, resetTo: %s", nextSend, t.nextSend, n, resetTo)
	if force || !t.timerRunning || !nextSend.After(t.nextSend) {
		d := nextSend.Sub(n)
		if nextSend.Equal(n) {
			Debug("timer: flusing a timer immediately", n)
			t.tickCh <- n
			Debug("timer: flushed a timer", n)
			return
		}

		Debugf("timer: reset to %s, %s later", nextSend, d)
		t.timerRunning = true
		t.timer.Reset(d)
		t.nextSend = nextSend
	}
}

// Pause the timer, stopping any existing timeouts
func (t *Timer) Pause() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.timerRunning = false
	t.timer.Stop()
}

func (t *Timer) timerLoop() {
	for {
		select {
		case <-t.shutdown:
			return
		case sendTime := <-t.tickCh:
			Debug("timer: received from tickCh:", sendTime)
			t.sendFunc(sendTime)
			Debug("timer: sent from tickCh:", sendTime)
		case sendTime := <-t.timer.C:
			Debug("timer: received from timer:", sendTime)
			t.Pause()
			t.sendFunc(sendTime)
			Debug("timer: sent from timer:", sendTime)
		}
	}
}
