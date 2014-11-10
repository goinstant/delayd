package delayd

import (
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// a generous 60 seconds to apply raft commands
const raftMaxTime = time.Duration(60) * time.Second

// Server is the delayd server. It handles the server lifecycle (startup, clean shutdown)
type Server struct {
	sender     Sender
	receiver   *AMQPReceiver
	raft       *Raft
	timer      *Timer
	shutdownCh chan bool
	leader     bool
	m          *sync.Mutex
}

// Run initializes the Server from a Config, and begins its main loop.
func (s *Server) Run(c Config) {
	s.leader = false
	s.m = new(sync.Mutex)
	s.shutdownCh = make(chan bool)

	if len(c.LogDir) != 0 {
		Debug("Creating log dir: ", c.LogDir)
		err := os.MkdirAll(c.LogDir, 0755)
		if err != nil {
			Fatal("Error creating log dir: ", err)
		}
		logFile := path.Join(c.LogDir, "delayd.log")
		logOutput, err := os.OpenFile(logFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		log.SetOutput(logOutput)
	}

	Info("Starting delayd")

	Debug("Creating data dir: ", c.DataDir)
	err := os.MkdirAll(c.DataDir, 0755)
	if err != nil {
		Fatal("Error creating data dir: ", err)
	}

	s.receiver, err = NewAMQPReceiver(c.AMQP)
	if err != nil {
		Fatal("Could not initialize receiver: ", err)
	}

	s.sender, err = NewAMQPSender(c.AMQP.URL)
	if err != nil {
		Fatal("Could not initialize sender: ", err)
	}

	s.raft, err = NewRaft(c.Raft, c.DataDir, c.LogDir)
	if err != nil {
		Fatal("Could not initialize raft: ", err)
	}

	s.timer = NewTimer(s.timerSend)
	go s.observeLeaderChanges()
	go s.observeNextTime()

	for {
		msg, ok := <-s.receiver.C
		entry := msg.Entry
		// XXX cleanup needed here before exit
		if !ok {
			//Fatal("Receiver Consumption failed!")
		}

		Debug("Got entry: ", entry)
		b, err := entry.ToBytes()
		if err != nil {
			Error("Error encoding entry", err)
			continue
		}

		err = s.raft.Add(b, raftMaxTime)
		if err != nil {
			msg.Nack()
		}

		msg.Ack()
	}
}

// Stop shuts down the Server cleanly. Order of the Close calls is important.
func (s *Server) Stop() {
	Info("Shutting down gracefully.")

	// stop triggering new changes to the FSM
	s.receiver.Close()
	s.timer.Stop()

	close(s.shutdownCh)

	// with no FSM stimulus, we don't need to send.
	s.sender.Close()

	s.raft.Close()

	Info("Terminated.")
}

func (s *Server) resetTimer() {
	ok, t, err := s.raft.fsm.store.NextTime()
	if err != nil {
		Fatal("Could not read initial send time from storage: ", err)
	}
	if ok {
		s.timer.Reset(t, true)
	}
}

// Listen for changes to raft for when we transition to and from the leader state
// and react accordingly.
func (s *Server) observeLeaderChanges() {
	for isLeader := range s.raft.LeaderCh() {
		s.m.Lock()
		s.leader = isLeader
		s.m.Unlock()

		switch isLeader {
		case true:
			Debug("Became raft leader")
			s.resetTimer()
			err := s.receiver.Start()
			if err != nil {
				Fatal("Error while starting receiver: ", err)
			}

		case false:
			Debug("Lost raft leadership")
			s.timer.Pause()
			err := s.receiver.Pause()
			if err != nil {
				Fatal("Error while starting receiver: ", err)
			}
		}
	}
}

// Listen to storage for next time changes on entry commits
func (s *Server) observeNextTime() {
	for {
		select {
		case <-s.shutdownCh:
			return
		case t := <-s.raft.fsm.store.C:
			s.m.Lock()
			if s.leader {
				Debug("got time", t)
				s.timer.Reset(t, false)
			}
			s.m.Unlock()
		}
	}
}

func (s *Server) timerSend(t time.Time) {
	uuids, entries, err := s.raft.fsm.store.Get(t)
	if err != nil {
		Fatal("Could not read entries from db: ", err)
	}

	Infof("Sending %d entries\n", len(entries))
	for i, e := range entries {
		err = s.sender.Send(e)

		// error 504 code means that the exchange we were trying
		// to send on didnt exist.  In the case of delayd this usually
		// means that a consumer didn't set up the exchange they wish
		// to be notified on.  We do not attempt to make this for them,
		// as we don't know what exchange options they would want, we
		// simply drop this message, other errors are fatal

		if err, ok := err.(*amqp.Error); ok {
			if err.Code != 504 {
				Fatal("Could not send entry: ", err)
			} else {
				Warnf("channel/connection not set up for exchange `%s`, message will be deleted", e.Target, err)
			}
		}

		err = s.raft.Remove(uuids[i], raftMaxTime)
		if err != nil {
			// This node is no longer the leader. give up on other amqp sends,
			// and scheduling the next emission
			Warnf("Lost raft leadership during remove. AMQP send will be a duplicate. uuid=%x\n", uuids[i])
			break
		}
	}

	if err != nil {
		// don't reschedule
		return
	}

	// ensure everyone is up to date
	err = s.raft.SyncAll()
	if err != nil {
		Warn("Lost raft leadership during sync after send.")
		return
	}

	return
}
