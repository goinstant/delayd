package delayd

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// a generous 60 seconds to apply raft commands
const raftMaxTime = time.Duration(60) * time.Second
const routingKey = "delayd"

// Server is the delayd server. It handles the server lifecycle (startup, clean shutdown)
type Server struct {
	sender     Sender
	receiver   *AMQPReceiver
	raft       *Raft
	timer      *Timer
	shutdownCh chan bool
	leader     bool
	mu         sync.Mutex
}

// NewServer initialize Server instance.
func NewServer(c Config) (*Server, error) {
	if len(c.LogDir) != 0 {
		logFile := filepath.Join(c.LogDir, "delayd.log")
		logOutput, err := os.OpenFile(logFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		log.SetOutput(logOutput)
	}

	receiver, err := NewAMQPReceiver(c.AMQP, routingKey)
	if err != nil {
		return nil, err
	}

	sender, err := NewAMQPSender(c.AMQP.URL)
	if err != nil {
		return nil, err
	}

	raft, err := NewRaft(c.Raft, c.DataDir, c.LogDir)
	if err != nil {
		return nil, err
	}

	return &Server{
		sender:     sender,
		receiver:   receiver,
		raft:       raft,
		shutdownCh: make(chan bool),
	}, nil
}

// Run starts server and begins its main loop.
func (s *Server) Run() {
	Info("server: starting delayd")

	s.timer = NewTimer(s.timerSend)

	go s.observeLeaderChanges()
	go s.observeNextTime()

	for {
		msg, ok := <-s.receiver.MessageCh()
		entry := msg.Entry
		// XXX cleanup needed here before exit
		if !ok {
			continue
		}

		Debug("server: got new request entry:", entry)
		b, err := entry.ToBytes()
		if err != nil {
			Error("server: error encoding entry: ", err)
			continue
		}

		if err := s.raft.Add(b, raftMaxTime); err != nil {
			Error("server: failed to add: ", err)
			msg.Nack()
			continue
		}

		msg.Ack()
	}
}

// Stop shuts down the Server cleanly. Order of the Close calls is important.
func (s *Server) Stop() {
	Info("server: shutting down gracefully.")

	// stop triggering new changes to the FSM
	s.receiver.Close()
	s.timer.Stop()

	close(s.shutdownCh)

	// with no FSM stimulus, we don't need to send.
	s.sender.Close()

	s.raft.Close()

	Info("server: terminated.")
}

func (s *Server) resetTimer() {
	ok, t, err := s.raft.fsm.store.NextTime()
	if err != nil {
		Fatal("server: could not read initial send time from storage:", err)
	}
	if ok {
		s.timer.Reset(t, true)
	}
}

// Listen for changes to raft for when we transition to and from the leader state
// and react accordingly.
func (s *Server) observeLeaderChanges() {
	for isLeader := range s.raft.LeaderCh() {
		s.mu.Lock()
		s.leader = isLeader
		s.mu.Unlock()

		if isLeader {
			Debug("server: became raft leader")
			s.resetTimer()
			err := s.receiver.Start()
			if err != nil {
				Fatal("server: error while starting receiver:", err)
			}
		} else {
			Debug("server: lost raft leadership")
			s.timer.Pause()
			err := s.receiver.Pause()
			if err != nil {
				Fatal("server: error while starting receiver:", err)
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
			s.mu.Lock()
			if s.leader {
				Debug("server: got time: ", t)
				s.timer.Reset(t, false)
			}
			s.mu.Unlock()
		}
	}
}

func (s *Server) timerSend(t time.Time) {
	// FIXME: In the case of error, we don't remove a log from raft log.
	uuids, entries, err := s.raft.fsm.store.Get(t)
	if err != nil {
		Fatal("server: could not read entries from db:", err)
	}

	Infof("server: sending %d entries", len(entries))
	if len(entries) < 1 {
		// FIXME: if no entries found, timer is missing newest entries.........
		_, entries, _ := s.raft.fsm.store.GetAll()
		Warnf("server: timer is missing newest entries. total: %d", len(entries))
	}

	for i, e := range entries {
		// error 504 code means that the exchange we were trying
		// to send on didnt exist.  In the case of delayd this usually
		// means that a consumer didn't set up the exchange they wish
		// to be notified on. We do not attempt to make this for them,
		// as we don't know what exchange options they would want, we
		// simply drop this message, other errors are fatal

		if err := s.sender.Send(e); err != nil {
			if err, ok := err.(*amqp.Error); ok && err.Code == 504 {
				Warnf("server: channel/connection not set up for exchange `%s`, message will be deleted", e.Target, err)
				break
			} else {
				Fatal("server: could not send entry: ", err)
			}
		}

		if err := s.raft.Remove(uuids[i], raftMaxTime); err != nil {
			// This node is no longer the leader. give up on other amqp sends,
			// and scheduling the next emission
			Warnf("server: lost raft leadership during remove. AMQP send will be a duplicate. uuid=%x", uuids[i])
			break
		}
	}

	// ensure everyone is up to date
	Debug("server: syncing raft after send.")
	err = s.raft.SyncAll()
	if err != nil {
		Warn("server: lost raft leadership during sync after send.")
		return
	}

	Debug("server: synced raft after send.")
}
