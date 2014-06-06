package delayd

import (
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

// a generous 60 seconds to apply raft commands
const raftMaxTime = time.Duration(60) * time.Second

// Server is the delayd server. It handles the server lifecycle (startup, clean shutdown)
type Server struct {
	sender   *AmqpSender
	receiver *AmqpReceiver
	storage  *Storage
	raft     *Raft
	timer    *Timer
}

// Run initializes the Server from a Config, and begins its main loop.
func (s *Server) Run(c Config) {
	log.Println("Starting delayd")

	log.Println("Creating data dir: ", c.DataDir)
	err := os.MkdirAll(c.DataDir, 0755)

	if err != nil {
		log.Fatal("Error creating data dir: ", err)
	}

	s.receiver, err = NewAmqpReceiver(c.Amqp)
	if err != nil {
		log.Fatal("Could not initialize receiver: ", err)
	}

	s.sender, err = NewAmqpSender(c.Amqp.URL)
	if err != nil {
		log.Fatal("Could not initialize sender: ", err)
	}

	s.storage, err = NewStorage(c.DataDir)
	if err != nil {
		log.Fatal("Could not initialize storage backend: ", err)
	}

	s.raft, err = NewRaft(c.DataDir, s.storage)
	if err != nil {
		log.Fatal("Could not initialize raft: ", err)
	}

	ok, t, err := s.storage.NextTime()
	if err != nil {
		log.Fatal("Could not read initial send time from storage: ", err)
	}
	s.timer = NewTimer(s.timerSend)
	if ok {
		s.timer.Reset(t, true)
	}

	for {
		eWrapper, ok := <-s.receiver.C
		entry := eWrapper.Entry
		// XXX cleanup needed here before exit
		if !ok {
			log.Fatal("Receiver Consumption failed!")
		}

		log.Println("Got entry: ", entry)
		b, err := entry.ToBytes()
		if err != nil {
			log.Println("Error encoding entry", err)
			continue
		}

		err = s.raft.Add(b, raftMaxTime)
		if err != nil {
			eWrapper.Done(false)
		}
		s.timer.Reset(entry.SendAt, false)

		eWrapper.Done(true)
	}
}

// Stop shuts down the Server cleanly. Order of the Close calls is important.
func (s *Server) Stop() {
	log.Println("Shutting down gracefully.")

	s.receiver.Close()
	s.storage.Close()
	s.sender.Close()

	s.raft.Close()

	log.Println("Terminated.")
}

func (s *Server) timerSend(t time.Time) (next time.Time, ok bool) {
	uuids, entries, err := s.storage.Get(t)
	if err != nil {
		log.Fatal("Could not read entries from db: ", err)
	}

	log.Printf("Sending %d entries\n", len(entries))
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
				log.Fatal("Could not send entry: ", err)
			} else {
				log.Printf("channel/connection not set up for exchange `%s`, message will be deleted", e.Target)
			}
		}

		err = s.raft.Remove(uuids[i], raftMaxTime)
		if err != nil {
			// XXX abort comitting for raft here, so it can retry.
			log.Fatal("Could not remove entry from db: ", err)
		}
	}

	// ensure everyone is up to date
	err = s.raft.SyncAll()
	if err != nil {
		log.Fatal("Could not sync all cluster nodes: ", err)
	}

	ok, next, err = s.storage.NextTime()
	if err != nil {
		log.Fatal("Could not read next time from db: ", err)
	}

	return
}
