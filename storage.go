package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"log"
	"os"
	"time"

	"github.com/szferi/gomdb"
)

type Storage struct {
	env  *mdb.Env
	dbi  *mdb.DBI
	send AmqpSender

	timerRunning bool
	timer        *time.Timer
	nextSend     time.Time
}

func NewStorage(send AmqpSender) (s *Storage, err error) {
	s = new(Storage)
	s.send = send

	s.env, err = mdb.NewEnv()
	if err != nil {
		return
	}

	storageDir := "db"
	err = os.MkdirAll(storageDir, 0755)
	if err != nil {
		return
	}

	// XXX deal with getting and deleting values with duplicate keys.
	err = s.env.Open(storageDir, 0, 0755)
	if err != nil {
		return
	}

	s.timer = time.NewTimer(time.Duration(24) * time.Hour)

	ok, t, err := s.nextTime()
	if err != nil {
		return
	}

	if ok {
		s.resetTimer(t)
	}

	go func() {
		for {
			// XXX change the timer to run a SEND command through raft, and emit on applying that command.
			t := <-s.timer.C
			entries, err := s.get(t)
			// XXX cleanup and shut down properly
			if err != nil {
				log.Fatal("Could not read entries from db: ", err)
			}

			log.Printf("Sending %d entries\n", len(entries))
			for _, e := range entries {
				s.send.C <- e
				err = s.remove(e)
				if err != nil {
					// XXX abort comitting for raft here, so it can retry.
					log.Fatal("Could not remove entry from db: ", err)
				}
			}

			ok, t, err = s.nextTime()
			if err != nil {
				log.Fatal("Could not read next time from db: ", err)
			}

			s.timerRunning = false
			if ok {
				s.resetTimer(t)
			}
		}
	}()

	return
}

func (s *Storage) Close() {
	s.env.Close()
	s.timer.Stop()
}

func (s *Storage) Add(e Entry, r []byte) error {
	txn, err := s.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}

	dbi, err := txn.DBIOpen(nil, 0)
	if err != nil {
		return err
	}

	k := uint64ToBytes(uint64(e.SendAt.UnixNano()))
	v := r

	txn.Put(dbi, k, v, 0)

	if !s.timerRunning || e.SendAt.Before(s.nextSend) {
		s.resetTimer(e.SendAt)
	}

	txn.Commit()

	return nil
}

func (s *Storage) get(t time.Time) (entries []Entry, err error) {
	txn, err := s.env.BeginTxn(nil, mdb.RDONLY)
	if err != nil {
		log.Println("Error creating transaction: ", err)
		return
	}
	defer txn.Abort()

	dbi, err := txn.DBIOpen(nil, 0)
	if err != nil {
		log.Println("Error opening dbi: ", err)
		return
	}
	defer s.env.DBIClose(dbi)

	cursor, err := txn.CursorOpen(dbi)
	if err != nil {
		log.Println("Error getting cursor: ", err)
		return
	}
	defer cursor.Close()

	sk := uint64(t.UnixNano())
	log.Println("Looking for: ", t, t.UnixNano())

	for {
		k, v, err := cursor.Get(nil, mdb.NEXT)
		if err == mdb.NotFound {
			break
		}
		if err != nil {
			panic(err)
		}

		kt := bytesToUint64(k)
		if kt > sk {
			break
		}

		entry := Entry{}
		dec := gob.NewDecoder(bytes.NewBuffer(v))
		err = dec.Decode(&entry)
		if err != nil {
			panic(err)
		}

		entries = append(entries, entry)
	}

	return
}

// get the next entry send time from the db
func (s *Storage) nextTime() (ok bool, t time.Time, err error) {
	ok = true
	txn, err := s.env.BeginTxn(nil, mdb.RDONLY)
	if err != nil {
		log.Println("Error creating transaction: ", err)
		return
	}
	defer txn.Abort()

	dbi, err := txn.DBIOpen(nil, 0)
	if err != nil {
		log.Println("Error opening dbi: ", err)
		return
	}
	defer s.env.DBIClose(dbi)

	cursor, err := txn.CursorOpen(dbi)
	if err != nil {
		log.Println("Error getting cursor: ", err)
		return
	}
	defer cursor.Close()

	k, _, err := cursor.Get(nil, mdb.FIRST)
	if err == mdb.NotFound {
		err = nil
		ok = false
		return
	}
	if err != nil {
		log.Println("Error reading next time from db: ", err)
		return
	}

	t = time.Unix(0, int64(bytesToUint64(k)))

	return
}

// Remove an emitted entry from the db
func (s *Storage) remove(e Entry) error {
	txn, err := s.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}

	dbi, err := txn.DBIOpen(nil, 0)
	if err != nil {
		return err
	}

	k := uint64ToBytes(uint64(e.SendAt.UnixNano()))

	err = txn.Del(dbi, k, nil)
	if err != nil {
		return err
	}

	txn.Commit()

	return nil
}

func (s *Storage) resetTimer(t time.Time) {
	if t.Before(time.Now()) {
		t = time.Now()
	}
	log.Println("Setting next timer for: ", t)
	s.timerRunning = true
	s.timer.Reset(t.Sub(time.Now()))
	s.nextSend = t
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts a uint to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
