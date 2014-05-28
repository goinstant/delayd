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

	err = s.env.Open(storageDir, 0, 0755)
	if err != nil {
		return
	}

	s.timerRunning = true
	s.timer = time.NewTimer(time.Duration(3) * time.Second)
	s.nextSend = time.Now().Add(time.Duration(3) * time.Second)
	go func() {
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
		s.timer.Reset(time.Now().Sub(e.SendAt))
		s.nextSend = e.SendAt
		s.timerRunning = true
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
