package main

import (
	"log"
	"os"
	"time"

	"github.com/szferi/gomdb"
)

type Storage struct {
	env  *mdb.Env
	dbi  *mdb.DBI
	send AmqpSender
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

	return
}

func (s *Storage) Close() {
	s.env.Close()
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

	k, err := e.SendAt.MarshalBinary()
	if err != nil {
		return err
	}

	v := r

	txn.Put(dbi, k, v, 0)
	txn.Commit()

	// XXX change the timer to run a SEND command through raft, and emit on applying that command.
	go func() {
		timer := time.NewTimer(e.SendAt.Sub(time.Now()))
		_ = <-timer.C
		log.Println("Sending entry: ", e)
		s.send.C <- e
	}()

	return nil
}
