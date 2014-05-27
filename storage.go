package main

import (
	"os"

	"github.com/szferi/gomdb"
)

type Storage struct {
	env *mdb.Env
	dbi *mdb.DBI
}

func NewStorage() (s *Storage, err error) {
	s = new(Storage)
	s.env, err = mdb.NewEnv()
	if err != nil {
		return
	}

	err = os.MkdirAll("storage", 755)
	if err != nil {
		return
	}

	err = s.env.Open("storage", 0, 0644)
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

	return nil
}
