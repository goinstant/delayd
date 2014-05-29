package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
	"log"
	"os"
	"time"

	"github.com/szferi/gomdb"
)

const (
	timeDB  = "time"    // map of emit time to entry uuid
	keyDB   = "keys"    // map of user provided key to entry uuid. optional
	entryDB = "entries" // map of uuid to entry
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

	// 3 sub dbs: Entries, time index, and key index
	err = s.env.SetMaxDBs(mdb.DBI(3))
	if err != nil {
		return
	}

	// XXX deal with getting and deleting values with duplicate keys.
	err = s.env.Open(storageDir, 0, 0755)
	if err != nil {
		return
	}

	// Initialize sub dbs
	txn, dbis, err := s.startTxn(false, timeDB, entryDB)
	if err != nil {
		s.abortTxn(txn, dbis)
		return
	}

	err = txn.Commit()
	if err != nil {
		s.closeDBIs(dbis)
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

	go s.startTimerLoop()
	return
}

func (s *Storage) Close() {
	s.env.Close()
	s.timer.Stop()
}

func (s *Storage) startTimerLoop() {
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

		ok, t, err := s.nextTime()
		if err != nil {
			log.Fatal("Could not read next time from db: ", err)
		}

		s.timerRunning = false
		if ok {
			s.resetTimer(t)
		}
	}
}

// startTxn is used to start a transaction and open all the associated sub-databases
func (s *Storage) startTxn(readonly bool, open ...string) (*mdb.Txn, []mdb.DBI, error) {
	var txnFlags uint = 0
	var dbiFlags uint = 0
	if readonly {
		txnFlags |= mdb.RDONLY
	} else {
		dbiFlags |= mdb.CREATE
	}

	txn, err := s.env.BeginTxn(nil, txnFlags)
	if err != nil {
		return nil, nil, err
	}

	var dbs []mdb.DBI
	for _, name := range open {
		dbi, err := txn.DBIOpen(&name, dbiFlags)
		if err != nil {
			txn.Abort()
			return nil, nil, err
		}
		dbs = append(dbs, dbi)
	}

	return txn, dbs, nil
}

func (s *Storage) closeDBIs(dbis []mdb.DBI) {
	for _, dbi := range dbis {
		s.env.DBIClose(dbi)
	}
}

func (s *Storage) abortTxn(txn *mdb.Txn, dbis []mdb.DBI) {
	txn.Abort()
	s.closeDBIs(dbis)
}

func (s *Storage) Add(e Entry, r []byte) (err error) {
	txn, dbis, err := s.startTxn(false, timeDB, entryDB)
	if err != nil {
		return
	}
	defer s.closeDBIs(dbis)

	uuid, err := newUUID()
	if err != nil {
		txn.Abort()
		return
	}

	k := uint64ToBytes(uint64(e.SendAt.UnixNano()))

	err = txn.Put(dbis[0], k, uuid, 0)
	if err != nil {
		txn.Abort()
		return
	}
	err = txn.Put(dbis[1], uuid, r, 0)

	if !s.timerRunning || e.SendAt.Before(s.nextSend) {
		s.resetTimer(e.SendAt)
	}

	err = txn.Commit()
	return
}

func (s *Storage) get(t time.Time) (entries []Entry, err error) {
	txn, dbis, err := s.startTxn(true, timeDB, entryDB)
	if err != nil {
		log.Println("Error creating transaction: ", err)
		return
	}
	defer s.abortTxn(txn, dbis)

	cursor, err := txn.CursorOpen(dbis[0])
	if err != nil {
		log.Println("Error getting cursor: ", err)
		return
	}
	defer cursor.Close()

	sk := uint64(t.UnixNano())
	log.Println("Looking for: ", t, t.UnixNano())

	for {
		k, uuid, err := cursor.Get(nil, mdb.NEXT)
		if err == mdb.NotFound {
			break
		}
		if err != nil {
			// XXX don't panic
			panic(err)
		}

		kt := bytesToUint64(k)
		if kt > sk {
			break
		}

		v, err := txn.Get(dbis[1], uuid)
		if err != nil {
			// XXX don't panic
			panic(err)
		}

		entry := Entry{}
		dec := gob.NewDecoder(bytes.NewBuffer(v))
		err = dec.Decode(&entry)
		if err != nil {
			// XXX don't panic
			panic(err)
		}

		entries = append(entries, entry)
	}

	return
}

// get the next entry send time from the db
func (s *Storage) nextTime() (ok bool, t time.Time, err error) {
	ok = true
	txn, dbis, err := s.startTxn(true, timeDB)
	if err != nil {
		log.Println("Error creating transaction: ", err)
		return
	}
	defer s.abortTxn(txn, dbis)

	cursor, err := txn.CursorOpen(dbis[0])
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
func (s *Storage) remove(e Entry) (err error) {
	txn, dbis, err := s.startTxn(false, timeDB, entryDB)
	if err != nil {
		return
	}
	defer s.closeDBIs(dbis)

	k := uint64ToBytes(uint64(e.SendAt.UnixNano()))

	uuid, err := txn.Get(dbis[0], k)
	if err != nil {
		return
	}

	err = txn.Del(dbis[0], k, nil)
	if err != nil {
		return
	}

	err = txn.Del(dbis[1], uuid, nil)
	if err != nil {
		return
	}

	err = txn.Commit()
	return
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

func newUUID() (uuid []byte, err error) {
	uuid = make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) {
		err = errors.New("Could not create uuid")
	}
	if err != nil {
		return
	}

	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return
}
