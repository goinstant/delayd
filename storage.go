package main

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"path"
	"time"

	"github.com/armon/gomdb"
)

const (
	metaDB  = "meta"    // Meta info (schema version, last applied index)
	timeDB  = "time"    // map of emit time to entry uuid
	keyDB   = "keys"    // map of user provided key to entry uuid. optional
	entryDB = "entries" // map of uuid to entry

	schemaVer = 1 // On-disk storage format version
)

type Storage struct {
	env  *mdb.Env
	dbi  *mdb.DBI
	send Sender

	timerRunning bool
	timer        *time.Timer
	nextSend     time.Time
}

func NewStorage(prefix string, send Sender) (s *Storage, err error) {
	s = new(Storage)
	s.send = send

	err = s.initDB(prefix)
	if err != nil {
		return
	}

	err = s.initTimer()
	if err != nil {
		return
	}

	return
}

func (s *Storage) initDB(prefix string) (err error) {
	s.env, err = mdb.NewEnv()
	if err != nil {
		return
	}

	storageDir := path.Join(prefix, "db")
	err = os.MkdirAll(storageDir, 0755)
	if err != nil {
		return
	}

	// 3 sub dbs: Entries, time index, and key index
	err = s.env.SetMaxDBs(mdb.DBI(4))
	if err != nil {
		return
	}

	// XXX deal with getting and deleting values with duplicate keys.
	err = s.env.Open(storageDir, 0, 0755)
	if err != nil {
		return
	}

	// Initialize sub dbs
	txn, dbis, err := s.startTxn(false, metaDB, timeDB, entryDB, keyDB)
	if err != nil {
		txn.Abort()
		return
	}

	// XXX check schema version first (once we change it)
	err = txn.Put(dbis[0], []byte("schema"), uint64ToBytes(schemaVer), 0)
	if err != nil {
		txn.Abort()
		return
	}

	err = txn.Commit()
	if err != nil {
		return
	}

	return
}

func (s *Storage) initTimer() (err error) {
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
			err = s.send.Send(e)
			if err != nil {
				log.Fatal("Could not send entry: ", err)
			}

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
	var txnFlags uint
	var dbiFlags uint
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
		dbi, err := txn.DBIOpen(name, dbiFlags)
		if err != nil {
			txn.Abort()
			return nil, nil, err
		}
		dbs = append(dbs, dbi)
	}

	return txn, dbs, nil
}

func (s *Storage) Add(e Entry, index uint64) (err error) {
	uuid, err := newUUID()
	if err != nil {
		return
	}

	txn, dbis, err := s.startTxn(false, timeDB, entryDB, keyDB, metaDB)
	if err != nil {
		return
	}

	if e.Key != "" {
		log.Println("Entry has key: ", e.Key)

		var ouuid []byte
		ouuid, err = txn.Get(dbis[2], []byte(e.Key))
		if err != nil && err != mdb.NotFound {
			txn.Abort()
			return
		}

		if err == nil {
			log.Println("Exising key found; removing.")
			var oeb []byte
			oeb, err = txn.Get(dbis[1], ouuid)
			if err != nil {
				txn.Abort()
				return
			}

			var oe Entry
			oe, err = entryFromBytes(oeb)
			if err != nil {
				txn.Abort()
				return
			}

			err = s.innerRemove(txn, dbis, oe)
			if err != nil {
				txn.Abort()
				return
			}
		}

		err = txn.Put(dbis[2], []byte(e.Key), uuid, 0)
		if err != nil {
			txn.Abort()
			return
		}
	}

	k := uint64ToBytes(uint64(e.SendAt.UnixNano()))
	err = txn.Put(dbis[0], k, uuid, 0)
	if err != nil {
		txn.Abort()
		return
	}

	b, err := e.ToBytes()
	if err != nil {
		txn.Abort()
		return
	}

	err = txn.Put(dbis[1], uuid, b, 0)
	if err != nil {
		txn.Abort()
		return
	}

	err = txn.Put(dbis[3], []byte("version"), uint64ToBytes(index), 0)
	if err != nil {
		txn.Abort()
		return
	}

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
	defer txn.Abort()

	cursor, err := txn.CursorOpen(dbis[0])
	if err != nil {
		log.Println("Error getting cursor for get entry: ", err)
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

		entry, err := entryFromBytes(v)
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
	defer txn.Abort()

	cursor, err := txn.CursorOpen(dbis[0])
	if err != nil {
		log.Println("Error getting cursor for next time: ", err)
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

func (s *Storage) innerRemove(txn *mdb.Txn, dbis []mdb.DBI, e Entry) (err error) {
	k := uint64ToBytes(uint64(e.SendAt.UnixNano()))

	uuid, err := txn.Get(dbis[0], k)
	if err != nil {
		log.Println("Could not read uuid: ", err)
		return
	}

	err = txn.Del(dbis[0], k, nil)
	if err != nil {
		log.Println("Could not delete from time series: ", err)
		return
	}

	err = txn.Del(dbis[1], uuid, nil)
	if err != nil {
		log.Println("Could not delete entry: ", err)
		return
	}

	// check if the key exists before deleting.
	cursor, err := txn.CursorOpen(dbis[2])
	if err != nil {
		log.Println("Error getting cursor for keys: ", err)
		return
	}
	defer cursor.Close()

	_, _, err = cursor.Get([]byte(e.Key), mdb.FIRST)
	if err == mdb.NotFound {
		err = nil
		return
	} else if err != nil {
		log.Println("Error reading cursor: ", err)
		return
	}

	err = txn.Del(dbis[2], []byte(e.Key), nil)
	if err != nil {
		log.Println("Could not delete from keys: ", err)
		return
	}

	return
}

// Remove an emitted entry from the db
func (s *Storage) remove(e Entry) (err error) {
	txn, dbis, err := s.startTxn(false, timeDB, entryDB, keyDB)
	if err != nil {
		return
	}

	err = s.innerRemove(txn, dbis, e)
	if err != nil {
		txn.Abort()
		return
	}

	txn.Commit()
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

// Return the current raft index version as stored in the db.
// Use this to determine if actions should be performed or not.
func (s *Storage) Version() (version uint64, err error) {
	txn, dbis, err := s.startTxn(true, metaDB)
	if err != nil {
		log.Println("Error creating transaction: ", err)
		return
	}
	defer txn.Abort()

	cursor, err := txn.CursorOpen(dbis[0])
	if err != nil {
		log.Println("Error getting cursor for version: ", err)
		return
	}
	defer cursor.Close()

	_, b, err := cursor.Get([]byte("version"), mdb.SET_KEY)
	if err == mdb.NotFound {
		err = nil
		return
	} else if err != nil {
		log.Println("Error reading cursor for version: ", err)
		return
	}

	version = bytesToUint64(b)
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
