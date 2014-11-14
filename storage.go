package delayd

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/armon/gomdb"
)

const (
	timeDB  = "time"    // map of emit time to entry uuid
	keyDB   = "keys"    // map of user provided key to entry uuid. optional
	entryDB = "entries" // map of uuid to entry

	dbMaxMapSize32bit uint64 = 512 * 1024 * 1024       // 512MB maximum size
	dbMaxMapSize64bit uint64 = 32 * 1024 * 1024 * 1024 // 32GB maximum size

	// Optimize our flags for speed over safety. Raft handles durable storage.
	dbFlags uint = mdb.NOMETASYNC | mdb.NOSYNC | mdb.NOTLS
)

var allDBs = []string{timeDB, keyDB, entryDB}

// Storage is the database backend for persisting Entries via LMDB, and triggering
// entry emission.
type Storage struct {
	env *mdb.Env
	dbi *mdb.DBI
	dir string

	C <-chan time.Time
	c chan<- time.Time
}

// NewStorage creates a new Storage instance. prefix is the base directory where
// data is written on-disk.
func NewStorage() (*Storage, error) {
	s := &Storage{}
	if err := s.initDB(); err != nil {
		return nil, err
	}

	c := make(chan time.Time, 10)
	s.C = c
	s.c = c

	return s, nil
}

func (s *Storage) initDB() error {
	env, err := mdb.NewEnv()
	if err != nil {
		return err
	}
	s.env = env

	// Set the maximum db size based on 32/64bit
	dbSize := dbMaxMapSize32bit
	if runtime.GOARCH == "amd64" {
		dbSize = dbMaxMapSize64bit
	}

	// Increase the maximum map size
	if err := s.env.SetMapSize(dbSize); err != nil {
		return err
	}

	dir, err := ioutil.TempDir("", "delayd")
	if err != nil {
		return err
	}
	s.dir = dir

	storageDir := filepath.Join(s.dir, "db")
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		return err
	}
	Debug("storage: created temporary storage directory:", storageDir)

	// 3 sub dbs: Entries, time index, and key index
	if err := s.env.SetMaxDBs(mdb.DBI(3)); err != nil {
		return err
	}

	if err := s.env.Open(storageDir, dbFlags, 0755); err != nil {
		return err
	}

	// Initialize sub dbs
	txn, _, err := s.startTxn(false, allDBs...)
	if err != nil {
		txn.Abort()
		return err
	}

	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

// Close gracefully shuts down a storage instance. Before calling it, ensure
// that all in-flight requests have been processed.
func (s *Storage) Close() {
	// mdb will segfault if any transactions are in process when this is called,
	// so ensure users of storage are stopped first.
	s.env.Close()
	os.RemoveAll(s.dir)
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

	dbis := []mdb.DBI{}
	for _, name := range open {
		// Allow duplicate entries for the same time
		realFlags := dbiFlags
		if name == timeDB {
			realFlags |= mdb.DUPSORT
		}

		dbi, err := txn.DBIOpen(name, realFlags)
		if err != nil {
			txn.Abort()
			return nil, nil, err
		}
		dbis = append(dbis, dbi)
	}

	return txn, dbis, nil
}

// Add an Entry to the database.
func (s *Storage) Add(uuid []byte, e Entry) error {
	txn, dbis, err := s.startTxn(false, timeDB, entryDB, keyDB)
	if err != nil {
		return err
	}

	if e.Key != "" {
		Debug("storage: adding entry has key:", e.Key)

		ouuid, err := txn.Get(dbis[2], []byte(e.Key))
		if err != nil && err != mdb.NotFound {
			txn.Abort()
			return err
		}

		if err == nil {
			Debug("storage: exising key found; removing.")

			err := s.innerRemove(txn, dbis, ouuid)
			if err != nil {
				txn.Abort()
				return err
			}
		}

		err = txn.Put(dbis[2], []byte(e.Key), uuid, 0)
		if err != nil {
			txn.Abort()
			return err
		}
	}

	k := uint64ToBytes(uint64(e.SendAt.UnixNano()))
	err = txn.Put(dbis[0], k, uuid, 0)
	if err != nil {
		txn.Abort()
		return err
	}

	b, err := e.ToBytes()
	if err != nil {
		txn.Abort()
		return err
	}

	err = txn.Put(dbis[1], uuid, b, 0)
	if err != nil {
		txn.Abort()
		return err
	}

	ok, t, err := s.innerNextTime(txn, dbis[0])
	if err != nil {
		txn.Abort()
		return err
	}

	err = txn.Commit()
	if err == nil && ok {
		s.c <- t
	}

	return nil
}

func (s *Storage) innerGet(t time.Time, all bool) ([][]byte, []Entry, error) {
	txn, dbis, err := s.startTxn(true, timeDB, entryDB)
	if err != nil {
		Error("storage: error creating transaction:", err)
		return nil, nil, err
	}
	defer txn.Abort()

	cursor, err := txn.CursorOpen(dbis[0])
	if err != nil {
		Error("storage: error getting cursor for get entry:", err)
		return nil, nil, err
	}
	defer cursor.Close()

	sk := uint64(t.UnixNano())
	Debug("storage: looking for:", t, t.UnixNano())

	uuids := [][]byte{}
	entries := []Entry{}
	for {
		k, uuid, err := cursor.Get(nil, mdb.NEXT)
		if err == mdb.NotFound {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		if kt := bytesToUint64(k); !all && kt > sk {
			// These are no more entries since keys are always sorted
			break
		}

		v, err := txn.Get(dbis[1], uuid)
		if err != nil {
			return nil, nil, err
		}

		entry, err := entryFromBytes(v)
		if err != nil {
			return nil, nil, err
		}

		entries = append(entries, entry)
		uuids = append(uuids, uuid)
	}

	return uuids, entries, nil
}

// Get returns all entries that occur at or before the provided time
func (s *Storage) Get(t time.Time) (uuids [][]byte, entries []Entry, err error) {
	return s.innerGet(t, false)
}

// GetAll returns every entry in storage
func (s *Storage) GetAll() (uuids [][]byte, entries []Entry, err error) {
	return s.innerGet(time.Now(), true)
}

func (s *Storage) innerNextTime(txn *mdb.Txn, dbi mdb.DBI) (bool, time.Time, error) {
	cursor, err := txn.CursorOpen(dbi)
	if err != nil {
		Error("storage: error getting cursor for next time:", err)
		return false, time.Time{}, err
	}
	defer cursor.Close()

	k, _, err := cursor.Get(nil, mdb.FIRST)
	if err == mdb.NotFound {
		return false, time.Time{}, nil
	}
	if err != nil {
		Error("storage: error reading next time from db:", err)
		return false, time.Time{}, err
	}

	return true, time.Unix(0, int64(bytesToUint64(k))), nil
}

// NextTime gets the next entry send time from the db
func (s *Storage) NextTime() (bool, time.Time, error) {
	txn, dbis, err := s.startTxn(true, timeDB)
	if err != nil {
		Error("storage: error creating transaction:", err)
		return false, time.Time{}, err
	}
	defer txn.Abort()

	return s.innerNextTime(txn, dbis[0])
}

func (s *Storage) innerRemove(txn *mdb.Txn, dbis []mdb.DBI, uuid []byte) error {
	be, err := txn.Get(dbis[1], uuid)
	if err != nil {
		Error("storage: could not read entry:", err)
		return err
	}

	e, err := entryFromBytes(be)
	if err != nil {
		Error("storage: could not parse entry:", err)
		return err
	}

	k := uint64ToBytes(uint64(e.SendAt.UnixNano()))
	if err := txn.Del(dbis[0], k, uuid); err != nil {
		Error("storage: could not delete from time series:", err)
		return err
	}

	if err := txn.Del(dbis[1], uuid, nil); err != nil {
		Error("storage: could not delete entry:", err)
		return err
	}

	// check if the key exists before deleting.
	cursor, err := txn.CursorOpen(dbis[2])
	if err != nil {
		Error("storage: error getting cursor for keys:", err)
		return err
	}
	defer cursor.Close()

	if e.Key == "" {
		return nil
	}

	_, _, err = cursor.Get([]byte(e.Key), mdb.FIRST)
	if err == mdb.NotFound {
		return nil
	}
	if err != nil {
		Error("storage: error reading cursor:", err)
		return err
	}

	err = txn.Del(dbis[2], []byte(e.Key), nil)
	if err != nil {
		Error("storage: could not delete from keys:", err)
	}
	return err
}

// Remove an emitted entry from the db. uuid is the Entry's UUID.
func (s *Storage) Remove(uuid []byte) error {
	txn, dbis, err := s.startTxn(false, timeDB, entryDB, keyDB)
	if err != nil {
		return err
	}

	if err = s.innerRemove(txn, dbis, uuid); err != nil {
		txn.Abort()
		return err
	}

	ok, t, err := s.innerNextTime(txn, dbis[0])
	if err != nil {
		txn.Abort()
		return err
	}

	err = txn.Commit()
	if err == nil && ok {
		s.c <- t
	}
	return err
}
