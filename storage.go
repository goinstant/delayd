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

// Storage is the database backend for persisting Entries via LMDB, and triggering
// entry emission.
type Storage struct {
	env *mdb.Env
	dbi *mdb.DBI
}

// NewStorage creates a new Storage instance. prefix is the base directory where
// data is written on-disk.
func NewStorage(prefix string) (s *Storage, err error) {
	s = new(Storage)

	err = s.initDB(prefix)
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

	// XXX check schema version first (once we change it). See #26
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

// Close gracefully shuts down a storage instance. Before calling it, ensure
// that all in-flight requests have been processed.
func (s *Storage) Close() {
	// we trust that mdb will wait for transactions to complete (also be clean)
	s.env.Close()
}

// startTxn is used to start a transaction and open all the associated sub-databases
func (s *Storage) startTxn(readonly bool, open ...string) (txn *mdb.Txn, dbis []mdb.DBI, err error) {
	var txnFlags uint
	var dbiFlags uint
	if readonly {
		txnFlags |= mdb.RDONLY
	} else {
		dbiFlags |= mdb.CREATE
	}

	txn, err = s.env.BeginTxn(nil, txnFlags)
	if err != nil {
		return
	}

	for _, name := range open {
		// Allow duplicate entries for the same time
		realFlags := dbiFlags
		if name == timeDB {
			realFlags |= mdb.DUPSORT
		}

		var dbi mdb.DBI
		dbi, err = txn.DBIOpen(name, realFlags)
		if err != nil {
			txn.Abort()
			return
		}
		dbis = append(dbis, dbi)
	}

	return
}

// Add an Entry to the database. index is the raft log entry's index that
// triggered this add. It is used to ensure we do not apply the same command
// twice on a restart.
func (s *Storage) Add(e Entry, index uint64) (uuid []byte, err error) {
	uuid, err = newUUID()
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

			err = s.innerRemove(txn, dbis, ouuid)
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

	err = txn.Commit()
	return
}

// Get returns all entries that occur at or before the provided time
func (s *Storage) Get(t time.Time) (uuids [][]byte, entries []Entry, err error) {
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
		var k, uuid, v []byte
		k, uuid, err = cursor.Get(nil, mdb.NEXT)
		if err == mdb.NotFound {
			err = nil
			break
		}
		if err != nil {
			return
		}

		kt := bytesToUint64(k)
		if kt > sk {
			err = nil
			break
		}

		v, err = txn.Get(dbis[1], uuid)
		if err != nil {
			return
		}

		var entry Entry
		entry, err = entryFromBytes(v)
		if err != nil {
			return
		}

		entries = append(entries, entry)
		uuids = append(uuids, uuid)
	}

	return
}

// NextTime gets the next entry send time from the db
func (s *Storage) NextTime() (ok bool, t time.Time, err error) {
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

func (s *Storage) innerRemove(txn *mdb.Txn, dbis []mdb.DBI, uuid []byte) (err error) {
	be, err := txn.Get(dbis[1], uuid)
	if err != nil {
		log.Println("Could not read entry: ", err)
		return
	}

	e, err := entryFromBytes(be)
	if err != nil {
		log.Println("Could not parse entry: ", err)
		return
	}

	k := uint64ToBytes(uint64(e.SendAt.UnixNano()))
	err = txn.Del(dbis[0], k, uuid)
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

// Remove an emitted entry from the db. uuid is the Entry's UUID. index is the
// entry's raft log index.
func (s *Storage) Remove(uuid []byte, index uint64) (err error) {
	txn, dbis, err := s.startTxn(false, timeDB, entryDB, keyDB, metaDB)
	if err != nil {
		return
	}

	err = s.innerRemove(txn, dbis, uuid)
	if err != nil {
		txn.Abort()
		return
	}

	err = txn.Put(dbis[3], []byte("version"), uint64ToBytes(index), 0)
	if err != nil {
		txn.Abort()
		return
	}

	txn.Commit()
	return
}

// Version returns the current raft index version as stored in the db.
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
