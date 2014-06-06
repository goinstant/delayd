package main

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type StubSender struct {
}

func (s StubSender) Send(e Entry) error {
	return nil
}

func TestNewStorageSetsTimer(t *testing.T) {
	dir, err := ioutil.TempDir("", "delayd-test")
	assert.Nil(t, err)
	defer os.Remove(dir)

	// create a storage instance to populate the db with an entry
	s, err := NewStorage(dir, StubSender{})
	assert.Nil(t, err)

	assert.False(t, s.timer.timerRunning)

	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}

	_, err = s.Add(e, 0)
	assert.Nil(t, err)

	s.Close()

	// create a storage instance to populate the db with an entry
	s, err = NewStorage(dir, StubSender{})
	defer s.Close()

	assert.Nil(t, err)
	assert.True(t, s.timer.timerRunning)
}

func innerTestAdd(t *testing.T, e Entry) {
	dir, err := ioutil.TempDir("", "delayd-test")
	assert.Nil(t, err)
	defer os.Remove(dir)

	s, err := NewStorage(dir, StubSender{})
	assert.Nil(t, err)
	defer s.Close()

	_, err = s.Add(e, 0)
	assert.Nil(t, err)

	uuids, entries, err := s.get(e.SendAt)
	assert.Nil(t, err)

	assert.Equal(t, len(entries), 1)
	assert.Equal(t, len(uuids), 1)
	assert.Equal(t, entries[0], e)
}

func TestAddNoKey(t *testing.T) {
	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}
	innerTestAdd(t, e)
}

func TestAddWithKey(t *testing.T) {
	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
		Key:    "user-key",
	}
	innerTestAdd(t, e)
}

func TestAddWithKeyReplacesExisting(t *testing.T) {
	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
		Key:    "user-key",
	}

	e2 := Entry{
		Target: "something-else",
		SendAt: time.Now().Add(time.Duration(110) * time.Minute),
		Key:    "user-key",
	}

	dir, err := ioutil.TempDir("", "delayd-test")
	assert.Nil(t, err)
	defer os.Remove(dir)

	s, err := NewStorage(dir, StubSender{})
	assert.Nil(t, err)
	defer s.Close()

	_, err = s.Add(e, 0)
	assert.Nil(t, err)

	_, err = s.Add(e2, 0)
	assert.Nil(t, err)

	// since e is before e2, this would return both.
	uuids, entries, err := s.get(e2.SendAt)
	assert.Nil(t, err)

	assert.Equal(t, len(entries), 1)
	assert.Equal(t, len(uuids), 1)
	assert.Equal(t, entries[0], e2)
}

func assertContains(t *testing.T, l []Entry, i Entry) {
	v := reflect.ValueOf(i)

	found := false
	for _, le := range l {
		if reflect.ValueOf(le) != v {
			found = true
			break
		}
	}

	if !found {
		t.Fail()
	}
}

func TestAddSameTime(t *testing.T) {
	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}

	e2 := Entry{
		Target: "something-else",
		SendAt: e.SendAt,
	}

	dir, err := ioutil.TempDir("", "delayd-test")
	assert.Nil(t, err)
	defer os.Remove(dir)

	s, err := NewStorage(dir, StubSender{})
	assert.Nil(t, err)
	defer s.Close()

	_, err = s.Add(e, 0)
	assert.Nil(t, err)

	_, err = s.Add(e2, 0)
	assert.Nil(t, err)

	// since e is before e2, this would return both.
	uuids, entries, err := s.get(e2.SendAt)
	assert.Nil(t, err)

	assert.Equal(t, len(uuids), 2)
	assert.Equal(t, len(entries), 2)

	// Entries don't come out in any particular order.
	assertContains(t, entries, e)
	assertContains(t, entries, e2)
}

func TestAddUpdatesVersion(t *testing.T) {
	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
		Key:    "user-key",
	}

	dir, err := ioutil.TempDir("", "delayd-test")
	assert.Nil(t, err)
	defer os.Remove(dir)

	s, err := NewStorage(dir, StubSender{})
	assert.Nil(t, err)
	defer s.Close()

	_, err = s.Add(e, 11)
	assert.Nil(t, err)

	version, err := s.Version()
	assert.Nil(t, err)
	assert.Equal(t, version, 11)
}

func innerTestRemove(t *testing.T, e Entry) {
	dir, err := ioutil.TempDir("", "delayd-test")
	assert.Nil(t, err)
	defer os.Remove(dir)

	s, err := NewStorage(dir, StubSender{})
	assert.Nil(t, err)
	defer s.Close()

	uuid, err := s.Add(e, 0)
	assert.Nil(t, err)

	err = s.remove(uuid)
	assert.Nil(t, err)

	uuids, entries, err := s.get(e.SendAt)
	assert.Nil(t, err)

	assert.Equal(t, len(entries), 0)
	assert.Equal(t, len(uuids), 0)
}

func TestRemoveNoKey(t *testing.T) {
	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}
	innerTestRemove(t, e)
}

func TestRemoveWithKey(t *testing.T) {
	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
		Key:    "user-key",
	}
	innerTestRemove(t, e)
}

func TestRemoveEntryNotFound(t *testing.T) {
	dir, err := ioutil.TempDir("", "delayd-test")
	assert.Nil(t, err)
	defer os.Remove(dir)

	s, err := NewStorage(dir, StubSender{})
	assert.Nil(t, err)
	defer s.Close()

	badUuid := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	err = s.remove(badUuid)
	assert.Error(t, err)
}

func TestRemoveSameTimeRemovesCorrectEntry(t *testing.T) {
	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}

	e2 := Entry{
		Target: "something-else",
		SendAt: e.SendAt,
	}

	dir, err := ioutil.TempDir("", "delayd-test")
	assert.Nil(t, err)
	defer os.Remove(dir)

	s, err := NewStorage(dir, StubSender{})
	assert.Nil(t, err)
	defer s.Close()

	_, err = s.Add(e, 0)
	assert.Nil(t, err)

	uuid, err := s.Add(e2, 0)
	assert.Nil(t, err)

	// remove only e2.
	err = s.remove(uuid)

	uuids, entries, err := s.get(e2.SendAt)
	assert.Nil(t, err)

	assert.Equal(t, len(uuids), 1)
	assert.Equal(t, len(entries), 1)

	assert.Equal(t, entries[0], e)
}

func TestNextTime(t *testing.T) {
	dir, err := ioutil.TempDir("", "delayd-test")
	assert.Nil(t, err)
	defer os.Remove(dir)

	s, err := NewStorage(dir, StubSender{})
	assert.Nil(t, err)
	defer s.Close()

	e := Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
		Key:    "user-key",
	}

	_, err = s.Add(e, 0)
	assert.Nil(t, err)

	ok, ts, err := s.nextTime()
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, ts, e.SendAt)
}

func TestNextTimeNoEntries(t *testing.T) {
	dir, err := ioutil.TempDir("", "delayd-test")
	assert.Nil(t, err)
	defer os.Remove(dir)

	s, err := NewStorage(dir, StubSender{})
	assert.Nil(t, err)
	defer s.Close()

	ok, _, err := s.nextTime()
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestVersionReturnsZeroIfNoEntries(t *testing.T) {
	dir, err := ioutil.TempDir("", "delayd-test")
	assert.Nil(t, err)
	defer os.Remove(dir)

	s, err := NewStorage(dir, StubSender{})
	assert.Nil(t, err)
	defer s.Close()

	version, err := s.Version()
	assert.Nil(t, err)
	assert.Equal(t, version, 0)
}
