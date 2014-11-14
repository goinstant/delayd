package delayd

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	dummyUUID, _  = newUUID()
	dummyUUID2, _ = newUUID()
)

func innerTestAdd(t *testing.T, e *Entry) {
	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	err = s.Add(dummyUUID, e)
	assert.Nil(t, err)

	uuids, entries, err := s.Get(e.SendAt)
	assert.Nil(t, err)

	assert.Equal(t, len(entries), 1)
	assert.Equal(t, len(uuids), 1)
	assert.Equal(t, entries[0], e)
}

func TestAddNoKey(t *testing.T) {
	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}
	innerTestAdd(t, e)
}

func TestAddWithKey(t *testing.T) {
	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
		Key:    "user-key",
	}
	innerTestAdd(t, e)
}

func TestAddWithKeyReplacesExisting(t *testing.T) {
	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
		Key:    "user-key",
	}

	e2 := &Entry{
		Target: "something-else",
		SendAt: time.Now().Add(time.Duration(110) * time.Minute),
		Key:    "user-key",
	}

	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	err = s.Add(dummyUUID, e)
	assert.Nil(t, err)

	err = s.Add(dummyUUID2, e2)
	assert.Nil(t, err)

	// since e is before e2, this would return both.
	uuids, entries, err := s.Get(e2.SendAt)
	assert.Nil(t, err)

	assert.Equal(t, len(entries), 1)
	assert.Equal(t, len(uuids), 1)
	assert.Equal(t, entries[0], e2)
}

func assertContains(t *testing.T, l []*Entry, i *Entry) {
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
	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}

	e2 := &Entry{
		Target: "something-else",
		SendAt: e.SendAt,
	}

	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	err = s.Add(dummyUUID, e)
	assert.Nil(t, err)

	err = s.Add(dummyUUID2, e2)
	assert.Nil(t, err)

	// since e is before e2, this would return both.
	uuids, entries, err := s.Get(e2.SendAt)
	assert.Nil(t, err)

	assert.Equal(t, len(uuids), 2)
	assert.Equal(t, len(entries), 2)

	// Entries don't come out in any particular order.
	assertContains(t, entries, e)
	assertContains(t, entries, e2)
}

func innerTestRemove(t *testing.T, e *Entry) {
	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	err = s.Add(dummyUUID, e)
	assert.Nil(t, err)

	err = s.Remove(dummyUUID)
	assert.Nil(t, err)

	uuids, entries, err := s.Get(e.SendAt)
	assert.Nil(t, err)

	assert.Equal(t, len(entries), 0)
	assert.Equal(t, len(uuids), 0)
}

func TestRemoveNoKey(t *testing.T) {
	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}
	innerTestRemove(t, e)
}

func TestRemoveWithKey(t *testing.T) {
	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
		Key:    "user-key",
	}
	innerTestRemove(t, e)
}

func TestRemoveEntryNotFound(t *testing.T) {
	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	badUUID := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	err = s.Remove(badUUID)
	assert.Error(t, err)
}

func TestRemoveSameTimeRemovesCorrectEntry(t *testing.T) {
	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}

	e2 := &Entry{
		Target: "something-else",
		SendAt: e.SendAt,
	}

	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	err = s.Add(dummyUUID, e)
	assert.Nil(t, err)

	err = s.Add(dummyUUID2, e2)
	assert.Nil(t, err)

	// remove only e2.
	err = s.Remove(dummyUUID2)

	uuids, entries, err := s.Get(e2.SendAt)
	assert.Nil(t, err)

	assert.Equal(t, len(uuids), 1)
	assert.Equal(t, len(entries), 1)

	assert.Equal(t, entries[0], e)
}

func TestNextTime(t *testing.T) {
	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
		Key:    "user-key",
	}

	err = s.Add(dummyUUID, e)
	assert.Nil(t, err)

	ok, ts, err := s.NextTime()
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, ts, e.SendAt)
}

func TestNextTimeNoEntries(t *testing.T) {
	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	ok, _, err := s.NextTime()
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestChannelSendsNextTimeOnAdd(t *testing.T) {
	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}
	s.Add(dummyUUID, e)

	next := <-s.C
	assert.Equal(t, next, e.SendAt)

	e2 := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(1) * time.Minute),
	}
	s.Add(dummyUUID2, e2)

	next = <-s.C
	assert.Equal(t, next, e2.SendAt)
}

func TestChannelSendNextTimeIfLater(t *testing.T) {
	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(1) * time.Minute),
	}
	s.Add(dummyUUID, e)

	next := <-s.C
	assert.Equal(t, next, e.SendAt)

	e2 := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}
	s.Add(dummyUUID2, e2)

	next = <-s.C
	assert.Equal(t, next, e.SendAt)
}

func TestChannelSendsNextTimeOnRemove(t *testing.T) {
	s, err := NewStorage()
	assert.Nil(t, err)
	defer s.Close()

	e := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(100) * time.Minute),
	}
	s.Add(dummyUUID, e)

	next := <-s.C
	assert.Equal(t, next, e.SendAt)

	e2 := &Entry{
		Target: "something",
		SendAt: time.Now().Add(time.Duration(1) * time.Minute),
	}
	s.Add(dummyUUID2, e2)

	next = <-s.C
	assert.Equal(t, next, e2.SendAt)

	s.Remove(dummyUUID2)

	next = <-s.C
	assert.Equal(t, next, e.SendAt)
}
