package main

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
)

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

// Converts bytes to an integer
func bytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// Converts a uint to a byte slice
func uint32ToBytes(u uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, u)
	return buf
}
