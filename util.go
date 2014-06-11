package main

import (
	"crypto/rand"
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
