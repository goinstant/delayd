package main

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	unknownAsk = "unknown option asked for"
)

type MockClientContext struct{}

func (m MockClientContext) String(ask string) string {
	switch ask {
	case "exchange":
		return "delayd-test"
	case "key":
		return "delayd-key"
	case "file":
		return "tests/in.txt"
	case "out":
		return "tests/out.txt"
	case "config":
		return "delayd.toml"
	default:
		panic(unknownAsk)
	}
}

func (m MockClientContext) Bool(ask string) bool {
	switch ask {
	case "repl":
		return false
	case "no-wait":
		return false
	default:
		panic(unknownAsk)
	}
}

func (m MockClientContext) Int(ask string) int {
	switch ask {
	case "delay":
		return 50
	default:
		panic(unknownAsk)
	}
}

func getFileAsString(path string) string {
	dat, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	return string(dat[:])
}

func cleanup(path string) {
	err := os.Remove(path)
	if err != nil {
		panic(err)
	}
}

func TestInAndOut(t *testing.T) {
	m := MockClientContext{}
	conf, err := loadConfig(m)

	// travis can't make a dir under /var/lib, but we can just put it
	// in its home dir
	if os.Getenv("TRAVIS") != "" {
		conf.DataDir = "delayd"
	}

	if err != nil {
		panic(err)
	}

	s := Server{}
	go s.Run(conf)

	c, err := NewClient(m)
	if err != nil {
		panic(err)
	}

	c.Run(conf)
	c.Stop()
	s.Stop()

	f1 := getFileAsString("tests/in.txt")
	f2 := getFileAsString("tests/out.txt")

	assert.Equal(t, f1, f2)

	cleanup("tests/out.txt")
}
