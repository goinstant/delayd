package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/codegangsta/cli"

	"github.com/nabeken/delayd"
)

// this is filled in at build time
var version string

type stopable interface {
	Stop()
}

// Context is an interface for command line arguments context.  Useful for replacing the lib
// when running a test
type Context interface {
	String(str string) string
	Bool(str string) bool
	Int(str string) int
}

// installSigHandler installs a signal handler to shutdown gracefully for ^C and kill
func installSigHandler(s stopable) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-ch:
			s.Stop()
			os.Exit(0)
		}
	}()
}

func execute(c *cli.Context) {
	delayd.Info("Starting delayd")

	config, err := loadConfig(c.String("config"))
	if err != nil {
		delayd.Fatal("Unable to read config file: ", err)
	}

	s, err := delayd.NewServer(config)
	if err != nil {
		panic(err)
	}
	installSigHandler(s)
	s.Run()
}

func main() {
	app := cli.NewApp()
	app.Name = "delayd"
	app.Usage = "available setTimeout()"
	app.Version = version

	flags := []cli.Flag{
		cli.StringFlag{
			Name: "config, c", Value: "/etc/delayd.toml", Usage: "config file"},
	}

	app.Commands = []cli.Command{
		{
			Name:        "server",
			ShortName:   "serv",
			Usage:       "Spawn a Delayd Server",
			Description: "Delay Daemon for replicated `setTimeout()`",
			Action:      execute,
			Flags:       flags,
		},
	}

	app.Run(os.Args)
}

// loadConfig loads delayd's toml configuration
func loadConfig(path string) (config delayd.Config, err error) {
	_, err = toml.DecodeFile(path, &config)
	return
}
