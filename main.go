package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/codegangsta/cli"
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

func sigHandler(s stopable) {
	// graceful shutdown for ^C and kill
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		for _ = range ch {
			s.Stop()
			os.Exit(0)
		}
	}()
}

func executeCli(c *cli.Context) {
	Info("Starting delayd.Client")

	config, err := loadConfig(c)
	if err != nil {
		Fatal("Unable to read config file: ", err)
	}

	cli, err := NewClient(c)
	if err != nil {
		Fatal("error creating client")
	}

	sigHandler(cli)

	cli.Run(config)
	cli.Stop()
}

func execute(c *cli.Context) {
	Info("Starting delayd")

	config, err := loadConfig(c)
	if err != nil {
		Fatal("Unable to read config file: ", err)
	}

	s := Server{}
	sigHandler(&s)
	s.Run(config)
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

	cliFlags := []cli.Flag{
		cli.StringFlag{
			Name: "config, c", Value: "/etc/delayd.toml", Usage: "config file"},
		cli.StringFlag{
			Name: "exchange, e", Value: "delayd-cli", Usage: "response exchange name"},
		cli.StringFlag{
			Name: "key, k", Value: "delayd-key", Usage: "key to store message under"},
		cli.BoolFlag{
			Name: "repl, r", Usage: "launch client in REPL mode"},
		cli.StringFlag{
			Name: "file, f ", Value: "msg.json", Usage: "read message from file"},
		cli.IntFlag{
			Name: "delay, d", Value: 1000, Usage: "expiry time"},
		cli.StringFlag{
			Name: "out, o", Usage: "write delayd response to file"},
		cli.BoolFlag{
			Name: "no-wait, n", Usage: "do not wait for amqp response before exiting"},
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
		{
			Name:        "client",
			ShortName:   "cli",
			Usage:       "Spawn a Delayd Client",
			Description: "CLI for Delayd Server process -- send commands via CLI",
			Action:      executeCli,
			Flags:       cliFlags,
		},
	}

	app.Run(os.Args)
}
