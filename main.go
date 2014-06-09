package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/codegangsta/cli"
)

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
	log.Println("Starting delayd.Client")

	config, err := loadConfig(c)
	if err != nil {
		log.Fatal("Unable to read config file: ", err)
	}

	cli, err := NewClient(c)
	if err != nil {
		log.Fatal("error creating client")
	}

	sigHandler(cli)

	cli.Run(config)
	cli.Stop()
}

func execute(c *cli.Context) {
	log.Println("Starting delayd")

	config, err := loadConfig(c)
	if err != nil {
		log.Fatal("Unable to read config file: ", err)
	}

	s := Server{}
	sigHandler(&s)
	s.Run(config)
}

func main() {
	app := cli.NewApp()
	app.Name = "delayd"
	app.Usage = "available setTimeout()"

	flags := []cli.Flag{
		cli.StringFlag{"config, c", "/etc/delayd.toml", "config file"},
	}

	cliFlags := []cli.Flag{
		cli.StringFlag{"config, c", "/etc/delayd.toml", "config file"},
		cli.StringFlag{"exchange, e", "delayd-cli", "response exchange name"},
		cli.StringFlag{"key, k", "delayd-key", "key to store message under"},
		cli.BoolFlag{"repl, r", "launch client in REPL mode"},
		cli.StringFlag{"file, f ", "msg.json", "read message from file"},
		cli.IntFlag{"delay, d", 1000, "expiry time"},
		cli.StringFlag{"out, o", "", "write delayd response to file"},
		cli.BoolFlag{"no-wait, n", "do not wait for amqp response before exiting"},
	}

	app.Commands = []cli.Command{
		{
			Name:        "server",
			ShortName:   "serv",
			Usage:       "Spawn a Delayd Server",
			Description: "Delay Daemon for distributed `setTimeout()`",
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
