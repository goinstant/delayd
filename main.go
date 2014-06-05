package main

import (
	"bufio"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/codegangsta/cli"
)

func executeCli(c *cli.Context) {
	log.Println("Starting delayd.Client")

	config, err := loadConfig(c)
	if err != nil {
		log.Fatal("Unable to read config file: ", err)
	}

	cli := Client{
		exchange: c.String("exchange"),
		key:      c.String("key"),
	}
	cli.stdin = make(chan []byte)
	go cli.Run(config)

	// we will only stick around reading stdin forever if the user specifies
	// --repl

	if c.Bool("repl") == false {
		f := c.String("file")
		dat, err := ioutil.ReadFile(f)
		if err != nil {
			log.Fatalf("Error reading from file: %s, got error: %s", f, err)
		}

		cli.stdin <- dat

		return
	}

	log.Println("Waiting for STDIN")
	bio := bufio.NewReader(os.Stdin)

	for {
		line, err := bio.ReadBytes('\n')
		if err != nil {
			log.Fatal("Error reading from STDIN")
		}

		cli.stdin <- line
	}
}

func execute(c *cli.Context) {
	log.Println("Starting delayd")

	config, err := loadConfig(c)
	if err != nil {
		log.Fatal("Unable to read config file: ", err)
	}

	s := Server{}

	// graceful shutdown for ^C and kill
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		for _ = range ch {
			s.Stop()
			os.Exit(0)
		}
	}()

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
