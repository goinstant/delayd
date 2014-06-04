package main

import (
	"bufio"
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

	cli := Client{}
	cli.stdin = make(chan []byte)
	go cli.Run(config)

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
			Flags:       flags,
		},
	}

	app.Run(os.Args)
}
