package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/codegangsta/cli"
)

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
	app.Action = execute
	app.Flags = []cli.Flag{
		cli.StringFlag{"config, c", "/etc/delayd.toml", "config file"},
	}
	app.Run(os.Args)
}
