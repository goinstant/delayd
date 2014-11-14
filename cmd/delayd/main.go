package main

import (
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/codegangsta/cli"

	"github.com/nabeken/delayd"
)

// this is filled in at build time
var version string

type Stopper interface {
	Stop()
}

// installSigHandler installs a signal handler to shutdown gracefully for ^C and kill
func installSigHandler(s Stopper) {
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
	delayd.Info("cli: starting delayd")

	config, err := loadConfig(c.String("config"))
	if err != nil {
		delayd.Fatal("cli: unable to read config file:", err)
	}

	// override configuration by envvars
	if url := os.Getenv("AMQP_URL"); url != "" {
		config.AMQP.URL = url
	}
	if raftHost := os.Getenv("RAFT_HOST"); raftHost != "" {
		config.Raft.Listen = raftHost
	}
	if peers := os.Getenv("RAFT_PEERS"); peers != "" {
		config.Raft.Peers = strings.Split(peers, ",")
	}

	// override raft single mode settings by flag
	config.Raft.Single = c.Bool("single")

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
		cli.StringFlag{Name: "config, c", Value: "/etc/delayd.toml", Usage: "config file"},
		cli.BoolFlag{Name: "single", Usage: "run raft single mode"},
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
