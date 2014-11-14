# Delayd

[![Build Status](https://travis-ci.org/nabeken/delayd.svg?branch=sqs)](https://travis-ci.org/nabeken/delayd)
[![BSD License](http://img.shields.io/badge/license-BSD-blue.svg)](https://github.com/goinstant/delayd/blob/master/LICENSE)

This repository is a fork of [goinstant/delayd](https://github.com/goinstant/delayd)
aiming to support SQS and docker.

We are working on SQS and docker support.

Delayd is an available `setTimeout()` service for scheduling message sends.

Delayd can send and receive messages over [AMQP](https://www.rabbitmq.com),
with more transports planned.

To ensure availability, Delayd is clustered via
[raft](http://raftconsensus.github.io/). You should run at least 3 Delayd
servers. This permits the cluster to keep running if one server fails.

## Usage

```bash
$ docker run -it --rm nabeken/delayd delayd -h
NAME:
   delayd - available setTimeout()

USAGE:
   delayd [global options] command [command options] [arguments...]

VERSION:
   

COMMANDS:
   server, serv	Spawn a Delayd Server
   help, h	Shows a list of commands or help for one command
   
GLOBAL OPTIONS:
   --help, -h		show help
   --version, -v	print the version
```

## Guarantees

Delayd ensures that after a message has been received, it will be delivered at
least once. A message that has reached its delay time may be delivered more
than once if the cluster leader fails after emitting the message, but before
this state has replicated to the other cluster members.

## AMQP

### Message Format

Message bodies are forwarded unchanged from received messages after their delay
lapses. All Delayd directives are taken from AMQP headers.

### Required Headers

- delayd-delay (number) Delay time in ms before emitting this message.
- delayd-target (string) Target exchange for this message.

### Optional Headers

- delayd-key (string) If a message with the same key exists in Delayd,
  this message will replace it.

## Development

`delayd` is built/developed with `go1.3`.

To get started:

```bash
make deps  # install required modules
make check # run fmt, vet, lint
make test  # run unit tests
make       # build the executable
```

## License (Docker and SQS support)

&copy; 2014 Ken-ichi TANABE. Licensed under the same terms as the original work.

## License (original work)
&copy; 2014 salesforce.com. Licensed under the BSD 3-clause license.

[![GoInstant](http://goinstant.com/static/img/logo.png)](http://goinstant.com)
