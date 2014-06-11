# Delayd

[![Build Status](https://magnum.travis-ci.com/goinstant/delayd.svg?token=pPAtatqxvKxCP6YPwTxz&branch=master)](https://magnum.travis-ci.com/goinstant/delayd)
[![BSD License](http://img.shields.io/badge/license-BSD-blue.svg)](https://github.com/goinstant/delayd/blob/master/LICENSE)

Delayd is an available `setTimeout()` service for scheduling message sends.

Delayd can send and receive messages over [AMQP](https://www.rabbitmq.com),
with more transports planned.

## Running

```bash
./delayd server -c delayd.toml
```

To ensure availability, Delayd is clustered via
[raft](http://raftconsensus.github.io/). You should run at least 3 Delayd
servers. This permits the cluster to keep running if one server fails.

## Guarantees

Delayd ensures that after a message has been received, it will be delivered at
least once. A message that has reached its delay time may be delivered more
than once if the cluster leader fails after emitting the message, but before
this state has replicated to the other cluster members.

## AMQP Message Format

Message bodies are forwarded unchanged from received messages after their delay
lapses. All Delayd directives are taken from AMQP headers.

### Required Headers
- delayd-delay (number) Delay time in ms before emitting this message.
- delayd-target (string) Target exchange for this message.

### Optional Headers
- delayd-key (string) If a message with the same key exists in Delayd,
  this message will replace it.

## Developing

`delayd` is built/developed with `go1.2`. I recommend using the excellent
[gvm](https://github.com/moovweb/gvm) to install it.

To get started:
```bash
make deps  # install required modules
make check # run fmt, vet, lint
make test  # run unit tests
make       # build the executable
```

## License
&copy; 2014 GoInstant Inc., a salesforce.com company. Licensed under the BSD
3-clause license.

[![GoInstant](http://goinstant.com/static/img/logo.png)](http://goinstant.com)
