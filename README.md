# Delayd

[![Build Status](https://travis-ci.org/goinstant/delayd.svg?branch=master)](https://travis-ci.org/goinstant/delayd)
[![BSD License](http://img.shields.io/badge/license-BSD-blue.svg)](https://github.com/goinstant/delayd/blob/master/LICENSE)

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

## Running

```bash
./delayd -c delayd.toml
```

## License
&copy; 2014 salesforce.com. Licensed under the BSD 3-clause license.

[![GoInstant](http://goinstant.com/static/img/logo.png)](http://goinstant.com)
