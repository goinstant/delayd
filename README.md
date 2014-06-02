# Delayd

[![Build Status](https://magnum.travis-ci.com/goinstant/delayd.svg?token=pPAtatqxvKxCP6YPwTxz&branch=master)](https://magnum.travis-ci.com/goinstant/delayd)

## Developing

`delayd` is built/developed with `go1.2`. I recommend using the excellent
[gvm](https://github.com/moovweb/gvm) to install it.

To get started:
```bash
make deps # install required modules
make test # run unit tests
make      # build the executable
```

## Running

```bash
./delayd -c delayd.toml
```

## License
&copy; 2014 GoInstant Inc., a salesforce.com company. Licensed under the BSD
3-clause license.

[![GoInstant](http://goinstant.com/static/img/logo.png)](http://goinstant.com)
