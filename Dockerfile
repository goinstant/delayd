FROM golang:1.3
MAINTAINER TANABE Ken-ichi <nabeken@tknetworks.org>

RUN mkdir -p /go/src/app
WORKDIR /go/src/app

COPY . /go/src/app

RUN cp -f delayd.toml.docker /etc/delayd.toml && \
  useradd -m delayd && \
  install -o delayd -g delayd -m 0700 -d /var/lib/delayd

RUN go-wrapper download ./...
RUN go install ./...

VOLUME ["/var/lib/delayd"]

USER delayd
