DEPS = $(shell go list -f '{{range .Imports}}{{.}} {{end}}' ./...)
TESTDEPS = $(shell go list -f '{{range .TestImports}}{{.}} {{end}}' ./...)
TOOLDEPS = github.com/golang/lint/golint
# TOOLDEPS += code.google.com/p/go.tools/cmd/vet

PRINTFUNCS = Debug:0,Debugf:1,Info:0,Infof:1,Warn:0,Warnf:1,Error:0,Errorf:1,\
	Fatal:0,Fatalf:1,Panic:0,Panicf:1

# GOPATH isn't in bin on travis
LINT=$(shell echo $$GOPATH | cut -d ":" -f1)/bin/golint

VERSION = $(shell git describe --tags | sed 's/^v//')

default:
	go build -ldflags "-X main.version $(VERSION)"

clean:
	rm -f delayd

deps:
	go get -d -v ./...
	echo $(TESTDEPS) | xargs -n1 go get -d -v
	echo $(TOOLDEPS) | xargs -n1 go get -v

update-deps:
	echo $(DEPS) | xargs -n1 go get -u -d -v
	echo $(TESTDEPS) | xargs -n1 go get -u -d -v
	echo $(TOOLDEPS) | xargs -n1 go get -u -v


test:
	go test -v -short -timeout=1s ./...

testint:
	go test -v -timeout=10s ./...

check: lint
	gofmt -l .
	[ -z "$$(gofmt -l .)" ]

# XXX vet fails to build at the moment. revisit later.
vet:
	go tool vet -printfuncs="$(PRINTFUNCS)" .

# golint has no options or ways to ignore values, so if we start getting false
# positives, just take it out of the build flow.
lint:
	$(LINT) .
	[ -z "$$($(LINT) .)" ]

cover:
	go test -cover ./...

# has to use the full package name for me
htmlcov:
	go test -coverprofile /tmp/delayd-coverprof.cov ./...
	go tool cover -html /tmp/delayd-coverprof.cov

# has to use the full package name for me
funccov:
	go test -coverprofile /tmp/delayd-coverprof.cov ./...
	go tool cover -func /tmp/delayd-coverprof.cov

ci: check testint
