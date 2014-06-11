DEPS = $(shell go list -f '{{range .Imports}}{{.}} {{end}}' ./...)
TESTDEPS = $(shell go list -f '{{range .TestImports}}{{.}} {{end}}' ./...)
TOOLDEPS = code.google.com/p/go.tools/cmd/vet
TOOLDEPS += github.com/golang/lint/golint

PRINTFUNCS = Debug:0,Debugf:1,Info:0,Infof:1,Warn:0,Warnf:1,Error:0,Errorf:1,\
	Fatal:0,Fatalf:1,Panic:0,Panicf:1

# GOPATH isn't in bin on travis
LINT=$(shell echo $$GOPATH | cut -d ":" -f1)/bin/golint

default:
	go build

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
	go test -short ./...

testint:
	go test ./...

check: lint vet
	gofmt -l .
	[ -z "$$(gofmt -l .)" ]

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
	go test -coverprofile /tmp/delayd-coverprof.cov github.com/goinstant/delayd
	go tool cover -html /tmp/delayd-coverprof.cov

# has to use the full package name for me
funccov:
	go test -coverprofile /tmp/delayd-coverprof.cov github.com/goinstant/delayd
	go tool cover -func /tmp/delayd-coverprof.cov

ci: check testint
