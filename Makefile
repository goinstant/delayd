DEPS = $(shell go list -f '{{range .Imports}}{{.}} {{end}}' ./...)
TESTDEPS = $(shell go list -f '{{range .TestImports}}{{.}} {{end}}' ./...)
TOOLDEPS = code.google.com/p/go.tools/cmd/vet
TOOLDEPS += github.com/golang/lint/golint

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
	go test ./...

check: lint
	gofmt -l .
	[ -z "$$(gofmt -l .)" ]
	go vet

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

ci: check test
