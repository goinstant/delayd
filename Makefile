DEPS = $(shell go list -f '{{range .TestImports}}{{.}} {{end}}' ./...)
TOOLDEPS = code.google.com/p/go.tools/cmd/vet


default:
	go build

clean:
	rm -f delayd

deps:
	go get -d -v ./...
	echo $(DEPS) | xargs -n1 go get -d -v
	echo $(TOOLDEPS) | xargs -n1 go get -v

test:
	go test ./...

check:
	gofmt -l .
	[ -z "$$(gofmt -l .)" ]
	go vet

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
