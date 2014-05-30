DEPS = $(shell go list -f '{{range .TestImports}}{{.}} {{end}}' ./...)

default:
	go build

clean:
	rm -f delayd

deps:
	go get -d -v ./...
	echo $(DEPS) | xargs -n1 go get -d -v

test:
	go test ./...

cover:
	go test -cover ./...

htmlcov:
	# has to use the full package name for me
	go test -coverprofile /tmp/delayd-coverprof.cov github.com/goinstant/delayd
	go tool cover -html /tmp/delayd-coverprof.cov
