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
