default:
	go build

clean:
	rm -f delayd

deps:
	go get -v ./...

test:
	go test -v
