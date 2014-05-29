default:
	go build

clean:
	rm -f delayd

deps:
	go get -d -v ./...
