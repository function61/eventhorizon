.PHONY: all fmt writer pusher importfromfile test vet release

all: fmt writer

fmt:
	go fmt ./...

writer:
	cd cli/writer/ && go build

pusher:
	cd cli/pusher/ && go build

importfromfile:
	cd cli/importfromfile/ && go build

test:
	go test ./...

vet:
	go vet ./...

release: fmt test vet writer
