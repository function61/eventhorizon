.PHONY: all fmt writer test vet release

all: fmt writer

fmt:
	go fmt ./...

writer:
	cd cli/writer/ && go build

test:
	go test ./...

vet:
	go vet ./...

release: fmt test vet writer
