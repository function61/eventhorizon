.PHONY: all fmt pyramid test vet release

all: fmt pyramid

fmt:
	go fmt ./...

pyramid:
	cd cli/pyramid/ && go build

test:
	go test ./...

vet:
	go vet ./...

release: fmt test vet pyramid
