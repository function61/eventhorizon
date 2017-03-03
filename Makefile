.PHONY: all build

all: build

build:
	go fmt ./...
	cd cli/writer/ && go build
