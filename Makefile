.PHONY: all build

all: build

build:
	go fmt ./...
	go build
