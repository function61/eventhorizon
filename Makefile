.PHONY: all fmt writer pusher importfromfile

all: fmt writer

fmt:
	go fmt ./...

writer:
	cd cli/writer/ && go build

pusher:
	cd cli/pusher/ && go build

importfromfile:
	cd cli/importfromfile/ && go build
