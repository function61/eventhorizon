.PHONY: all fmt pyramid test vet release release-pkg

all: fmt pyramid

fmt:
	go fmt ./...

pyramid:
	# compile statically so this works on Alpine that doesn't have glibc
	cd cli/pyramid/ && CGO_ENABLED=0 go build --ldflags '-extldflags "-static"'

test:
	go test ./...

vet:
	go vet ./...

release: fmt test vet pyramid release-pkg

release-pkg:
	./make-release-pkg.sh
