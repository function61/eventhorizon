.PHONY: all fmt horizon test vet release release-pkg

all: fmt horizon

fmt:
	go fmt ./...

horizon:
	# compile statically so this works on Alpine that doesn't have glibc
	cd cli/horizon/ && CGO_ENABLED=0 go build --ldflags '-extldflags "-static"'

test:
	go test ./...

vet:
	go vet ./...

release: fmt test vet horizon release-pkg

release-pkg:
	./make-release-pkg.sh
