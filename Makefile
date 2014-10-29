.PHONY: deps test build

BINARY := performance-datastore

all: deps fmt test build

deps:
	go get github.com/mattn/gom
	gom -test install

fmt:
	gofmt -w=1 *.go
	gofmt -w=1 pkg

test:
	gom test \
		. \
		./pkg/config_api/ \
		./pkg/dataset/ \
		./pkg/handlers/ \
		./pkg/request/ \
		./pkg/validation/

build:
	gom build -o $(BINARY)

clean:
	rm -rf $(BINARY)
