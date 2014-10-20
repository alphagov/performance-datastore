.PHONY: deps test build

BINARY := performance_datastore

all: deps fmt test build

deps:
	go get github.com/tools/godep
	godep restore

fmt:
	gofmt -w=1 *.go
	gofmt -w=1 pkg

test:
	godep go test \
		. \
		./pkg/config_api/ \
		./pkg/dataset/ \
		./pkg/handlers/ \
		./pkg/request/ \
		./pkg/json_response/ \
		./pkg/validation/

build:
	godep go build -o $(BINARY)

clean:
	rm -rf $(BINARY)
