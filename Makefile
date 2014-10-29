.PHONY: deps test build

BINARY := performance-datastore
current_dir := $(shell pwd)

all: deps fmt test build

deps:
	go get github.com/mattn/gom
	go get github.com/onsi/ginkgo/ginkgo
	go get code.google.com/p/go.tools/cmd/cover
	gom -test install

fmt:
	gofmt -w=1 *.go
	gofmt -w=1 pkg

test:
	gom exec ginkgo -cover \
		. \
		./pkg/config_api/ \
		./pkg/dataset/ \
		./pkg/handlers/ \
		./pkg/request/ \
		./pkg/validation/
	# rewrite the generated .coverprofile files so that you can run the command
	# gom tool cover -html=./pkg/handlers/handlers.coverprofile and other lovely stuff
	find . -name '*.coverprofile' -type f -exec sed -i '' 's|_'$(current_dir)'|\.|' {} \;

build:
	gom build -o $(BINARY)

clean:
	rm -rf $(BINARY)
