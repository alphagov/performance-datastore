.PHONY: deps test build

BINARY := performance-datastore
IMPORT_BASE := github.com/alphagov
IMPORT_PATH := $(IMPORT_BASE)/performance-datastore

all: deps _vendor fmt test build

deps:
	go get github.com/mattn/gom
	go get github.com/onsi/ginkgo/ginkgo
	go get code.google.com/p/go.tools/cmd/cover

fmt:
	gofmt -w=1 *.go
	gofmt -w=1 pkg

test:
	gom exec ginkgo -cover \
		. \
		./pkg/config/ \
		./pkg/dataset/ \
		./pkg/handlers/ \
		./pkg/request/ \
		./pkg/validation/
	# rewrite the generated .coverprofile files so that you can run the command
	# gom tool cover -html=./pkg/handlers/handlers.coverprofile and other lovely stuff
	find . -name '*.coverprofile' -type f -exec sed -i '' 's|_'$(CURDIR)'|\.|' {} \;

build:
	gom build -o $(BINARY)

clean:
	rm -rf $(BINARY)

_vendor: Gomfile _vendor/src/$(IMPORT_PATH)
	gom -test install
	touch _vendor

_vendor/src/$(IMPORT_PATH):
	rm -f _vendor/src/$(IMPORT_PATH)
	mkdir -p _vendor/src/$(IMPORT_BASE)
	ln -s $(CURDIR) _vendor/src/$(IMPORT_PATH)
