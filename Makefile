.PHONY: deps test build

BINARY := performance-datastore
IMPORT_BASE := github.com/alphagov
IMPORT_PATH := $(IMPORT_BASE)/performance-datastore

GOOS := $(shell uname | tr '[:upper:]' '[:lower:]')

FMT := $(shell { type goimports || type gofmt; } 2>/dev/null | cut -f 1 -d ' ')

all: deps _vendor fmt test build

deps:
	-go get github.com/mattn/gom
	-go get code.google.com/p/go.tools/cmd/cover
	-go get golang.org/x/tools/cmd/cover

fmt:
	$(FMT) -w=1 *.go
	$(FMT) -w=1 pkg

test:
	gom test -cover \
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
	GO_ENABLED=0 GOOS=$(GOOS) gom build -race -a -tags netgo -ldflags '-w' -o $(BINARY) .

clean:
	rm -rf $(BINARY)

_vendor: Gomfile _vendor/src/$(IMPORT_PATH)
	gom -test install
	touch _vendor

_vendor/src/$(IMPORT_PATH):
	rm -f _vendor/src/$(IMPORT_PATH)
	mkdir -p _vendor/src/$(IMPORT_BASE)
	ln -s $(CURDIR) _vendor/src/$(IMPORT_PATH)
