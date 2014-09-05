.PHONY: deps test build rm_compiled_self

BINARY := performance_datastore
ORG_PATH := github.com/jabley
REPO_PATH := $(ORG_PATH)/performance-datastore

all: deps fmt test build

deps: third_party/src/$(REPO_PATH) rm_compiled_self
	go run third_party.go get -t -v .

rm_compiled_self:
	rm -rf third_party/pkg/*/$(REPO_PATH)

third_party/src/$(REPO_PATH):
	mkdir -p third_party/src/$(ORG_PATH)
	ln -s ../../../.. third_party/src/$(REPO_PATH)

fmt:
	gofmt -w=1 *.go
	gofmt -w=1 pkg

test:
	go run third_party.go test -v \
		$(REPO_PATH) $(REPO_PATH)/pkg/dataset/ \
		$(REPO_PATH)/pkg/json_response/ \
		$(REPO_PATH)/pkg/validation/

build:
	go run third_party.go build -o $(BINARY)
