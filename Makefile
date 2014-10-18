.PHONY: deps test build

BINARY := performance_datastore
ORG_PATH := github.com/jabley
REPO_PATH := $(ORG_PATH)/performance-datastore

all: deps fmt test build

deps:
	go get github.com/tools/godep
	godep restore

fmt:
	gofmt -w=1 *.go
	gofmt -w=1 pkg

test:
	godep go test \
		$(REPO_PATH) $(REPO_PATH)/pkg/config_api/ \
		$(REPO_PATH) $(REPO_PATH)/pkg/dataset/ \
		$(REPO_PATH) $(REPO_PATH)/pkg/handlers/ \
		$(REPO_PATH) $(REPO_PATH)/pkg/request/ \
		$(REPO_PATH)/pkg/json_response/ \
		$(REPO_PATH)/pkg/validation/

build:
	godep go build -o $(BINARY)

clean:
	rm -rf $(BINARY)
