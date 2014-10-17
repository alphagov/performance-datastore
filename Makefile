.PHONY: deps test build rm_compiled_self

BINARY := performance_datastore
ORG_PATH := github.com/jabley
REPO_PATH := $(ORG_PATH)/performance-datastore

all: deps fmt test build

release: reproducible fmt test build

deps: third_party/src/$(REPO_PATH) rm_compiled_self
	go run third_party.go get -t -v .

reproducible: third_party/src/$(REPO_PATH) rm_compiled_self
	go run third_party.go bump github.com/alext/tablecloth b373a9a6ff0ebb8953da0681db7a72202c73e2ef
	go run third_party.go bump github.com/cenkalti/backoff c201004c081d767e9749c64d1039749c1353da7b
	go run third_party.go bump github.com/codegangsta/inject 4b8172520a03fa190f427bbd284db01b459bfce7
	go run third_party.go bump github.com/go-martini/martini 7d32ea3fa6590565c928b90a48178b60b96df98f
	go run third_party.go bump github.com/onsi/ginkgo 9064f1684d498eba1ec104747a766ee0f9222f44
	go run third_party.go bump github.com/onsi/gomega 0251f51e656d4db8980bdd6b15596abb27fdfe87
	go run third_party.go bump github.com/quipo/statsd e260042c957aa6ffa369c4f38c21d66cfad15ffc
	go run third_party.go bump github.com/xeipuuv/gojsonpointer 57ab5e9c764219a3e0c4d7759797fefdcab22e9c
	go run third_party.go bump github.com/xeipuuv/gojsonreference 47e61ed14ee33a23dadf5baffffc7d1b35d0b0ec
	go run third_party.go bump github.com/xeipuuv/gojsonschema b77385941381681f1bba177899ed1d1bedd32ea2
	go run third_party.go bump gopkg.in/mgo.v2 c9fd3712fbf3e92924c974dce16da2d322508fe2
	go run third_party.go bump gopkg.in/unrolled/render.v1 cf57b1afa5d93abdbd356aad95d7c2720dad6d01

rm_compiled_self:
	rm -rf third_party/pkg/*/$(REPO_PATH)

third_party/src/$(REPO_PATH):
	mkdir -p third_party/src/$(ORG_PATH)
	ln -s ../../../.. third_party/src/$(REPO_PATH)

fmt:
	gofmt -w=1 *.go
	gofmt -w=1 pkg

test:
	go run third_party.go test \
		$(REPO_PATH) $(REPO_PATH)/pkg/config_api/ \
		$(REPO_PATH) $(REPO_PATH)/pkg/dataset/ \
		$(REPO_PATH) $(REPO_PATH)/pkg/handlers/ \
		$(REPO_PATH) $(REPO_PATH)/pkg/request/ \
		$(REPO_PATH)/pkg/json_response/ \
		$(REPO_PATH)/pkg/validation/

build:
	go run third_party.go build -o $(BINARY)
