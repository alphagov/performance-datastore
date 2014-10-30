package handlers

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestHandlers(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Handlers Suite")
}

func testHandlerServer(handler interface{}) *httptest.Server {
	var h http.Handler
	switch handler := handler.(type) {
	case http.Handler:
		h = handler
	case func(http.ResponseWriter, *http.Request):
		h = http.HandlerFunc(handler)
	default:
		// error
		panic("handler cannot be used in an HTTP Server")
	}
	return httptest.NewServer(h)
}

func readResponseBody(response *http.Response) (string, error) {
	body, err := ioutil.ReadAll(response.Body)
	defer response.Body.Close()

	return strings.TrimSpace(string(body)), err
}
