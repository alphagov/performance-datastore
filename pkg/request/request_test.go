package request

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewRequest", func() {
	It("sets the bearer token in the header when making requests", func() {
		bearerToken := "FOO"

		ts := testServer(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+bearerToken {
				w.WriteHeader(http.StatusUnauthorized)
				fmt.Fprintln(w, "Not authorized!")
				return
			}

			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "You're authorized!")
		})

		defer ts.Close()

		response, err := NewRequest(ts.URL, bearerToken)
		Expect(err).To(BeNil())
		Expect(response.StatusCode).To(Equal(200))

		body, err := ReadResponseBody(response)
		Expect(err).To(BeNil())
		Expect(strings.TrimSpace(string(body))).To(Equal(
			"You're authorized!"))
	})

	It("handles bad networking from the origin server", func() {
		ts := testServer(func(w http.ResponseWriter, r *http.Request) {
			hj, ok := w.(http.Hijacker)
			if !ok {
				panic("webserver doesn't support hijacking – failing the messy way")
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				panic("webserver doesn't support hijacking – failing the messy way")
				return
			}
			// Fail in a clean way so that we don't clutter the output
			conn.Close()
		})
		defer ts.Close()
		// Ensure this isn't a slow test by restricting how many retries happen
		response, err := NewRequest(ts.URL, "FOO", MaxElapsedTime(5*time.Millisecond))
		Expect(response).To(BeNil())
		Expect(err).ShouldNot(BeNil())
	})

	It("retries server unavailable in a forgiving manner", func() {
		semaphore := make(chan struct{})

		ts := testServer(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-semaphore:
				// Second time through, the channel is closed, so we succeed
				w.WriteHeader(http.StatusOK)
			default:
				// First time through, channel gives nothing so we error
				w.WriteHeader(http.StatusServiceUnavailable)
				close(semaphore)
			}
		})
		defer ts.Close()
		response, err := NewRequest(ts.URL, "FOO")
		Expect(response).ShouldNot(BeNil())
		Expect(err).Should(BeNil())
	})

	It("propagates 404s", func() {
		ts := testServer(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		})
		defer ts.Close()
		response, err := NewRequest(ts.URL, "FOO")
		Expect(response).Should(BeNil())
		Expect(err).ShouldNot(BeNil())
		Expect(err).Should(Equal(ErrNotFound))
	})

	It("propagates 401s", func() {
		ts := testServer(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		})
		defer ts.Close()
		response, err := NewRequest(ts.URL, "FOO")
		// Allow clients to examine the response body
		Expect(response).ShouldNot(BeNil())
		Expect(err).ShouldNot(BeNil())
		Expect(err.Error()).Should(Equal("Unexpected status code 401"))
	})
})

var _ = Describe("ReadResponseBody", func() {
	It("errors when given a nil response", func() {
		body, err := ReadResponseBody(nil)
		Expect(body).Should(BeNil())
		Expect(err).Should(Equal(io.ErrUnexpectedEOF))
	})
})

func testServer(handler interface{}) *httptest.Server {
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
