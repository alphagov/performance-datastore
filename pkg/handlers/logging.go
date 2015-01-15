package handlers

import (
	"bufio"
	"fmt"
	"github.com/Sirupsen/logrus"
	"net"
	"net/http"
	"time"
)

type statusCapturingResponseWriter struct {
	statusCode int
	delegate   http.ResponseWriter
}

func newStatusCapturingResponseWriter(delegate http.ResponseWriter) *statusCapturingResponseWriter {
	return &statusCapturingResponseWriter{http.StatusOK, delegate}
}

// ResponseWriter implementation
func (rw *statusCapturingResponseWriter) Header() http.Header {
	return rw.delegate.Header()
}

func (rw *statusCapturingResponseWriter) Write(bytes []byte) (int, error) {
	return rw.delegate.Write(bytes)
}

func (rw *statusCapturingResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.delegate.WriteHeader(code)
}

// CloseNotifier
func (rw *statusCapturingResponseWriter) CloseNotify() <-chan bool {
	return rw.delegate.(http.CloseNotifier).CloseNotify()
}

// Flusher implementation
func (rw *statusCapturingResponseWriter) Flush() {
	flusher, ok := rw.delegate.(http.Flusher)
	if ok {
		flusher.Flush()
	}
}

// Hijacker interface
func (rw *statusCapturingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := rw.delegate.(http.Hijacker)
	if ok {
		return hj.Hijack()
	}
	return nil, nil, fmt.Errorf("Delegate does not support hijacking")
}

// NewLoggingHandler returns a http.Handler middleware that logs details about a response
func NewLoggingHandler(h http.Handler, log *logrus.Logger) http.Handler {
	return http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		setLogger(req, log)
		start := time.Now()
		statusRes := newStatusCapturingResponseWriter(res)
		h.ServeHTTP(statusRes, req)
		latency := time.Since(start)

		log.WithFields(logrus.Fields{
			"method":       req.Method,
			"path":         req.URL.Path,
			"request_uri":  req.RequestURI,
			"status":       statusRes.statusCode,
			"time":         latency,
			"request_time": latency.Seconds(),
		}).Info("Handled a request")
	})
}
