package handlers

import (
	"github.com/Sirupsen/logrus"
	"github.com/go-martini/martini"
	"net/http"
	"time"
)

func NewLoggingMiddleware() martini.Handler {
	return func(res http.ResponseWriter, req *http.Request, c martini.Context, log *logrus.Logger) {
		start := time.Now()
		c.Next()
		latency := time.Since(start)

		rw := res.(martini.ResponseWriter)

		log.WithFields(logrus.Fields{
			"method":      req.Method,
			"path":        req.URL.Path,
			"request_uri": req.RequestURI,
			"status":      rw.Status(),
			"time":        latency,
		}).Info("Handled a request")
	}
}
