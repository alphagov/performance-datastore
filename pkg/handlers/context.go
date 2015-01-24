package handlers

import (
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/context"
)

const (
	logKey         = 1 << iota // 1 (i.e. 1 << 0)
	datasetNameKey             // 2 (i.e. 1 << 1)
)

// Type-safe application helpers to manage attributes on the request

func setLogger(r *http.Request, logger *logrus.Logger) {
	context.Set(r, logKey, logger)
}

func getLogger(r *http.Request) *logrus.Logger {
	if rv := context.Get(r, logKey); rv != nil {
		return rv.(*logrus.Logger)
	}
	return nil
}

func setDatasetName(r *http.Request, name string) {
	context.Set(r, datasetNameKey, name)
}

func getDatasetName(r *http.Request) string {
	if rv := context.Get(r, datasetNameKey); rv != nil {
		return rv.(string)
	}
	return ""
}
