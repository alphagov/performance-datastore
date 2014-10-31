package handlers

import (
	"github.com/alphagov/performance-datastore/pkg/config"
	"github.com/alphagov/performance-datastore/pkg/dataset"
	"github.com/quipo/statsd"
	"gopkg.in/unrolled/render.v1"
	"net/http"
	"time"
)

// ErrorInfo is as described at jsonapi.org
type ErrorInfo struct {
	ID     string   `json:"id,omitempty"`
	HREF   string   `json:"href,omitempty"`
	Status string   `json:"status,omitempty"`
	Code   string   `json:"code,omitempty"`
	Title  string   `json:"title,omitempty"`
	Detail string   `json:"detail,omitempty"`
	Links  []string `json:"links,omitempty"`
	Path   string   `json:"path,omitempty"`
}

type APIResponse struct {
	Status  string      `json:"status"`
	Message string      `json:"message,omitempty"`
	Errors  []ErrorInfo `json:"errors"`
}

var (
	// DataSetStorage is the application global for talking to persistent storage
	// It is like this to allow test implementations to be injected.
	DataSetStorage dataset.DataSetStorage

	// ConfigAPIClient allows the client to be injected for testing purposes
	ConfigAPIClient config.Client

	// StatsdClient allows the statsd implementation to be injected for testing purposes
	StatsdClient statsd.Statsd

	renderer = render.New(render.Options{})
)

// MethodNotAllowedHandler is an http.Handler implementation for when an HTTP method is used which isn't supported by the resource.
func MethodNotAllowedHandler(w http.ResponseWriter, r *http.Request) {
	renderError(w, http.StatusMethodNotAllowed,
		"Method "+r.Method+" not allowed for <"+r.URL.RequestURI()+">")
}

func renderError(w http.ResponseWriter, status int, errorString ...string) {
	errors := newErrorInfos(errorString...)

	var message string
	if len(errors) == 1 {
		message = errorString[0]
	}
	renderer.JSON(w, status, APIResponse{Status: "error", Message: message, Errors: errors})
}

// NewStatsDClient returns a statsd.Statsd implementation
func NewStatsDClient(host, prefix string) *statsd.StatsdClient {
	statsdClient := statsd.NewStatsdClient(host, prefix)
	statsdClient.CreateSocket()

	return statsdClient
}

func statsDTiming(label string, start, end time.Time) {
	StatsdClient.Timing("time."+label,
		int64(end.Sub(start)/time.Millisecond))
}

func newErrorInfos(errorString ...string) []ErrorInfo {
	errors := make([]ErrorInfo, len(errorString))
	for i, detail := range errorString {
		errors[i] = ErrorInfo{Detail: detail}
	}
	return errors
}
