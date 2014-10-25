package handlers

import (
	"github.com/alphagov/performance-datastore/pkg/config_api"
	"github.com/alphagov/performance-datastore/pkg/dataset"
	"github.com/quipo/statsd"
	"gopkg.in/unrolled/render.v1"
	"net/http"
	"time"
)

type ErrorInfo struct {
	Id     string   `json:"id,omitempty"`
	HREF   string   `json:"href,omitempty"`
	Status string   `json:"status,omitempty"`
	Code   string   `json:"code,omitempty"`
	Title  string   `json:"title,omitempty"`
	Detail string   `json:"detail,omitempty"`
	Links  []string `json:"links,omitempty"`
	Path   string   `json:"path,omitempty"`
}

type errorResponse struct {
	Errors []*ErrorInfo `json:"errors"`
}

var (
	// DataSetStorage is the application global for talking to persistent storage
	// It is like this to allow test implementations to be injected.
	DataSetStorage dataset.DataSetStorage

	// ConfigAPIClient allows the client to be injected for testing purposes
	ConfigAPIClient config_api.Client

	// StatsdClient allows the statsd implementation to be injected for testing purposes
	StatsdClient statsd.Statsd

	renderer = render.New(render.Options{})
)

func MethodNotAllowedHandler(w http.ResponseWriter, r *http.Request) {
	renderError(w, http.StatusMethodNotAllowed,
		"Method "+r.Method+" not allowed for <"+r.URL.RequestURI()+">")
}

func renderError(w http.ResponseWriter, status int, errorString string) {
	renderer.JSON(w, status, &errorResponse{Errors: []*ErrorInfo{&ErrorInfo{Detail: errorString}}})
}

func NewStatsDClient(host, prefix string) *statsd.StatsdClient {
	statsdClient := statsd.NewStatsdClient(host, prefix)
	statsdClient.CreateSocket()

	return statsdClient
}

func statsDTiming(label string, start, end time.Time) {
	StatsdClient.Timing("time."+label,
		int64(end.Sub(start)/time.Millisecond))
}
