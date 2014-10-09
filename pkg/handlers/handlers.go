package handlers

import (
	"fmt"
	"github.com/go-martini/martini"
	"github.com/jabley/performance-datastore/pkg/config_api"
	"github.com/jabley/performance-datastore/pkg/dataset"
	"github.com/jabley/performance-datastore/pkg/json_response"
	"github.com/jabley/performance-datastore/pkg/validation"
	"github.com/quipo/statsd"
	"gopkg.in/unrolled/render.v1"
	"net/http"
	"strings"
	"time"
)

type ErrorInfo struct {
	Id     *string `json:"id"`
	Status *string `json:"status"`
	Code   *string `json:"code"`
	Title  *string `json:"title"`
	Detail *string `json:"detail"`
}

type errorResponse struct {
	Errors []*ErrorInfo `json:"errors"`
}

type WarningResponse struct {
	Data    interface{}
	Warning string `json:"warning"`
}

var (
	// DataSetStorage is the application global for talking to persistent storage
	// It is like this to allow test implementations to be injected.
	DataSetStorage dataset.DataSetStorage
	renderer       = render.New(render.Options{})
	statsdClient   = newStatsDClient("localhost:8125", "datastore.")
)

// DataTypeHandler is responsible for serving data type meta data
//
// GET|OPTIONS /data/:data_group/data_type
func DataTypeHandler(w http.ResponseWriter, r *http.Request, params martini.Params) {
	metaData, err := fetchDataMetaData(params["data_group"], params["data_type"])
	if err != nil {
		panic(err)
	}

	dataStart := time.Now()
	defer statsDTiming(fmt.Sprintf("data.%s.%s", params["data_group"], params["data_type"]),
		dataStart, time.Now())
	fetch(metaData, w, r)
}

// CreateHandler is responsible for creating data
//
// POST /data/:data_group/:data_type
func CreateHandler(w http.ResponseWriter, r *http.Request, params martini.Params) {
	metaData, err := fetchDataMetaData(params["data_group"], params["data_type"])
	if err != nil {
		panic(err)
	}

	dataSet := dataset.DataSet{nil, metaData}

	err = validateAuthorization(r, dataSet)
	if err != nil {
		renderError(w, http.StatusUnauthorized, err.Error())
		return
	}

	data, err := json_response.ParseArray(r.Body)

	if err != nil {
		renderError(w, http.StatusBadRequest, err.Error())
		return
	}

	errors := dataSet.Append(data)

	if len(errors) > 0 {
		renderError(w, http.StatusBadRequest, "All the errors")
	} else {
		renderer.JSON(w, http.StatusOK, map[string]string{"status": "OK"})
	}
}

// UpdateHandler is responsible for updating data
//
// PUT /data/:data_group/:data_type
func UpdateHandler(w http.ResponseWriter, r *http.Request, params martini.Params) {

}

func fetch(metaData dataset.DataSetMetaData, w http.ResponseWriter, r *http.Request) {
	if metaData == nil {
		renderError(w, http.StatusNotFound, "data_set not found")
		return
	}

	dataSet := dataset.DataSet{DataSetStorage, metaData}

	// Is the data set queryable?
	if !dataSet.IsQueryable() {
		renderError(w, http.StatusNotFound, fmt.Sprintf("data_set %s not found", dataSet.Name()))
		return
	}

	// OPTIONS?
	if r.Method == "OPTIONS" {
		// TODO Set allowed methods
		w.Header().Set("Access-Control-Max-Age", "86400")
		w.Header().Set("Access-Control-Allow-Headers",
			"Cache-Control, GOVUK-Request-Id, Request-Id")
		return
	}

	if err := validateRequest(r, dataSet); err != nil {
		renderError(w, http.StatusNotFound, fmt.Sprintf(err.Error(), dataSet.Name()))
		return
	}

	query := parseQuery(r)
	data, err := dataSet.Execute(query)

	if err != nil {
		renderError(w, http.StatusBadRequest, fmt.Sprintf("Invalid collect function", dataSet.Name()))
		return
	}

	var body interface{}

	if !dataSet.IsPublished() {
		warning := "Warning: This data-set is unpublished. \n" +
			"Data may be subject to change or be inaccurate."
		w.Header().Set("Cache-Control", "no-cache")
		body = WarningResponse{data, warning}
	} else {
		maxAge := dataSet.CacheDuration()
		w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%d, must-revalidate", maxAge))
		body = data
	}

	renderer.JSON(w, http.StatusOK, &body)
}

func parseQuery(r *http.Request) dataset.Query {
	return dataset.Query{}
}

func validateRequest(r *http.Request, dataSet dataset.DataSet) error {
	return validation.ValidateRequestArgs(r.URL.Query(), dataSet.AllowRawQueries())
}

func validateAuthorization(r *http.Request, dataSet dataset.DataSet) (err error) {
	authorization := r.Header.Get("Authorization")

	if len(authorization) == 0 {
		return fmt.Errorf("Expected header of form: Authorization: Bearer <token>")
	}

	token, valid := extractBearerToken(dataSet, authorization)

	if !valid {
		return fmt.Errorf("Unauthorized: Invalid bearer token '%s' for '%s'", token, dataSet.Name())
	}
	return
}

func extractBearerToken(dataSet dataset.DataSet, authorization string) (token string, ok bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(authorization, prefix) {
		return "", false
	}

	token = authorization[len(prefix):]
	return token, token == dataSet.BearerToken()
}

func renderError(w http.ResponseWriter, status int, errorString string) {
	renderer.JSON(w, status, &errorResponse{Errors: []*ErrorInfo{&ErrorInfo{Detail: &errorString}}})
}

func newStatsDClient(host, prefix string) *statsd.StatsdClient {
	statsdClient := statsd.NewStatsdClient(host, prefix)
	statsdClient.CreateSocket()

	return statsdClient
}

func statsDTiming(label string, start, end time.Time) {
	statsdClient.Timing("time."+label,
		int64(end.Sub(start)/time.Millisecond))
}

func fetchDataMetaData(dataGroup string, dataType string) (map[string]interface{}, error) {
	dataTypeStart := time.Now()
	defer statsDTiming(fmt.Sprintf("config.%s.%s", dataGroup, dataType),
		dataTypeStart, time.Now())
	return config_api.DataType(dataGroup, dataType)
}
