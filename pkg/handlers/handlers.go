package handlers

import (
	"encoding/json"
	"fmt"
	"github.com/go-martini/martini"
	"github.com/jabley/performance-datastore/pkg/config_api"
	"github.com/jabley/performance-datastore/pkg/dataset"
	"github.com/jabley/performance-datastore/pkg/json_response"
	"github.com/jabley/performance-datastore/pkg/validation"
	"net/http"
	"strings"
	"sync"
	"time"
)

type statusResponse struct {
	// Field names should be public, so that encoding/json can see them
	Status  string `json:"status"`
	Message string `json:"message"`
	Code    int    `json:"code"`
}

type WarningResponse struct {
	Data    interface{}
	Warning string `json:"warning"`
}

var DataSetStorage dataset.DataSetStorage

// StatusHandler is the basic healthcheck for the application
//
// GET /_status
func StatusHandler(w http.ResponseWriter, r *http.Request) {
	status := statusResponse{
		Status:  "ok",
		Message: "database seems fine",
	}

	if !DataSetStorage.Alive() {
		status.Status = "error"
		status.Message = "cannot connect to database"
		w.WriteHeader(http.StatusInternalServerError)
	}

	setStatusHeaders(w)
	serialiseJSON(w, status)
}

type dataSetStatusResponse struct {
	// Field names should be public, so that encoding/json can see them
	Status   string `json:"status"`
	Message  string `json:"message"`
	Code     int    `json:"code"`
	DataSets []DataSetStatus
}

type DataSetStatus struct {
	Name             string    `json:"name"`
	SecondsOutOfDate int       `json:"seconds-out-of-date"`
	LastUpdated      time.Time `json:"last-updated"`
	MaxAgeExpected   int       `json:"max-age-expected"`
}

// DataSetStatusHandler is basic healthcheck for all of the datasets
//
// GET /_status/data-sets
func DataSetStatusHandler(w http.ResponseWriter, r *http.Request) {
	datasets, err := config_api.ListDataSets()

	if err != nil {
		panic(err)
	}

	failing := collectStaleness(datasets)
	status := summariseStaleness(failing)

	setStatusHeaders(w)
	serialiseJSON(w, status)
}

// DataTypeHandler is responsible for serving data type meta data
//
// GET|OPTIONS /data/:data_group/data_type
func DataTypeHandler(w http.ResponseWriter, r *http.Request, params martini.Params) {
	metaData, err := config_api.DataType(params["data_group"], params["data_type"])
	if err != nil {
		panic(err)
	}
	fetch(metaData, w, r)
}

// CreateHandler is responsible for creating data
//
// POST /data/:data_group/:data_type
func CreateHandler(w http.ResponseWriter, r *http.Request, params martini.Params) {
	// with statsd.timer('write.route.data.{data_group}.{data_type}'.format(
	//         data_group=data_group,
	//         data_type=data_type)):

	metaData, err := config_api.DataType(params["data_group"], params["data_type"])
	if err != nil {
		panic(err)
	}

	dataSet := dataset.DataSet{nil, metaData}

	err = validateAuthorization(r, dataSet)
	if err != nil {
		logAndReturn(w, err.Error())
		return
	}

	data, err := json_response.ParseArray(r.Body)

	if err != nil {
		logAndReturn(w, "400")
		return
	}

	errors := dataSet.Append(data)

	if len(errors) > 0 {
		serialiseJSON(w, statusResponse{"error", "All the errors", 400})
	} else {
		serialiseJSON(w, statusResponse{"ok", "", 0})
	}
}
func logAndReturn(w http.ResponseWriter, message string) {
	w.WriteHeader(http.StatusNotFound)
	setStatusHeaders(w)
	serialiseJSON(w, statusResponse{"error", message, 0})
}

func fetch(metaData dataset.DataSetMetaData, w http.ResponseWriter, r *http.Request) {
	if metaData == nil {
		logAndReturn(w, "data_set not found")
		return
	}

	dataSet := dataset.DataSet{DataSetStorage, metaData}

	// Is the data set queryable?
	if !dataSet.IsQueryable() {
		logAndReturn(w, fmt.Sprintf("data_set %s not found", dataSet.Name()))
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
		logAndReturn(w, fmt.Sprintf(err.Error(), dataSet.Name()))
		return
	}

	query := parseQuery(r)
	data, err := dataSet.Execute(query)

	if err != nil {
		logAndReturn(w, fmt.Sprintf("Invalid collect function", dataSet.Name()))
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

	serialiseJSON(w, body)
}

func parseQuery(r *http.Request) dataset.Query {
	return dataset.Query{}
}

func validateRequest(r *http.Request, dataSet dataset.DataSet) error {
	return validation.ValidateRequestArgs(r.URL.Query(), dataSet.AllowRawQueries())
}

func serialiseJSON(w http.ResponseWriter, status interface{}) {
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(status); err != nil {
		panic(err)
	}
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

func checkFreshness(
	dataSet dataset.DataSet,
	failing chan DataSetStatus,
	wg *sync.WaitGroup) {
	defer wg.Done()

	if dataSet.IsStale() && dataSet.IsPublished() {
		failing <- DataSetStatus{dataSet.Name(), 0, time.Now(), 0}
	}
}

func setStatusHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "none")
}

func collectStaleness(datasets []interface{}) (failing chan DataSetStatus) {
	wg := &sync.WaitGroup{}
	wg.Add(len(datasets))
	failing = make(chan DataSetStatus, len(datasets))

	for _, m := range datasets {
		metaData := m.(dataset.DataSetMetaData)
		dataSet := dataset.DataSet{DataSetStorage, metaData}
		go checkFreshness(dataSet, failing, wg)
	}

	wg.Wait()

	return
}

func summariseStaleness(failing chan DataSetStatus) dataSetStatusResponse {
	allGood := true

	message := "All data-sets are in date"

	var failures []DataSetStatus

	for failure := range failing {
		allGood = false
		failures = append(failures, failure)
	}

	if allGood {
		return dataSetStatusResponse{
			Status:  "ok",
			Message: message,
		}
	} else {
		message = fmt.Sprintf("%d %s out of date", len(failures), pluraliseDataSets(failures))
		return dataSetStatusResponse{
			Status:   "not okay",
			Message:  message,
			DataSets: failures,
		}
	}
}

func pluraliseDataSets(failures []DataSetStatus) string {
	if len(failures) > 1 {
		return "data-sets are"
	} else {
		return "data-set is"
	}
}
