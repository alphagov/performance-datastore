package handlers

import (
	"expvar"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/alphagov/performance-datastore/pkg/config"
	"github.com/alphagov/performance-datastore/pkg/dataset"
	"github.com/alphagov/performance-datastore/pkg/utils"
	"github.com/gorilla/context"
	"github.com/gorilla/mux"
)

// goodJSONContinuation is the signature of the function called by handleWriteRequest if:
//
// - the context request is JSON
// - the request is correctly authorised
// - the DataSet seems good
type goodJSONContinuation func(jsonArray []interface{}, dataSet dataset.DataSet)

// NewHandler returns an http.Handler implementation for the server.
func NewHandler(maxGzipBody int, logger *logrus.Logger) http.Handler {
	router := mux.NewRouter()

	// We wrap the http.Handler chain in a ClearHandler. We want the logger and
	// things available for use in our other middleware
	router.KeepContext = true

	router.HandleFunc("/_status", StatusHandler).Methods("GET", "HEAD")
	router.HandleFunc("/_status", MethodNotAllowedHandler)
	router.HandleFunc("/_status/vars", varsHandler)
	router.HandleFunc("/_status/data-sets", DataSetStatusHandler).Methods("GET", "HEAD")
	router.HandleFunc("/data/{data_group}/{data_type}", CreateHandler).Methods("POST")
	router.HandleFunc("/data/{data_group}/{data_type}", UpdateHandler).Methods("PUT")

	// Wrap up all our middleware
	return context.ClearHandler(
		NewLoggingHandler(
			NewRecoveryHandler(
				NewDecompressingHandler(router, maxGzipBody)),
			logger))
}

func varsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

// CreateHandler is responsible for creating data
//
// POST /data/:data_group/:data_type
func CreateHandler(w http.ResponseWriter, r *http.Request) {
	handleWriteRequest(w, r, func(jsonArray []interface{}, dataSet dataset.DataSet) {
		errors := dataSet.Append(jsonArray)

		if len(errors) > 0 {
			errorMessages := make([]string, len(errors))
			for i, e := range errors {
				errorMessages[i] = e.Error()
			}
			renderError(w, http.StatusBadRequest, errorMessages...)
		} else {
			renderer.JSON(w, http.StatusOK, APIResponse{
				Status: "ok"})
		}
	})
}

// UpdateHandler is responsible for updating data
//
// PUT /data/:data_group/:data_type
func UpdateHandler(w http.ResponseWriter, r *http.Request) {
	handleWriteRequest(w, r, func(jsonArray []interface{}, dataSet dataset.DataSet) {
		if len(jsonArray) > 0 {
			renderError(w, http.StatusBadRequest, "Not implemented: you can only pass an empty JSON list")
			return
		}
		if err := dataSet.Empty(); err != nil {
			renderError(w, http.StatusInternalServerError, err.Error())
			return
		}
		renderer.JSON(w, http.StatusOK, APIResponse{
			Status:  "ok",
			Message: dataSet.Name() + " now contains 0 records"})
	})
}

func handleWriteRequest(
	w http.ResponseWriter,
	r *http.Request,
	continuation goodJSONContinuation) {

	params := mux.Vars(r)

	metaData, err := fetchDataMetaData(params["data_group"], params["data_type"])
	if err != nil {
		renderError(w, http.StatusInternalServerError, err.Error())
		return
	}

	dataSet := dataset.DataSet{DataSetStorage, *metaData}

	// Make the dataSet available to the request context
	setDatasetName(r, dataSet.Name())

	err = validateAuthorization(r, dataSet)
	if err != nil {
		w.Header().Add("WWW-Authenticate", "bearer")
		renderError(w, http.StatusUnauthorized, err.Error())
		return
	}

	jsonBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		if gzerr, ok := err.(*gzipBombError); ok {
			renderError(w, http.StatusRequestEntityTooLarge, gzerr.Error())
		} else {
			renderError(w, http.StatusBadRequest, err.Error())
		}
		return
	}

	if len(jsonBytes) == 0 {
		renderError(w, http.StatusBadRequest, "Expected JSON request body but received 0 bytes")
		return
	}

	var data interface{}
	err = utils.Unmarshal(jsonBytes, &data)

	if err != nil {
		renderError(w, http.StatusBadRequest, "Error parsing JSON: "+err.Error())
		return
	}

	jsonArray := ensureIsArray(data)
	continuation(jsonArray, dataSet)
}

func ensureIsArray(data interface{}) []interface{} {
	switch reflect.ValueOf(data).Kind() {
	case reflect.Array, reflect.Slice:
		return data.([]interface{})
	default:
		return []interface{}{data}
	}
}

func validateAuthorization(r *http.Request, dataSet dataset.DataSet) (err error) {
	authorization := r.Header.Get("Authorization")

	if len(authorization) == 0 {
		return fmt.Errorf("Expected header of form: Authorization: Bearer token")
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

func fetchDataMetaData(dataGroup string, dataType string) (*config.DataSetMetaData, error) {
	dataTypeStart := time.Now()
	defer statsDTiming(fmt.Sprintf("config.%s.%s", dataGroup, dataType),
		dataTypeStart, time.Now())
	return ConfigAPIClient.DataType(dataGroup, dataType)
}
