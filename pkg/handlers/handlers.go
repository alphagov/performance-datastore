package handlers

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/alphagov/performance-datastore/pkg/config"
	"github.com/alphagov/performance-datastore/pkg/dataset"
	"github.com/alphagov/performance-datastore/pkg/utils"
	"github.com/go-martini/martini"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"time"
)

// goodJSONContinuation is the signature of the function called by handleWriteRequest if:
//
// - the context request is JSON
// - the request is correctly authorised
// - the DataSet seems good
type goodJSONContinuation func(jsonArray []interface{}, dataSet dataset.DataSet)

// NewHandler returns an http.Handler implementation for the server.
func NewHandler(maxGzipBody int, logger *logrus.Logger) http.Handler {
	m := martini.Classic()
	m.Map(logger)
	m.Handlers(
		NewLoggingMiddleware(),
		NewRecoveryHandler(),
		martini.Static("public"),
		NewDecompressingMiddleware(maxGzipBody))
	m.Get("/_status", StatusHandler)
	m.Post("/_status", MethodNotAllowedHandler)
	m.Get("/_status/data-sets", DataSetStatusHandler)
	m.Post("/data/:data_group/:data_type", CreateHandler)
	m.Put("/data/:data_group/:data_type", UpdateHandler)
	return m
}

// CreateHandler is responsible for creating data
//
// POST /data/:data_group/:data_type
func CreateHandler(c martini.Context, w http.ResponseWriter, r *http.Request, params martini.Params) {
	handleWriteRequest(c, w, r, params, func(jsonArray []interface{}, dataSet dataset.DataSet) {
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
func UpdateHandler(c martini.Context, w http.ResponseWriter, r *http.Request, params martini.Params) {
	handleWriteRequest(c, w, r, params, func(jsonArray []interface{}, dataSet dataset.DataSet) {
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
	c martini.Context,
	w http.ResponseWriter,
	r *http.Request,
	params martini.Params,
	continuation goodJSONContinuation) {

	metaData, err := fetchDataMetaData(params["data_group"], params["data_type"])
	if err != nil {
		renderError(w, http.StatusInternalServerError, err.Error())
		return
	}

	dataSet := dataset.DataSet{DataSetStorage, *metaData}

	// Make the dataSet available to the request context
	c.Map(dataSet)

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
