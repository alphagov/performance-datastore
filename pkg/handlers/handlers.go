package handlers

import (
	"encoding/json"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/alphagov/performance-datastore/pkg/config_api"
	"github.com/alphagov/performance-datastore/pkg/dataset"
	"github.com/alphagov/performance-datastore/pkg/validation"
	"github.com/go-martini/martini"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"time"
)

type WarningResponse struct {
	Data    interface{}
	Warning string `json:"warning"`
}

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
	m.Get("/data/:data_group/:data_type", DataTypeHandler)
	m.Options("/data/:data_group/:data_type", DataTypeHandler)
	m.Post("/data/:data_group/:data_type", CreateHandler)
	m.Put("/data/:data_group/:data_type", UpdateHandler)
	return m
}

// DataTypeHandler is responsible for serving data type meta data
//
// GET|OPTIONS /data/:data_group/data_type
func DataTypeHandler(w http.ResponseWriter, r *http.Request, params martini.Params) {
	metaData, err := fetchDataMetaData(params["data_group"], params["data_type"])
	if err != nil {
		renderError(w, http.StatusInternalServerError, err.Error())
		return
	}

	dataStart := time.Now()
	defer statsDTiming(fmt.Sprintf("data.%s.%s", params["data_group"], params["data_type"]),
		dataStart, time.Now())
	fetch(metaData, w, r)
}

// CreateHandler is responsible for creating data
//
// POST /data/:data_group/:data_type
func CreateHandler(c martini.Context, w http.ResponseWriter, r *http.Request, params martini.Params) {
	handleWriteRequest(c, w, r, params, func(jsonArray []interface{}, dataSet dataset.DataSet) {
		errors := dataSet.Append(jsonArray)

		if len(errors) > 0 {
			renderError(w, http.StatusBadRequest, "All the errors")
		} else {
			renderer.JSON(w, http.StatusOK, map[string]string{"status": "OK"})
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
		renderer.JSON(w, http.StatusOK, map[string]string{
			"status":  "OK",
			"message": dataSet.Name() + " now contains 0 records"})
	})
}

func handleWriteRequest(
	c martini.Context,
	w http.ResponseWriter,
	r *http.Request,
	params martini.Params,
	f func(arr []interface{}, ds dataset.DataSet)) {

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
		renderError(w, http.StatusBadRequest, "Expected JSON request body but received zero bytes")
		return
	}

	var data interface{}
	err = json.Unmarshal(jsonBytes, &data)

	if err != nil {
		renderError(w, http.StatusBadRequest, "Error parsing JSON: "+err.Error())
		return
	}

	jsonArray := ensureIsArray(data)
	f(jsonArray, dataSet)
}

func ensureIsArray(data interface{}) []interface{} {
	switch reflect.ValueOf(data).Kind() {
	case reflect.Array, reflect.Slice:
		return data.([]interface{})
	default:
		return []interface{}{data}
	}
}

func fetch(metaData *config_api.DataSetMetaData, w http.ResponseWriter, r *http.Request) {
	if metaData == nil {
		renderError(w, http.StatusNotFound, "data_set not found")
		return
	}

	dataSet := dataset.DataSet{DataSetStorage, *metaData}

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

func fetchDataMetaData(dataGroup string, dataType string) (*config_api.DataSetMetaData, error) {
	dataTypeStart := time.Now()
	defer statsDTiming(fmt.Sprintf("config.%s.%s", dataGroup, dataType),
		dataTypeStart, time.Now())
	return ConfigAPIClient.DataType(dataGroup, dataType)
}
