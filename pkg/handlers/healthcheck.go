package handlers

import (
	"fmt"
	"github.com/jabley/performance-datastore/pkg/config_api"
	"github.com/jabley/performance-datastore/pkg/dataset"
	"net/http"
	"sync"
	"time"
)

// StatusHandler is the basic healthcheck for the application
//
// GET /_status
func StatusHandler(w http.ResponseWriter, r *http.Request) {
	setStatusHeaders(w)

	if !DataSetStorage.Alive() {
		renderError(w, http.StatusInternalServerError, "cannot connect to database")
	} else {
		renderer.JSON(w, http.StatusOK, map[string]string{
			"status":  "OK",
			"message": "database seems fine",
		})
	}
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
	datasets, err := ConfigAPIClient.ListDataSets()

	if err != nil {
		panic(err)
	}

	failing := collectStaleness(datasets)
	status := summariseStaleness(failing)

	setStatusHeaders(w)

	if status != nil {
		renderer.JSON(w, http.StatusOK, status)
	} else {
		renderer.JSON(w, http.StatusOK, map[string]string{"status": "OK"})
	}
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

func collectStaleness(datasets []config_api.DataSetMetaData) (failing chan DataSetStatus) {
	if len(datasets) == 0 {
		failing = make(chan DataSetStatus)
		close(failing)
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(datasets))
	failing = make(chan DataSetStatus, len(datasets))

	for _, metaData := range datasets {
		dataSet := dataset.DataSet{DataSetStorage, metaData}
		go checkFreshness(dataSet, failing, wg)
	}

	wg.Wait()

	return
}

func setStatusHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "none")
}

func summariseStaleness(failing chan DataSetStatus) *ErrorInfo {
	allGood := true

	message := "All data-sets are in date"

	var failures []DataSetStatus

	for failure := range failing {
		allGood = false
		failures = append(failures, failure)
	}

	if allGood {
		return nil
	} else {
		message = fmt.Sprintf("%d %s out of date", len(failures), pluraliseDataSets(failures))

		return &ErrorInfo{
			Status: "not okay",
			Detail: message,
			// Other: failures,
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
