package handlers

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/alphagov/performance-datastore/pkg/config"
	"github.com/alphagov/performance-datastore/pkg/dataset"
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
			"status":  "ok",
			"message": "database seems fine",
		})
	}
}

// DataSetStatus is a representation of health for a DataSet.
type DataSetStatus struct {
	Name             string    `json:"name"`
	SecondsOutOfDate int64     `json:"seconds-out-of-date"`
	LastUpdated      time.Time `json:"last-updated"`
	MaxAgeExpected   int64     `json:"max-age-expected"`
}

func (d DataSetStatus) String() string {
	return fmt.Sprintf("name: %v, seconds-out-of-date: %v, last-updated: %v, max-age-expected: %v", d.Name, d.SecondsOutOfDate, d.LastUpdated, d.MaxAgeExpected)
}

// ByName implements sort.Interface for []DataSetStatus based on
// the Name field.
type ByName []DataSetStatus

func (a ByName) Len() int           { return len(a) }
func (a ByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByName) Less(i, j int) bool { return a[i].Name < a[j].Name }

// DataSetStatusHandler is basic healthcheck for all of the datasets
//
// GET /_status/data-sets
func DataSetStatusHandler(w http.ResponseWriter, r *http.Request) {
	datasets, err := ConfigAPIClient.ListDataSets()

	if err != nil {
		renderError(w, http.StatusInternalServerError, err.Error())
		return
	}

	failing := collectStaleness(datasets)
	status := summariseStaleness(failing)

	setStatusHeaders(w)

	renderer.JSON(w, http.StatusOK, status)
}

func checkFreshness(
	dataSet dataset.DataSet,
	failing chan DataSetStatus,
	wg *sync.WaitGroup) {
	defer wg.Done()

	if staleness := dataSet.IsStale(); staleness.IsStale() && dataSet.IsPublished() {
		failing <- DataSetStatus{dataSet.Name(), staleness.SecondsOutOfDate, *staleness.LastUpdated, *staleness.MaxExpectedAge}
	}
}

func collectStaleness(datasets []config.DataSetMetaData) (failing chan DataSetStatus) {
	failing = make(chan DataSetStatus, len(datasets))

	if len(datasets) == 0 {
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(datasets))

	for _, metaData := range datasets {
		go checkFreshness(dataset.DataSet{DataSetStorage, metaData}, failing, wg)
	}

	wg.Wait()

	return
}

func setStatusHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "none")
}

func summariseStaleness(failing chan DataSetStatus) APIResponse {
	// close the channel so that we don't block trying to read when we get to the end
	close(failing)

	message := "All data-sets are in date"

	var failures []DataSetStatus

	for failure := range failing {
		failures = append(failures, failure)
	}

	if len(failures) == 0 {
		return APIResponse{
			Status: "ok"}
	}

	message = fmt.Sprintf("%d %s out of date", len(failures), pluraliseDataSets(failures))

	errorStrings := make([]string, len(failures))

	sort.Sort(ByName(failures))

	for i, f := range failures {
		errorStrings[i] = f.String()
	}

	errors := newErrorInfos(errorStrings...)

	return APIResponse{
		Status:  "not okay",
		Message: message,
		Errors:  errors}

}

func pluraliseDataSets(failures []DataSetStatus) string {
	if len(failures) > 1 {
		return "data-sets are"
	}
	return "data-set is"
}
