package main

import (
	"encoding/json"
	"github.com/jabley/performance-datastore/config_api"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"net/http"
	"sync"
	"time"
)

type statusResponse struct {
	// Field names should be public, so that encoding/json can see them
	Status  string `json:"status"`
	Message string `json:"message"`
	Code    int    `json:"code"`
}

var (
	mgoSession      *mgo.Session
	mgoDatabaseName = "backdrop"
	mgoURL          = "localhost"
)

func getMgoSession() *mgo.Session {
	if mgoSession == nil {
		var err error
		mgoSession, err = mgo.DialWithTimeout(mgoURL, 5*time.Second)
		if err != nil {
			panic(err)
		}
		// Set timeout to suitably small value by default.
		mgoSession.SetSyncTimeout(5 * time.Second)
	}
	return mgoSession.Copy()
}

// statusHandler is the basic healthcheck for the application
func statusHandler(w http.ResponseWriter, r *http.Request) {
	session := getMgoSession()
	defer session.Close()

	session.SetMode(mgo.Eventual, true)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "none")

	status := statusResponse{
		Status:  "ok",
		Message: "database seems fine",
	}

	if err := session.Ping(); err != nil {
		status.Status = "error"
		status.Message = err.Error()
		w.WriteHeader(http.StatusInternalServerError)
	}

	encoder := json.NewEncoder(w)
	if err := encoder.Encode(status); err != nil {
		panic(err)
	}
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

// dataSetStatusHandler is basic healthcheck for all of the datasets
func dataSetStatusHandler(w http.ResponseWriter, r *http.Request) {
	session := getMgoSession()
	defer session.Close()

	session.SetMode(mgo.Eventual, true)

	datasets, err := config_api.ListDataSets()

	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(len(datasets))
	failing := make(chan DataSetStatus, len(datasets))

	for _, dataset := range datasets {
		go checkFreshness(dataset.(map[string]interface{}), failing, wg)
	}

	wg.Wait()

	status := summariseStaleness(failing)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "none")

	encoder := json.NewEncoder(w)
	if err := encoder.Encode(status); err != nil {
		panic(err)
	}
}

func checkFreshness(
	dataset map[string]interface{},
	failing chan DataSetStatus,
	wg sync.WaitGroup) {
	defer wg.Done()
	session := getMgoSession()
	defer session.Close()

	if isStale(dataset, session) {
		failing <- DataSetStatus{dataset["name"].(string), 0, time.Now(), 0}
	}
}

func isStale(dataset map[string]interface{}, session *mgo.Session) bool {
	expectedMaxAge := getMaxExpectedAge(dataset)
	now := time.Now()
	lastUpdated := getLastUpdated(dataset, session)

	if isStalenessAppropriate(expectedMaxAge, lastUpdated) {
		return now.Sub(*lastUpdated) > time.Duration(*expectedMaxAge)
	}

	return false
}

func getMaxExpectedAge(dataset map[string]interface{}) (maxExpectedAge *int64) {
	value, ok := dataset["max_age_expected"].(int64)

	// where does the responsibility for setting a default lie? I suggest
	// within the Configuration API
	if ok {
		maxExpectedAge = &value
	}
	return maxExpectedAge
}

func getLastUpdated(dataset map[string]interface{}, session *mgo.Session) (t *time.Time) {
	var lastUpdated bson.M
	session.SetMode(mgo.Monotonic, true)

	coll := session.DB("backdrop").C(dataset["name"].(string))
	err := coll.Find(nil).Sort("-_updated_at").One(&lastUpdated)

	if err != nil {
		panic(err)
	}

	t = nil

	value, isTime := lastUpdated["_updated_at"].(time.Time)

	if isTime {
		t = &value
	}
	return
}

// isStalenessAppropriate returns false if there is no limit on
// expected max age or the data set has never been updated, otherwise
// returns true
func isStalenessAppropriate(maxAge *int64, lastUpdated *time.Time) bool {
	return maxAge != nil && lastUpdated != nil
}

func summariseStaleness(failing chan DataSetStatus) dataSetStatusResponse {
	allGood := true

	message := "All data_sets are in date"

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
		return dataSetStatusResponse{
			Status:   "not okay",
			Message:  "Whoops",
			DataSets: failures,
		}
	}
}
