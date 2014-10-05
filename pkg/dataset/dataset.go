package dataset

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

type DataSetMetaData map[string]interface{}

type DataSet struct {
	Storage  *mgo.Session
	MetaData DataSetMetaData
}

type Query struct {
}

func (d DataSet) IsQueryable() bool {
	return d.booleanValue("queryable")
}

func (d DataSet) IsPublished() bool {
	return d.booleanValue("published")
}

func (d DataSet) IsStale() bool {
	expectedMaxAge := d.getMaxExpectedAge()
	now := time.Now()
	lastUpdated := d.getLastUpdated()

	if isStalenessAppropriate(expectedMaxAge, lastUpdated) {
		return now.Sub(*lastUpdated) > time.Duration(*expectedMaxAge)
	}

	return false
}

func (d DataSet) Execute(query Query) (interface{}, error) {
	return nil, nil
}

func (d DataSet) isRealtime() bool {
	return d.booleanValue("realtime")
}

func (d DataSet) CacheDuration() int {
	if d.isRealtime() {
		return 120
	}
	return 1800
}

func (d DataSet) getMaxExpectedAge() (maxExpectedAge *int64) {
	value, ok := d.MetaData["max_age_expected"].(int64)

	// where does the responsibility for setting a default lie? I suggest
	// within the Configuration API
	if ok {
		maxExpectedAge = &value
	}

	return
}

func (d DataSet) AllowRawQueries() bool {
	return d.booleanValue("raw_queries_allowed")
}

func (d DataSet) Name() string {
	return d.MetaData["name"].(string)
}

func (d DataSet) getLastUpdated() (t *time.Time) {
	var lastUpdated bson.M
	d.Storage.SetMode(mgo.Monotonic, true)

	coll := d.Storage.DB("backdrop").C(d.Name())
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

func (d DataSet) booleanValue(field string) (result bool) {
	value, ok := d.MetaData[field].(bool)

	if ok {
		result = value
	}

	return
}
