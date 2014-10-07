package dataset

import (
	"fmt"
	"time"
)

type DataSetMetaData map[string]interface{}

type DataSetStorage interface {
	Create(name string, cappedSize *int) error
	Exists(name string) bool
	Alive() bool
	LastUpdated(name string) *time.Time
}

type DataSet struct {
	Storage  DataSetStorage
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

func (d DataSet) Append(data []interface{}) []error {
	d.createIfNecessary()
	return d.store(data)
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

func (d DataSet) BearerToken() string {
	return d.stringValue("bearer_token")
}

func (d DataSet) CappedSize() *int {
	return d.intValue("capped_size")
}

func (d DataSet) Name() string {
	return d.MetaData["name"].(string)
}

func (d DataSet) getLastUpdated() (t *time.Time) {
	return d.Storage.LastUpdated(d.Name())
}

// isStalenessAppropriate returns false if there is no limit on
// expected max age or the data set has never been updated, otherwise
// returns true
func isStalenessAppropriate(maxAge *int64, lastUpdated *time.Time) bool {
	return maxAge != nil && lastUpdated != nil
}

func (d DataSet) createIfNecessary() {
	if !d.collectionExists(d.Name()) {
		err := d.createCollection()
		if err != nil {
			panic(err)
		}
	}
}

func (d DataSet) booleanValue(field string) (result bool) {
	value, ok := d.MetaData[field].(bool)

	if ok {
		result = value
	}

	return
}

func (d DataSet) intValue(field string) (result *int) {
	value, ok := d.MetaData[field]

	if ok {
		cast, ok := value.(int)
		if ok {
			result = &cast
		}
	}

	return
}

func (d DataSet) stringValue(field string) (result string) {
	value, ok := d.MetaData[field].(string)

	if ok {
		result = value
	}

	return
}

func (d DataSet) store(data []interface{}) (errors []error) {

	d.validateAgainstSchema(&data, &errors)
	d.processAutoIds(&data, &errors)
	d.parseTimestamps(&data, &errors)
	d.validateRecords(&data, &errors)

	if len(errors) > 0 {
		return errors
	}

	d.addPeriodData(&data)

	for _, record := range data {
		d.saveRecord(record)
	}

	return
}

func (d DataSet) validateAgainstSchema(data *[]interface{}, errors *[]error) {
	schema, ok := d.MetaData["schema"].(string)

	if ok {
		for _, record := range *data {
			e := validateRecord(record, schema)
			if e != nil {
				*errors = append(*errors, e)
			}
		}
	}
}

func (d DataSet) addPeriodData(data *[]interface{}) {

}

func (d DataSet) validateRecords(data *[]interface{}, errors *[]error) {

}

func (d DataSet) saveRecord(record interface{}) {

}

func (d DataSet) parseTimestamps(data *[]interface{}, errors *[]error) {

}

func (d DataSet) processAutoIds(data *[]interface{}, errors *[]error) {
	values, ok := d.MetaData["auto_ids"]

	if ok {
		autoIds, ok := values.([]string)
		if !ok {
			*errors = append(*errors, fmt.Errorf("Unable to read auto_ids from %s", d.Name()))
		} else {
			addAutoIds(data, autoIds, errors)
		}
	}
}

func validateRecord(record interface{}, schema string) error {
	return nil
}

func addAutoIds(data *[]interface{}, autoIds []string, errors *[]error) {

}

func (d DataSet) collectionExists(name string) bool {
	return d.Storage.Exists(name)
}

func (d DataSet) createCollection() error {
	return d.Storage.Create(d.Name(), d.CappedSize())
}
