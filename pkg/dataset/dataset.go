package dataset

import (
	"github.com/jabley/performance-datastore/pkg/config_api"
	"time"
)

type DataSetStorage interface {
	Create(name string, cappedSize int64) error
	Exists(name string) bool
	Alive() bool
	LastUpdated(name string) *time.Time
}

type DataSet struct {
	Storage  DataSetStorage
	MetaData config_api.DataSetMetaData
}

type Query struct {
}

func (d DataSet) IsQueryable() bool {
	return d.MetaData.Queryable
}

func (d DataSet) IsPublished() bool {
	return d.MetaData.Published
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

func (d DataSet) Append(data []map[string]interface{}) []error {
	d.createIfNecessary()
	return d.store(data)
}

func (d DataSet) Execute(query Query) (interface{}, error) {
	return nil, nil
}

func (d DataSet) isRealtime() bool {
	return d.MetaData.Realtime
}

func (d DataSet) CacheDuration() int {
	if d.isRealtime() {
		return 120
	}
	return 1800
}

func (d DataSet) getMaxExpectedAge() *int64 {
	return d.MetaData.MaxExpectedAge
}

func (d DataSet) AllowRawQueries() bool {
	return d.MetaData.AllowRawQueries
}

func (d DataSet) BearerToken() string {
	return d.MetaData.BearerToken
}

func (d DataSet) CappedSize() int64 {
	return d.MetaData.CappedSize
}

func (d DataSet) Name() string {
	return d.MetaData.Name
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

func (d DataSet) store(data []map[string]interface{}) (errors []error) {

	d.ValidateAgainstSchema(data, &errors)
	d.ProcessAutoIds(data, &errors)
	d.ParseTimestamps(data, &errors)
	d.ValidateRecords(data, &errors)

	if len(errors) > 0 {
		return errors
	}

	d.addPeriodData(data)

	for _, record := range data {
		d.saveRecord(record)
	}

	return
}

func (d DataSet) ValidateAgainstSchema(data []map[string]interface{}, errors *[]error) {
	// schema, ok := d.MetaData.Schema
	ok := false

	if ok {
		// for _, record := range *data {
		// e := validateRecord(record, schema)
		// if e != nil {
		// 	*errors = append(*errors, e)
		// }
		// }
	}
}

func (d DataSet) addPeriodData(data []map[string]interface{}) {

}

func (d DataSet) ValidateRecords(data []map[string]interface{}, errors *[]error) {

}

func (d DataSet) saveRecord(record map[string]interface{}) {

}

func (d DataSet) ParseTimestamps(data []map[string]interface{}, errors *[]error) {

}

func (d DataSet) ProcessAutoIds(data []map[string]interface{}, errors *[]error) interface{} {
	if len(d.MetaData.AutoIds) > 0 {
		return addAutoIds(data, d.MetaData.AutoIds, errors)
	}
	return data
}

func validateRecord(record map[string]interface{}, schema string) error {
	return nil
}

func addAutoIds(data []map[string]interface{}, autoIds []string, errors *[]error) interface{} {
	if len(data) == 0 {
		return data
	}

	for _, record := range data {
		generateAutoID(&record, autoIds, errors)
	}

	return data
}

func generateAutoID(record *map[string]interface{}, autoIds []string, errors *[]error) {

}

func (d DataSet) collectionExists(name string) bool {
	return d.Storage.Exists(name)
}

func (d DataSet) createCollection() error {
	return d.Storage.Create(d.Name(), d.CappedSize())
}
