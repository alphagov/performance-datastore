package dataset

import (
	"encoding/base64"
	"fmt"
	"github.com/jabley/performance-datastore/pkg/config_api"
	"github.com/jabley/performance-datastore/pkg/validation"
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

func (d DataSet) Append(data []interface{}) []error {
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

func (d DataSet) store(data []interface{}) (errors []error) {

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

func (d DataSet) ValidateAgainstSchema(data []interface{}, errors *[]error) {
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

func (d DataSet) addPeriodData(data []interface{}) {

}

func (d DataSet) ValidateRecords(data []interface{}, errors *[]error) {

}

func (d DataSet) saveRecord(record interface{}) {

}

func (d DataSet) ParseTimestamps(data []interface{}, errors *[]error) {
	for _, r := range data {
		parseTimestamp(r, errors)
	}
}

func parseTimestamp(r interface{}, errors *[]error) {
	record, ok := r.(map[string]interface{})
	if !ok {
		*errors = append(*errors, fmt.Errorf("Unable to handle record as map"))
		return
	}

	current, hasTimestamp := record["_timestamp"]

	if hasTimestamp {
		if res, err := tryParseTimestamp(current); err != nil {
			*errors = append(*errors, err)
		} else {
			record["_timestamp"] = *res
		}
	}
}

func tryParseTimestamp(t interface{}) (*time.Time, error) {
	res := validation.ParseDateTime(t)
	if res != nil {
		return res, nil
	}

	return nil, fmt.Errorf("_timestamp is not a valid timestamp, it must be ISO8601")
}

func (d DataSet) ProcessAutoIds(data []interface{}, errors *[]error) interface{} {
	if len(d.MetaData.AutoIds) > 0 && len(data) != 0 {
		return addAutoIds(data, d.MetaData.AutoIds, errors)
	}
	return data
}

func validateRecord(record interface{}, schema string) error {
	return nil
}

func addAutoIds(data []interface{}, autoIds []string, errors *[]error) interface{} {
	for _, record := range data {
		addAutoId(record, autoIds, errors)
	}

	return data
}

func addAutoId(r interface{}, autoIds []string, errors *[]error) {
	record, ok := r.(map[string]interface{})
	if !ok {
		*errors = append(*errors, fmt.Errorf("Unable to handle record as map"))
		return
	}

	keys := make([]string, len(record))
	i := 0
	for k, _ := range record {
		keys[i] = k
		i++
	}

	missingIdFields := []string{}
	for _, id := range autoIds {
		_, ok = record[id]
		if !ok {
			missingIdFields = append(missingIdFields, id)
		}
	}

	if len(missingIdFields) > 0 {
		// "The following required id fields are missing: {}".format(
		// ', '.join(missing_keys)))
		panic("The following required id fields are missing: ")
	}

	record["_id"] = generateAutoId(record, autoIds)
}

func generateAutoId(record map[string]interface{}, autoIDs []string) string {
	b := ""
	for _, id := range autoIDs {
		var sep = "."
		if len(b) == 0 {
			sep = ""
		}
		b = b + sep + string(record[id].(string))
	}
	return base64.StdEncoding.EncodeToString([]byte(b))
}

func (d DataSet) collectionExists(name string) bool {
	return d.Storage.Exists(name)
}

func (d DataSet) createCollection() error {
	return d.Storage.Create(d.Name(), d.CappedSize())
}
