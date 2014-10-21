package dataset

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/jabley/performance-datastore/pkg/config_api"
	"github.com/jabley/performance-datastore/pkg/validation"
	"github.com/xeipuuv/gojsonschema"
	"time"
)

type DataSetStorage interface {
	Create(name string, cappedSize int64) error
	Exists(name string) bool
	Alive() bool
	LastUpdated(name string) *time.Time
	SaveRecord(name string, record map[string]interface{}) error
}

type DataSet struct {
	Storage  DataSetStorage
	MetaData config_api.DataSetMetaData
}

type Query struct {
}

type StalenessResult struct {
	MaxExpectedAge   *int64
	LastUpdated      *time.Time
	SecondsOutOfDate int64
}

func (s *StalenessResult) IsStale() bool {
	return s.SecondsOutOfDate > 0
}

func (d DataSet) IsQueryable() bool {
	return d.MetaData.Queryable
}

func (d DataSet) IsPublished() bool {
	return d.MetaData.Published
}

func (d DataSet) IsStale() (r StalenessResult) {
	expectedMaxAge := d.getMaxExpectedAge()
	now := time.Now()
	lastUpdated := d.getLastUpdated()

	r = StalenessResult{expectedMaxAge, lastUpdated, 0}

	if isStalenessAppropriate(expectedMaxAge, lastUpdated) {
		r.SecondsOutOfDate = int64((now.Sub(*lastUpdated) - time.Duration(*expectedMaxAge)).Seconds())
	}

	return
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

	records := unwrap(data)

	d.ValidateAgainstSchema(records, &errors)
	d.ProcessAutoIds(records, &errors)
	d.ParseTimestamps(records, &errors)
	d.ValidateRecords(records, &errors)

	if len(errors) > 0 {
		return errors
	}

	d.AddPeriodData(records)

	for _, record := range records {
		if err := d.saveRecord(record); err != nil {
			panic(err)
		}
	}

	return
}

func unwrap(data []interface{}) []map[string]interface{} {
	records := make([]map[string]interface{}, len(data))

	for i, d := range data {
		records[i] = d.(map[string]interface{})
	}

	return records
}

func (d DataSet) ValidateAgainstSchema(data []map[string]interface{}, errors *[]error) {
	schema := d.MetaData.Schema

	if schema != nil {
		var jsonDoc map[string]interface{}
		err := json.Unmarshal(d.MetaData.Schema, &jsonDoc)

		if err != nil {
			panic(err)
		}

		schemaDocument, err := gojsonschema.NewJsonSchemaDocument(jsonDoc)
		if err != nil {
			panic(err)
		}
		for _, r := range data {
			result := schemaDocument.Validate(r)
			if !result.Valid() {
				for _, err := range result.Errors() {
					*errors = append(*errors, fmt.Errorf(err.Description))
				}
			}
		}
	}
}

func (d DataSet) AddPeriodData(data []map[string]interface{}) {
	for _, r := range data {
		addPeriodData(r)
	}
}

func addPeriodData(record map[string]interface{}) {
	t, ok := record["_timestamp"]
	if ok {
		switch t.(type) {
		case time.Time:
			{
				// add other fields based on t
				v := t.(time.Time)
				for _, p := range Periods {
					record[p.FieldName()] = p.Value(v)
				}
			}
		default:
			panic("_timestamp is not a time.Time")
		}
	}
}

func (d DataSet) ValidateRecords(data []map[string]interface{}, errors *[]error) {
	for _, r := range data {
		validateRecord(r, errors)
	}
}

func (d DataSet) saveRecord(record map[string]interface{}) error {
	record["_updated_at"] = time.Now()
	return d.Storage.SaveRecord(d.Name(), record)
}

func (d DataSet) ParseTimestamps(data []map[string]interface{}, errors *[]error) {
	for _, r := range data {
		parseTimestamp(r, errors)
	}
}

func parseTimestamp(record map[string]interface{}, errors *[]error) {
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

func (d DataSet) ProcessAutoIds(data []map[string]interface{}, errors *[]error) interface{} {
	if len(d.MetaData.AutoIds) > 0 && len(data) != 0 {
		return addAutoIds(data, d.MetaData.AutoIds, errors)
	}
	return data
}

func validateRecord(record map[string]interface{}, errors *[]error) {
	for k, v := range record {
		if !validation.IsValidKey(k) {
			*errors = append(*errors, fmt.Errorf("%v is not a valid key", k))
			return
		}

		if validation.IsInternalKey(k) &&
			!validation.IsReservedKey(k) {
			*errors = append(*errors, fmt.Errorf("%v is not a recognised internal field", k))
			return
		}

		if !validation.IsValidValue(v) {
			*errors = append(*errors, fmt.Errorf("%v has an invalid value", k))
			return
		}

		if k == "_timestamp" {
			switch v.(type) {
			case time.Time:
			default:
				{
					*errors = append(*errors, fmt.Errorf("_timestamp is not a valid datetime object"))
					return
				}
			}
		}

		if k == "_id" && !validation.IsValidID(v) {
			*errors = append(*errors, fmt.Errorf("id is not a valid ID"))
			return
		}
	}
}

func addAutoIds(data []map[string]interface{}, autoIds []string, errors *[]error) interface{} {
	for _, record := range data {
		addAutoId(record, autoIds, errors)
	}

	return data
}

func addAutoId(record map[string]interface{}, autoIds []string, errors *[]error) {
	keys := make([]string, len(record))
	i := 0
	for k, _ := range record {
		keys[i] = k
		i++
	}

	missingIdFields := []string{}
	for _, id := range autoIds {
		_, ok := record[id]
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
