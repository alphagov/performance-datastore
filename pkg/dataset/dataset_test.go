package dataset_test

import (
	"encoding/json"
	"github.com/jabley/performance-datastore/pkg/config_api"
	. "github.com/jabley/performance-datastore/pkg/dataset"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

func Unmarshal(t string) interface{} {
	var r interface{}
	err := json.Unmarshal([]byte(t), &r)
	Expect(err).To(BeNil())
	return r
}

var _ = Describe("Dataset", func() {
	var (
		metaData config_api.DataSetMetaData
		dataSet  DataSet
	)

	BeforeEach(func() {
		metaData = config_api.DataSetMetaData{}
		dataSet = DataSet{nil, metaData}
	})

	Describe("DataSetMetaData", func() {
		It("should default IsPublished to false", func() {
			Expect(dataSet.IsPublished()).Should(BeFalse())
		})

		It("IsPublished should be read from the meta data", func() {
			dataSet.MetaData.Published = true
			Expect(dataSet.IsPublished()).Should(BeTrue())
		})

		It("should default IsQueryable to false", func() {
			Expect(dataSet.IsQueryable()).Should(BeFalse())
		})

		It("IsQueryable should be read from the meta data", func() {
			dataSet.MetaData.Queryable = true
			Expect(dataSet.IsQueryable()).Should(BeTrue())
		})

		It("CappedSize should default to 0", func() {
			Expect(dataSet.CappedSize()).Should(Equal(int64(0)))
		})

		It("CappedSize should be read from the meta data", func() {
			dataSet.MetaData.CappedSize = 12345
			Expect(dataSet.CappedSize()).Should(Equal(int64(12345)))
		})
	})

	Describe("Auto IDs", func() {
		It("Should not alter input when there are no auto IDs defined", func() {
			record := Unmarshal(`{"foo": "foo", "bar": "bar"}`)
			records := []interface{}{record}
			actual := dataSet.ProcessAutoIds(records, nil)
			Expect(records).Should(Equal(actual))
		})

		It("Should add an auto ID based on a single field", func() {
			dataSet.MetaData.AutoIds = []string{"foo"}
			record := Unmarshal(`{"foo": "foo", "bar": "bar"}`)
			records := []interface{}{record}
			actual := dataSet.ProcessAutoIds(records, nil)
			expected := Unmarshal(`{"foo": "foo", "bar": "bar","_id": "Zm9v"}`)
			Expect([]interface{}{expected}).Should(Equal(actual))
		})

		It("Should add an auto ID based on multiple fields", func() {
			dataSet.MetaData.AutoIds = []string{"foo", "bar"}
			record := Unmarshal(`{"foo": "foo", "bar": "bar"}`)
			records := []interface{}{record}
			actual := dataSet.ProcessAutoIds(records, nil)
			expected := Unmarshal(`{"foo": "foo", "bar": "bar","_id": "Zm9vLmJhcg=="}`)
			Expect([]interface{}{expected}).Should(Equal(actual))
		})
	})

	Describe("Timestamps", func() {
		It("Should not modify a record which does not contain a timestamp", func() {
			record := Unmarshal(`{}`)
			records := []interface{}{record}
			errors := []error{}
			dataSet.ParseTimestamps(records, &errors)
			Expect(len(errors)).Should(Equal(0))
			expected := map[string]interface{}{}
			Expect([]interface{}{expected}).Should(Equal(records))
		})

		It("Should convert valid string timestamp to time.Date", func() {
			record := Unmarshal(`{"_timestamp": "2012-12-12T00:00:00"}`)
			records := []interface{}{record}
			errors := []error{}
			dataSet.ParseTimestamps(records, &errors)
			Expect(len(errors)).Should(Equal(0))
			expected := map[string]interface{}{"_timestamp": time.Date(2012, 12, 12, 0, 0, 0, 0, time.UTC)}
			Expect([]interface{}{expected}).Should(Equal(records))
		})

		It("Should return an error with an invalid timestamp", func() {
			record := Unmarshal(`{"_timestamp": "invalid"}`)
			records := []interface{}{record}
			errors := []error{}
			dataSet.ParseTimestamps(records, &errors)
			Expect(len(errors)).Should(Equal(1))
			expected := map[string]interface{}{"_timestamp": "invalid"}
			Expect([]interface{}{expected}).Should(Equal(records))
			Expect(errors[0].Error()).Should(Equal("_timestamp is not a valid timestamp, it must be ISO8601"))
		})
	})

	Describe("Validation", func() {
		It("Should not allow invalid keys", func() {
			record := Unmarshal(`{"1": "foo"}`)
			records := []interface{}{record}
			errors := []error{}
			dataSet.ValidateRecords(records, &errors)
			Expect(len(errors)).Should(Equal(1))
			expected := Unmarshal(`{"1": "foo"}`)
			Expect([]interface{}{expected}).Should(Equal(records))
		})

		It("Should not allow things that look like unreserved keys", func() {
			record := Unmarshal(`{"_foo": "foo"}`)
			records := []interface{}{record}
			errors := []error{}
			dataSet.ValidateRecords(records, &errors)
			Expect(len(errors)).Should(Equal(1))
			expected := Unmarshal(`{"_foo": "foo"}`)
			Expect([]interface{}{expected}).Should(Equal(records))
		})

		It("Should allow things that look like reserved keys", func() {
			record := Unmarshal(`{"_id": "foo"}`)
			records := []interface{}{record}
			errors := []error{}
			dataSet.ValidateRecords(records, &errors)
			Expect(len(errors)).Should(Equal(0))
			expected := Unmarshal(`{"_id": "foo"}`)
			Expect([]interface{}{expected}).Should(Equal(records))
		})

		It("Should not allow values that aren't whitelisted", func() {
			record := Unmarshal(`{"id": ["foo", "bar"]}`)
			records := []interface{}{record}
			errors := []error{}
			dataSet.ValidateRecords(records, &errors)
			Expect(len(errors)).Should(Equal(1))
			expected := Unmarshal(`{"id": ["foo", "bar"]}`)
			Expect([]interface{}{expected}).Should(Equal(records))
		})

		It("Should only allow timestamps that look contain a time.Time instance", func() {
			record := map[string]interface{}{"_timestamp": time.Date(2012, 12, 12, 0, 0, 0, 0, time.UTC)}
			records := []interface{}{record}
			errors := []error{}
			dataSet.ValidateRecords(records, &errors)
			Expect(len(errors)).Should(Equal(0))
			expected := map[string]interface{}{"_timestamp": time.Date(2012, 12, 12, 0, 0, 0, 0, time.UTC)}
			Expect([]interface{}{expected}).Should(Equal(records))
		})

		It("Should not allow an int as an _id", func() {
			record := map[string]interface{}{"_id": 1}
			records := []interface{}{record}
			errors := []error{}
			dataSet.ValidateRecords(records, &errors)
			Expect(len(errors)).Should(Equal(1))
			expected := map[string]interface{}{"_id": 1}
			Expect([]interface{}{expected}).Should(Equal(records))
		})

		It("Should not allow a string with spaces as an _id", func() {
			record := map[string]interface{}{"_id": "this should fail"}
			records := []interface{}{record}
			errors := []error{}
			dataSet.ValidateRecords(records, &errors)
			Expect(len(errors)).Should(Equal(1))
			expected := map[string]interface{}{"_id": "this should fail"}
			Expect([]interface{}{expected}).Should(Equal(records))
		})
	})
})
