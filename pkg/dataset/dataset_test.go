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
	Describe("DataSetMetaData", func() {
		It("should default IsPublished to false", func() {
			metaData := config_api.DataSetMetaData{}
			dataSet := DataSet{nil, metaData}
			Expect(dataSet.IsPublished()).Should(BeFalse())
		})

		It("IsPublished should be read from the meta data", func() {
			metaData := config_api.DataSetMetaData{}
			metaData.Published = true
			dataSet := DataSet{nil, metaData}
			Expect(dataSet.IsPublished()).Should(BeTrue())
		})

		It("should default IsQueryable to false", func() {
			metaData := config_api.DataSetMetaData{}
			dataSet := DataSet{nil, metaData}
			Expect(dataSet.IsQueryable()).Should(BeFalse())
		})

		It("IsQueryable should be read from the meta data", func() {
			metaData := config_api.DataSetMetaData{}
			metaData.Queryable = true
			dataSet := DataSet{nil, metaData}
			Expect(dataSet.IsQueryable()).Should(BeTrue())
		})

		It("CappedSize should default to 0", func() {
			metaData := config_api.DataSetMetaData{}
			dataSet := DataSet{nil, metaData}
			Expect(dataSet.CappedSize()).Should(Equal(int64(0)))
		})

		It("CappedSize should be read from the meta data", func() {
			metaData := config_api.DataSetMetaData{}
			metaData.CappedSize = 12345
			dataSet := DataSet{nil, metaData}
			Expect(dataSet.CappedSize()).Should(Equal(int64(12345)))
		})
	})

	Describe("Auto IDs", func() {
		It("Should not alter input when there are no auto IDs defined", func() {
			metaData := config_api.DataSetMetaData{}
			dataSet := DataSet{nil, metaData}
			record := Unmarshal(`{"foo": "foo", "bar": "bar"}`)
			records := []interface{}{record}
			actual := dataSet.ProcessAutoIds(records, nil)
			Expect(records).Should(Equal(actual))
		})

		It("Should add an auto ID based on a single field", func() {
			metaData := config_api.DataSetMetaData{}
			metaData.AutoIds = []string{"foo"}
			dataSet := DataSet{nil, metaData}
			record := Unmarshal(`{"foo": "foo", "bar": "bar"}`)
			records := []interface{}{record}
			actual := dataSet.ProcessAutoIds(records, nil)
			expected := Unmarshal(`{"foo": "foo", "bar": "bar","_id": "Zm9v"}`)
			Expect([]interface{}{expected}).Should(Equal(actual))
		})

		It("Should add an auto ID based on multiple fields", func() {
			metaData := config_api.DataSetMetaData{}
			metaData.AutoIds = []string{"foo", "bar"}
			dataSet := DataSet{nil, metaData}
			record := Unmarshal(`{"foo": "foo", "bar": "bar"}`)
			records := []interface{}{record}
			actual := dataSet.ProcessAutoIds(records, nil)
			expected := Unmarshal(`{"foo": "foo", "bar": "bar","_id": "Zm9vLmJhcg=="}`)
			Expect([]interface{}{expected}).Should(Equal(actual))
		})

	})

	Describe("Timestamps", func() {
		It("Should not modify a record which does not contain a timestamp", func() {
			metaData := config_api.DataSetMetaData{}
			dataSet := DataSet{nil, metaData}
			record := Unmarshal(`{}`)
			records := []interface{}{record}
			errors := []error{}
			dataSet.ParseTimestamps(records, &errors)
			Expect(len(errors)).Should(Equal(0))
			expected := map[string]interface{}{}
			Expect([]interface{}{expected}).Should(Equal(records))
		})

		It("Should convert valid string timestamp to time.Date", func() {
			metaData := config_api.DataSetMetaData{}
			dataSet := DataSet{nil, metaData}
			record := Unmarshal(`{"_timestamp": "2012-12-12T00:00:00"}`)
			records := []interface{}{record}
			errors := []error{}
			dataSet.ParseTimestamps(records, &errors)
			Expect(len(errors)).Should(Equal(0))
			expected := map[string]interface{}{"_timestamp": time.Date(2012, 12, 12, 0, 0, 0, 0, time.UTC)}
			Expect([]interface{}{expected}).Should(Equal(records))
		})

		It("Should return an error with an invalid timestamp", func() {
			metaData := config_api.DataSetMetaData{}
			dataSet := DataSet{nil, metaData}
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
})
