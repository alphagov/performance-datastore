package dataset_test

import (
	"github.com/jabley/performance-datastore/pkg/config_api"
	. "github.com/jabley/performance-datastore/pkg/dataset"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

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
			record := map[string]string{"foo": "foo", "bar": "bar"}
			records := []map[string]string{record}
			actual := dataSet.ProcessAutoIds(records, nil)
			Expect(records).Should(Equal(actual))
		})

	})
})
