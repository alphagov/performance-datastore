package dataset

import (
	"encoding/json"
	"github.com/alphagov/performance-datastore/pkg/config"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

func Unmarshal(t string) map[string]interface{} {
	var r map[string]interface{}
	err := json.Unmarshal([]byte(t), &r)
	Expect(err).To(BeNil())
	return r
}

var _ = Describe("Dataset", func() {
	var (
		metaData config.DataSetMetaData
		dataSet  DataSet
		errors   []error
	)

	BeforeEach(func() {
		metaData = config.DataSetMetaData{}
		dataSet = DataSet{nil, metaData}
		errors = []error{}
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
			records := []map[string]interface{}{record}
			actual := dataSet.ProcessAutoIDs(records, &errors)
			Expect(records).Should(Equal(actual))
		})

		It("Should add an auto ID based on a single field", func() {
			dataSet.MetaData.AutoIds = []string{"foo"}
			record := Unmarshal(`{"foo": "foo", "bar": "bar"}`)
			records := []map[string]interface{}{record}
			actual := dataSet.ProcessAutoIDs(records, &errors)
			expected := Unmarshal(`{"foo": "foo", "bar": "bar","_id": "Zm9v"}`)
			Expect([]map[string]interface{}{expected}).Should(Equal(actual))
		})

		It("Should add an auto ID based on multiple fields", func() {
			dataSet.MetaData.AutoIds = []string{"foo", "bar"}
			record := Unmarshal(`{"foo": "foo", "bar": "bar"}`)
			records := []map[string]interface{}{record}
			actual := dataSet.ProcessAutoIDs(records, &errors)
			expected := Unmarshal(`{"foo": "foo", "bar": "bar","_id": "Zm9vLmJhcg=="}`)
			Expect([]map[string]interface{}{expected}).Should(Equal(actual))
		})
	})

	Describe("Timestamps", func() {
		It("Should not modify a record which does not contain a timestamp", func() {
			record := Unmarshal(`{}`)
			records := []map[string]interface{}{record}
			dataSet.ParseTimestamps(records, &errors)
			Expect(len(errors)).Should(Equal(0))
			expected := map[string]interface{}{}
			Expect([]map[string]interface{}{expected}).Should(Equal(records))
		})

		It("Should convert valid string timestamp to time.Date", func() {
			record := Unmarshal(`{"_timestamp": "2012-12-12T00:00:00"}`)
			records := []map[string]interface{}{record}
			dataSet.ParseTimestamps(records, &errors)
			Expect(len(errors)).Should(Equal(0))
			expected := map[string]interface{}{"_timestamp": time.Date(2012, 12, 12, 0, 0, 0, 0, time.UTC)}
			Expect([]map[string]interface{}{expected}).Should(Equal(records))
		})

		It("Should return an error with an invalid timestamp", func() {
			record := Unmarshal(`{"_timestamp": "invalid"}`)
			records := []map[string]interface{}{record}
			dataSet.ParseTimestamps(records, &errors)
			Expect(len(errors)).Should(Equal(1))
			expected := map[string]interface{}{"_timestamp": "invalid"}
			Expect([]map[string]interface{}{expected}).Should(Equal(records))
			Expect(errors[0].Error()).Should(Equal("_timestamp is not a valid timestamp, it must be ISO8601"))
		})
	})

	Describe("Validation", func() {
		It("Should not allow invalid keys", func() {
			record := Unmarshal(`{"1": "foo"}`)
			records := []map[string]interface{}{record}
			dataSet.ValidateRecords(records, &errors)
			Expect(len(errors)).Should(Equal(1))
			expected := Unmarshal(`{"1": "foo"}`)
			Expect([]map[string]interface{}{expected}).Should(Equal(records))
		})

		It("Should not allow things that look like unreserved keys", func() {
			record := Unmarshal(`{"_foo": "foo"}`)
			records := []map[string]interface{}{record}
			dataSet.ValidateRecords(records, &errors)
			Expect(len(errors)).Should(Equal(1))
			expected := Unmarshal(`{"_foo": "foo"}`)
			Expect([]map[string]interface{}{expected}).Should(Equal(records))
		})

		It("Should allow things that look like reserved keys", func() {
			record := Unmarshal(`{"_id": "foo"}`)
			records := []map[string]interface{}{record}
			dataSet.ValidateRecords(records, &errors)
			Expect(len(errors)).Should(Equal(0))
			expected := Unmarshal(`{"_id": "foo"}`)
			Expect([]map[string]interface{}{expected}).Should(Equal(records))
		})

		It("Should not allow values that aren't whitelisted", func() {
			record := Unmarshal(`{"id": ["foo", "bar"]}`)
			records := []map[string]interface{}{record}
			dataSet.ValidateRecords(records, &errors)
			Expect(len(errors)).Should(Equal(1))
			expected := Unmarshal(`{"id": ["foo", "bar"]}`)
			Expect([]map[string]interface{}{expected}).Should(Equal(records))
		})

		It("Should only allow timestamps that look contain a time.Time instance", func() {
			record := map[string]interface{}{"_timestamp": time.Date(2012, 12, 12, 0, 0, 0, 0, time.UTC)}
			records := []map[string]interface{}{record}
			dataSet.ValidateRecords(records, &errors)
			Expect(len(errors)).Should(Equal(0))
			expected := map[string]interface{}{"_timestamp": time.Date(2012, 12, 12, 0, 0, 0, 0, time.UTC)}
			Expect([]map[string]interface{}{expected}).Should(Equal(records))
		})

		It("Should not allow an int as an _id", func() {
			record := map[string]interface{}{"_id": 1}
			records := []map[string]interface{}{record}
			dataSet.ValidateRecords(records, &errors)
			Expect(len(errors)).Should(Equal(1))
			expected := map[string]interface{}{"_id": 1}
			Expect([]map[string]interface{}{expected}).Should(Equal(records))
		})

		It("Should not allow a string with spaces as an _id", func() {
			record := map[string]interface{}{"_id": "this should fail"}
			records := []map[string]interface{}{record}
			dataSet.ValidateRecords(records, &errors)
			Expect(len(errors)).Should(Equal(1))
			expected := map[string]interface{}{"_id": "this should fail"}
			Expect([]map[string]interface{}{expected}).Should(Equal(records))
		})
	})

	Describe("Schema", func() {
		BeforeEach(func() {
			dataSet.MetaData.Schema = json.RawMessage(`
{
  "definitions": {
  	"_timestamp": {
    	"$schema": "http://json-schema.org/schema#",
     	"required": [
      	"_timestamp"
     	],
     	"type": "object",
     	"properties": {
      	"_timestamp": {
       		"type": "string",
       		"description": "An ISO8601 formatted date time",
       		"format": "date-time"
      	}
     	},
     	"title": "Timestamps"
    }
  },
  "description": "Schema for deposit-foreign-marriage/journey",
  "allOf": [
  	{
     "$ref": "#/definitions/_timestamp"
    }
  ]
}`)
		})

		It("Should validate against a simple schema", func() {
			record := map[string]interface{}{"_timestamp": "2012-12-12T00:00:00"}
			records := []map[string]interface{}{record}
			dataSet.ValidateAgainstSchema(records, &errors)
			Expect(len(errors)).Should(Equal(0))
			expected := map[string]interface{}{"_timestamp": "2012-12-12T00:00:00"}
			Expect([]map[string]interface{}{expected}).Should(Equal(records))
		})

		It("Should propagate schema validation failures", func() {
			record := map[string]interface{}{"foo": "bar"}
			records := []map[string]interface{}{record}
			dataSet.ValidateAgainstSchema(records, &errors)
			Expect(len(errors)).Should(Equal(2))
			Expect(errors[0].Error()).Should(Equal(`"_timestamp" property is missing and required`))
			Expect(errors[1].Error()).Should(Equal(`must validate all the schemas (allOf)`))
			expected := map[string]interface{}{"foo": "bar"}
			Expect([]map[string]interface{}{expected}).Should(Equal(records))
		})

		It("date-time field is validated", func() {
			record := map[string]interface{}{"_timestamp": "bar"}
			records := []map[string]interface{}{record}
			dataSet.ValidateAgainstSchema(records, &errors)
			Expect(len(errors)).Should(Equal(0))
			expected := map[string]interface{}{"_timestamp": "bar"}
			Expect([]map[string]interface{}{expected}).Should(Equal(records))

			// Currently the go implementation does the MAY aspect of the spec,
			// and doesn't validate formats.
			// http://tools.ietf.org/html/draft-fge-json-schema-validation-00#section-7.2
			// We validate that separately ourselves for _timestamp. If other schemas are defined
			// with "format": "date-time", we may want to add those to our own validation.
		})
	})

	Describe("Period Data", func() {
		It("Should add period data for richer querying", func() {
			record := map[string]interface{}{"_timestamp": time.Date(2012, 12, 12, 12, 12, 0, 0, time.UTC)}
			records := []map[string]interface{}{record}
			dataSet.AddPeriodData(records)
			expected := map[string]interface{}{
				"_timestamp":        time.Date(2012, 12, 12, 12, 12, 0, 0, time.UTC),
				"_hour_start_at":    time.Date(2012, 12, 12, 12, 0, 0, 0, time.UTC),
				"_day_start_at":     time.Date(2012, 12, 12, 0, 0, 0, 0, time.UTC),
				"_week_start_at":    time.Date(2012, 12, 10, 0, 0, 0, 0, time.UTC),
				"_month_start_at":   time.Date(2012, 12, 1, 0, 0, 0, 0, time.UTC),
				"_quarter_start_at": time.Date(2012, 10, 1, 0, 0, 0, 0, time.UTC),
				"_year_start_at":    time.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC)}
			Expect([]map[string]interface{}{expected}).Should(Equal(records))
		})

	})
})

var _ = Describe("Periods", func() {
	Describe("FieldNames", func() {
		It("should have appropriate FieldNames", func() {
			Expect(Hour.FieldName()).Should(Equal("_hour_start_at"))
			Expect(Day.FieldName()).Should(Equal("_day_start_at"))
			Expect(Week.FieldName()).Should(Equal("_week_start_at"))
			Expect(Month.FieldName()).Should(Equal("_month_start_at"))
			Expect(Quarter.FieldName()).Should(Equal("_quarter_start_at"))
			Expect(Year.FieldName()).Should(Equal("_year_start_at"))
		})
	})
	Describe("Values", func() {
		var currentTime time.Time

		BeforeEach(func() {
			currentTime = time.Date(2006, time.January, 2, 22, 04, 05, 0, time.UTC)
		})

		It("Hour value should be start of hour", func() {
			Expect(Hour.Value(currentTime)).Should(Equal(time.Date(2006, time.January, 2, 22, 0, 0, 0, time.UTC)))
		})

		It("Day value should be midnight UTC of the given day", func() {
			Expect(Day.Value(currentTime)).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
		})

		Describe("Week", func() {
			oneDay := time.Hour * 24

			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime)).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
			})
			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime.Add(oneDay))).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
			})
			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime.Add(oneDay * 2))).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
			})
			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime.Add(oneDay * 3))).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
			})
			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime.Add(oneDay * 4))).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
			})
			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime.Add(oneDay * 5))).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
			})
			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime.Add(oneDay * 6))).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
			})
			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime.Add(oneDay * 7))).Should(Equal(time.Date(2006, time.January, 9, 0, 0, 0, 0, time.UTC)))
			})
		})

		It("Month value should be midnight UTC of the given day", func() {
			Expect(Month.Value(currentTime)).Should(Equal(time.Date(2006, time.January, 1, 0, 0, 0, 0, time.UTC)))
		})

		Describe("Quarter", func() {
			Context("January quarter", func() {
				It("Quarter for January", func() {
					Expect(Quarter.Value(time.Date(2006, time.January, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.January, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for February", func() {
					Expect(Quarter.Value(time.Date(2006, time.February, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.January, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for March", func() {
					Expect(Quarter.Value(time.Date(2006, time.March, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.January, 1, 0, 0, 0, 0, time.UTC)))
				})
			})
			Context("April Quarter", func() {
				It("Quarter for April", func() {
					Expect(Quarter.Value(time.Date(2006, time.April, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.April, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for May", func() {
					Expect(Quarter.Value(time.Date(2006, time.May, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.April, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for June", func() {
					Expect(Quarter.Value(time.Date(2006, time.June, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.April, 1, 0, 0, 0, 0, time.UTC)))
				})
			})
			Context("July Quarter", func() {
				It("Quarter for July", func() {
					Expect(Quarter.Value(time.Date(2006, time.July, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.July, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for August", func() {
					Expect(Quarter.Value(time.Date(2006, time.August, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.July, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for September", func() {
					Expect(Quarter.Value(time.Date(2006, time.September, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.July, 1, 0, 0, 0, 0, time.UTC)))
				})
			})
			Context("October Quarter", func() {
				It("Quarter for October", func() {
					Expect(Quarter.Value(time.Date(2006, time.October, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.October, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for November", func() {
					Expect(Quarter.Value(time.Date(2006, time.November, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.October, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for December", func() {
					Expect(Quarter.Value(time.Date(2006, time.December, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.October, 1, 0, 0, 0, 0, time.UTC)))
				})
			})
		})

		It("Year value should be midnight UTC of the given day", func() {
			Expect(Year.Value(currentTime)).Should(Equal(time.Date(2006, time.January, 1, 0, 0, 0, 0, time.UTC)))
		})
	})
})
