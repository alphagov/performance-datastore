package config_api_test

import (
	. "github.com/jabley/performance-datastore/pkg/config_api"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/onsi/gomega/ghttp"
	"net/http"
)

var _ = Describe("Config API", func() {

	var server *ghttp.Server
	var client Client

	BeforeEach(func() {
		server = ghttp.NewServer()
		client = NewClient(server.URL(), "EMPTY")
	})

	AfterEach(func() {
		server.Close()
	})

	Describe("Unmarshalling", func() {
		Describe("DataSet", func() {
			It("responds with a status of OK", func() {

				server.RouteToHandler("GET", "/data-sets/deposit_foreign_marriage_journey",
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/data-sets/deposit_foreign_marriage_journey"),
						ghttp.RespondWith(http.StatusOK, `
{
  "name": "deposit_foreign_marriage_journey",
  "data_group": "deposit-foreign-marriage",
  "data_type": "journey",
  "raw_queries_allowed": true,
  "bearer_token": "woo-hoo",
  "upload_format": "csv",
  "upload_filters": [
   "backdrop.core.upload.filters.first_sheet_filter"
  ],
  "auto_ids": [],
  "queryable": true,
  "realtime": false,
  "capped_size": 0,
  "max_age_expected": null,
  "published": true,
  "schema": {
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
  }
 }`)))

				metaData, err := client.DataSet("deposit_foreign_marriage_journey")
				Expect(err).To(BeNil())
				Expect(metaData).ToNot(BeNil())
				Expect(metaData.Name).To(Equal("deposit_foreign_marriage_journey"))
				Expect(metaData.DataGroup).To(Equal("deposit-foreign-marriage"))
				Expect(metaData.DataType).To(Equal("journey"))
				Expect(metaData.AllowRawQueries).To(Equal(true))
				Expect(metaData.BearerToken).To(Equal("woo-hoo"))
				Expect(metaData.UploadFormat).To(Equal("csv"))
				Expect(len(metaData.UploadFilters)).To(Equal(1))
				Expect(metaData.UploadFilters[0]).To(Equal("backdrop.core.upload.filters.first_sheet_filter"))
				Expect(len(metaData.AutoIds)).To(Equal(0))
				Expect(metaData.Queryable).To(Equal(true))
				Expect(metaData.Realtime).To(Equal(false))
				Expect(metaData.CappedSize).To(Equal(int64(0)))
				Expect(metaData.MaxExpectedAge).To(BeNil())
				Expect(metaData.Published).To(Equal(true))
				Expect(metaData.Schema).ToNot(BeNil())
			})

			It("gracefully handles failure in remote API", func() {
				server.RouteToHandler("GET", "/data-sets/deposit_foreign_marriage_journey",
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/data-sets/deposit_foreign_marriage_journey"),
						ghttp.RespondWith(http.StatusInternalServerError, ``)))

				metaData, err := client.DataSet("deposit_foreign_marriage_journey")
				Expect(metaData).To(BeNil())
				Expect(err).ToNot(BeNil())
			})
		})
	})

})
