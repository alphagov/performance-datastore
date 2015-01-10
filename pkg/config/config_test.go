package config

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/Sirupsen/logrus"
	"github.com/onsi/gomega/ghttp"

	"net/http"
)

var _ = Describe("Config API", func() {

	var server *ghttp.Server
	var client Client

	BeforeEach(func() {
		server = ghttp.NewServer()
		client = NewClient(server.URL(), "EMPTY", logrus.New())
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

			It("gracefully handles non-JSON responses from remote API", func() {
				server.RouteToHandler("GET", "/data-sets/deposit_foreign_marriage_journey",
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/data-sets/deposit_foreign_marriage_journey"),
						ghttp.RespondWith(http.StatusOK, `This is not JSON`)))

				metaData, err := client.DataSet("deposit_foreign_marriage_journey")
				Expect(metaData).To(BeNil())
				Expect(err).ToNot(BeNil())
			})
		})

		Describe("ListDataSets", func() {
			It("responds with a status of OK", func() {

				server.RouteToHandler("GET", "/data-sets",
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/data-sets"),
						ghttp.RespondWith(http.StatusOK, `
[
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
  },
 {
  "name": "evl_channel_volumetrics",
  "data_group": "vehicle-licensing",
  "data_type": "channels",
  "raw_queries_allowed": false,
  "bearer_token": "another-woo-hoo",
  "upload_format": "excel",
  "upload_filters": [
   "backdrop.core.upload.filters.first_sheet_filter",
   "backdrop.contrib.evl_upload_filters.channel_volumetrics"
  ],
  "auto_ids": [],
  "queryable": true,
  "realtime": false,
  "capped_size": 0,
  "max_age_expected": 2678400,
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
   "description": "Schema for vehicle-licensing/channels",
   "allOf": [
    {
     "$ref": "#/definitions/_timestamp"
    }
   ]
  }
 }
]`)))

				metaData, err := client.ListDataSets()
				Expect(metaData).ToNot(BeNil())
				Expect(err).To(BeNil())
				Expect(len(metaData)).To(Equal(2))

				Expect(metaData[0].Name).To(Equal("deposit_foreign_marriage_journey"))
				Expect(metaData[0].DataGroup).To(Equal("deposit-foreign-marriage"))
				Expect(metaData[0].DataType).To(Equal("journey"))
				Expect(metaData[0].AllowRawQueries).To(Equal(true))
				Expect(metaData[0].BearerToken).To(Equal("woo-hoo"))
				Expect(metaData[0].UploadFormat).To(Equal("csv"))
				Expect(len(metaData[0].UploadFilters)).To(Equal(1))
				Expect(metaData[0].UploadFilters[0]).To(Equal("backdrop.core.upload.filters.first_sheet_filter"))
				Expect(len(metaData[0].AutoIds)).To(Equal(0))
				Expect(metaData[0].Queryable).To(Equal(true))
				Expect(metaData[0].Realtime).To(Equal(false))
				Expect(metaData[0].CappedSize).To(Equal(int64(0)))
				Expect(metaData[0].MaxExpectedAge).To(BeNil())
				Expect(metaData[0].Published).To(Equal(true))
				Expect(metaData[0].Schema).ToNot(BeNil())

				Expect(metaData[1].Name).To(Equal("evl_channel_volumetrics"))
				Expect(metaData[1].DataGroup).To(Equal("vehicle-licensing"))
				Expect(metaData[1].DataType).To(Equal("channels"))
				Expect(metaData[1].AllowRawQueries).To(Equal(false))
				Expect(metaData[1].BearerToken).To(Equal("another-woo-hoo"))
				Expect(metaData[1].UploadFormat).To(Equal("excel"))
				Expect(len(metaData[1].UploadFilters)).To(Equal(2))
				Expect(metaData[1].UploadFilters[0]).To(Equal("backdrop.core.upload.filters.first_sheet_filter"))
				Expect(metaData[1].UploadFilters[1]).To(Equal("backdrop.contrib.evl_upload_filters.channel_volumetrics"))
				Expect(len(metaData[1].AutoIds)).To(Equal(0))
				Expect(metaData[1].Queryable).To(Equal(true))
				Expect(metaData[1].Realtime).To(Equal(false))
				Expect(metaData[1].CappedSize).To(Equal(int64(0)))
				Expect(*(metaData[1].MaxExpectedAge)).To(Equal(int64(2678400)))
				Expect(metaData[1].Published).To(Equal(true))
				Expect(metaData[1].Schema).ToNot(BeNil())
			})

			It("gracefully handles failure in remote API", func() {
				server.RouteToHandler("GET", "/data-sets",
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/data-sets"),
						ghttp.RespondWith(http.StatusInternalServerError, ``)))

				metaData, err := client.ListDataSets()
				Expect(metaData).To(BeNil())
				Expect(err).ToNot(BeNil())
			})

			It("gracefully handles non-JSON responses from remote API", func() {
				server.RouteToHandler("GET", "/data-sets",
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/data-sets"),
						ghttp.RespondWith(http.StatusOK, `This is not JSON`)))

				metaData, err := client.ListDataSets()
				Expect(metaData).To(BeNil())
				Expect(err).ToNot(BeNil())
			})
		})

		Describe("DataType", func() {
			It("responds with a status of OK", func() {

				server.RouteToHandler("GET", "/data-sets",
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/data-sets"),
						ghttp.RespondWith(http.StatusOK, `
[
 {
  "name": "evl_channel_volumetrics",
  "data_group": "vehicle-licensing",
  "data_type": "channels",
  "raw_queries_allowed": false,
  "bearer_token": "another-woo-hoo",
  "upload_format": "excel",
  "upload_filters": [
   "backdrop.core.upload.filters.first_sheet_filter",
   "backdrop.contrib.evl_upload_filters.channel_volumetrics"
  ],
  "auto_ids": [],
  "queryable": true,
  "realtime": false,
  "capped_size": 0,
  "max_age_expected": 2678400,
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
   "description": "Schema for vehicle-licensing/channels",
   "allOf": [
    {
     "$ref": "#/definitions/_timestamp"
    }
   ]
  }
 }
]`)))

				metaData, err := client.DataType("vehicle-licensing", "channels")
				Expect(metaData).ToNot(BeNil())
				Expect(err).To(BeNil())

				Expect(metaData.Name).To(Equal("evl_channel_volumetrics"))
				Expect(metaData.DataGroup).To(Equal("vehicle-licensing"))
				Expect(metaData.DataType).To(Equal("channels"))
				Expect(metaData.AllowRawQueries).To(Equal(false))
				Expect(metaData.BearerToken).To(Equal("another-woo-hoo"))
				Expect(metaData.UploadFormat).To(Equal("excel"))
				Expect(len(metaData.UploadFilters)).To(Equal(2))
				Expect(metaData.UploadFilters[0]).To(Equal("backdrop.core.upload.filters.first_sheet_filter"))
				Expect(metaData.UploadFilters[1]).To(Equal("backdrop.contrib.evl_upload_filters.channel_volumetrics"))
				Expect(len(metaData.AutoIds)).To(Equal(0))
				Expect(metaData.Queryable).To(Equal(true))
				Expect(metaData.Realtime).To(Equal(false))
				Expect(metaData.CappedSize).To(Equal(int64(0)))
				Expect(*(metaData.MaxExpectedAge)).To(Equal(int64(2678400)))
				Expect(metaData.Published).To(Equal(true))
				Expect(metaData.Schema).ToNot(BeNil())
			})

			It("gracefully handles failure in remote API", func() {
				server.RouteToHandler("GET", "/data-sets",
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/data-sets"),
						ghttp.RespondWith(http.StatusInternalServerError, ``)))

				metaData, err := client.DataType("vehicle-licensing", "channels")
				Expect(metaData).To(BeNil())
				Expect(err).ToNot(BeNil())
			})

			It("gracefully handles non-JSON responses from remote API", func() {
				server.RouteToHandler("GET", "/data-sets",
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/data-sets"),
						ghttp.RespondWith(http.StatusOK, `This is not JSON`)))

				metaData, err := client.DataType("vehicle-licensing", "channels")
				Expect(metaData).To(BeNil())
				Expect(err).ToNot(BeNil())
			})
		})

	})

})
