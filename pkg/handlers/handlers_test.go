package handlers_test

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/jabley/performance-datastore/pkg/config_api"
	"github.com/jabley/performance-datastore/pkg/dataset"
	"github.com/jabley/performance-datastore/pkg/handlers"

	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type TestDataSetStorage struct {
	alive       bool
	lastUpdated *time.Time
	exists      bool
	error       error
}

func (mock *TestDataSetStorage) Alive() bool {
	return mock.alive
}

func (mock *TestDataSetStorage) Create(name string, cappedSize int64) error {
	return nil
}

func (mock *TestDataSetStorage) Empty(name string) error {
	return nil
}

func (mock *TestDataSetStorage) Exists(name string) bool {
	return mock.exists
}

func (mock *TestDataSetStorage) LastUpdated(name string) *time.Time {
	return mock.lastUpdated
}

func (mock *TestDataSetStorage) SaveRecord(name string, record map[string]interface{}) error {
	return mock.error
}

func NewTestDataSetStorage(alive bool, lastUpdated *time.Time, exists bool, err error) dataset.DataSetStorage {
	return &TestDataSetStorage{alive, lastUpdated, exists, err}
}

type TestConfigAPIClient struct {
	Error    error
	MetaData *config_api.DataSetMetaData
	DataSets []config_api.DataSetMetaData
}

func NewTestConfigAPIClient(err error, metaData *config_api.DataSetMetaData, datasets []config_api.DataSetMetaData) config_api.Client {
	return &TestConfigAPIClient{err, metaData, datasets}
}

func (c *TestConfigAPIClient) DataSet(name string) (*config_api.DataSetMetaData, error) {
	return c.MetaData, c.Error
}

func (c *TestConfigAPIClient) DataType(group string, dataType string) (*config_api.DataSetMetaData, error) {
	return c.MetaData, c.Error
}

func (c *TestConfigAPIClient) ListDataSets() ([]config_api.DataSetMetaData, error) {
	return c.DataSets, c.Error
}

func Unmarshal(body io.ReadCloser) map[string]interface{} {
	defer body.Close()
	var r map[string]interface{}
	bytes, err := ioutil.ReadAll(body)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(bytes, &r)

	if err != nil {
		panic(err)
	}

	return r
}

var _ = Describe("Healthcheck", func() {
	Describe("Status", func() {
		It("responds with a status of OK", func() {
			testServer := testHandlerServer(handlers.StatusHandler)
			defer testServer.Close()

			handlers.DataSetStorage = NewTestDataSetStorage(true, nil, false, nil)

			response, err := http.Get(testServer.URL)
			Expect(err).To(BeNil())
			Expect(response.StatusCode).To(Equal(http.StatusOK))

			body, err := readResponseBody(response)
			Expect(err).To(BeNil())
			Expect(body).To(Equal(`{"message":"database seems fine","status":"ok"}`))
		})

		It("responds with a status of ruh roh when the storage is down", func() {
			testServer := testHandlerServer(handlers.StatusHandler)
			defer testServer.Close()

			handlers.DataSetStorage = NewTestDataSetStorage(false, nil, false, nil)

			response, err := http.Get(testServer.URL)
			Expect(err).To(BeNil())
			Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))

			body, err := readResponseBody(response)
			Expect(err).To(BeNil())
			Expect(body).To(Equal(`{"errors":[{"detail":"cannot connect to database"}]}`))
		})

	})

	Describe("DataSets", func() {

		BeforeEach(func() {
			roughly30DaysAgo := time.Now().Add(time.Duration(-24*30) * time.Hour)
			handlers.DataSetStorage = NewTestDataSetStorage(true, &roughly30DaysAgo, false, nil)
		})

		It("responds with a status of OK when there are no datasets", func() {
			testServer := testHandlerServer(handlers.DataSetStatusHandler)
			defer testServer.Close()

			handlers.ConfigAPIClient = NewTestConfigAPIClient(nil, nil, nil)

			response, err := http.Get(testServer.URL)
			Expect(err).To(BeNil())
			Expect(response.StatusCode).To(Equal(http.StatusOK))

			body, err := readResponseBody(response)
			Expect(err).To(BeNil())
			Expect(body).To(Equal(`{"status":"ok"}`))
		})

		It("responds with a status of ruh roh when unable to talk to the config API", func() {
			testServer := testHandlerServer(handlers.DataSetStatusHandler)
			defer testServer.Close()

			handlers.ConfigAPIClient = NewTestConfigAPIClient(fmt.Errorf("Unable to connect to host"), nil, nil)

			response, err := http.Get(testServer.URL)
			Expect(err).To(BeNil())
			Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))

			body, err := readResponseBody(response)
			Expect(err).To(BeNil())
			Expect(body).To(Equal(`{"errors":[{"detail":"Unable to connect to host"}]}`))
		})

		It("responds with a status of OK when there are no stale datasets", func() {
			testServer := testHandlerServer(handlers.DataSetStatusHandler)
			defer testServer.Close()

			handlers.ConfigAPIClient = NewTestConfigAPIClient(nil, nil,
				[]config_api.DataSetMetaData{
					config_api.DataSetMetaData{},
					config_api.DataSetMetaData{}})

			response, err := http.Get(testServer.URL)
			Expect(err).To(BeNil())
			Expect(response.StatusCode).To(Equal(http.StatusOK))

			body, err := readResponseBody(response)
			Expect(err).To(BeNil())
			Expect(body).To(Equal(`{"status":"ok"}`))
		})

		It("responds with a status of ruh roh when there are stale datasets", func() {
			testServer := testHandlerServer(handlers.DataSetStatusHandler)
			defer testServer.Close()

			stale := config_api.DataSetMetaData{}
			stale.Published = true
			maxExpectedAge := int64(8400)
			stale.MaxExpectedAge = &maxExpectedAge

			handlers.ConfigAPIClient = NewTestConfigAPIClient(nil, nil,
				[]config_api.DataSetMetaData{
					config_api.DataSetMetaData{},
					stale})

			response, err := http.Get(testServer.URL)
			Expect(err).To(BeNil())
			Expect(response.StatusCode).To(Equal(http.StatusOK))

			body, err := readResponseBody(response)
			Expect(err).To(BeNil())
			Expect(body).To(Equal(`{"status":"not okay","detail":"1 data-set is out of date"}`))

		})
	})

	Describe("Creating data", func() {
		var testServer *httptest.Server
		var client *http.Client
		BeforeEach(func() {
			handler := handlers.NewHandler()
			testServer = testHandlerServer(handler)
			client = &http.Client{}
		})

		AfterEach(func() {
			defer testServer.Close()
		})

		Context("When there is no valid Authorization credentials", func() {
			It("Should fail with an Authorization required response when there is no Authorization header", func() {
				handlers.ConfigAPIClient = NewTestConfigAPIClient(nil,
					&config_api.DataSetMetaData{},
					nil)
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type", nil)
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))
			})

			It("Should fail with an Authorization required response when the Authorization header isn't a valid bearer token", func() {
				handlers.ConfigAPIClient = NewTestConfigAPIClient(nil,
					&config_api.DataSetMetaData{},
					nil)
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Not a bearer token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))
			})

			It("Should fail with an Authorization required response when the Authorization bearer token does not match the data set bearer token", func() {
				handlers.ConfigAPIClient = NewTestConfigAPIClient(nil,
					&config_api.DataSetMetaData{},
					nil)
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Not a bearer token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))
			})
			It("Should fail with an Authorization required response when the Authorization bearer token does not match the data set bearer token", func() {
				handlers.ConfigAPIClient = NewTestConfigAPIClient(nil,
					&config_api.DataSetMetaData{BearerToken: "the-bearer-token"},
					nil)
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Not a bearer token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))
			})
		})

		Context("When there are valid Authorization credentials", func() {
			BeforeEach(func() {
				handlers.ConfigAPIClient = NewTestConfigAPIClient(nil,
					&config_api.DataSetMetaData{BearerToken: "the-bearer-token"},
					nil)
			})

			It("Should need a request body", func() {
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Bearer the-bearer-token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusBadRequest))
			})

			It("Should need a JSON request body", func() {
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type",
					strings.NewReader("this is not JSON"))
				req.Header.Add("Authorization", "Bearer the-bearer-token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusBadRequest))
			})

			It("Should persist the update for a single object", func() {
				handlers.DataSetStorage = NewTestDataSetStorage(true, nil, true, nil)
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type",
					strings.NewReader(`{"animal":"parrot", "status":"pining"}`))
				req.Header.Add("Authorization", "Bearer the-bearer-token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusOK))
			})

			It("Should persist the update for an array of objects", func() {
				handlers.DataSetStorage = NewTestDataSetStorage(true, nil, true, nil)
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type",
					strings.NewReader(`[
	{"animal":"parrot", "status":"pining"},
	{"animal":"fish", "status":"slapping"}
]`))
				req.Header.Add("Authorization", "Bearer the-bearer-token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusOK))
			})

			It("Should propagate failure to persist the updates", func() {
				handlers.DataSetStorage = NewTestDataSetStorage(true, nil, true, fmt.Errorf("Mongo connection is down"))
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type",
					strings.NewReader(`{"animal":"parrot", "status":"pining"}`))
				req.Header.Add("Authorization", "Bearer the-bearer-token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusInternalServerError))
			})
		})
	})

	Describe("Updating data", func() {
		var testServer *httptest.Server
		var client *http.Client
		BeforeEach(func() {
			handler := handlers.NewHandler()
			testServer = testHandlerServer(handler)
			client = &http.Client{}
		})

		AfterEach(func() {
			defer testServer.Close()
		})

		Context("When there is no valid Authorization credentials", func() {
			It("Should fail with an Authorization required response when there is no Authorization header", func() {
				handlers.ConfigAPIClient = NewTestConfigAPIClient(nil,
					&config_api.DataSetMetaData{},
					nil)
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type", nil)
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))
			})

			It("Should fail with an Authorization required response when the Authorization header isn't a valid bearer token", func() {
				handlers.ConfigAPIClient = NewTestConfigAPIClient(nil,
					&config_api.DataSetMetaData{},
					nil)
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Not a bearer token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))
			})

			It("Should fail with an Authorization required response when the Authorization bearer token does not match the data set bearer token", func() {
				handlers.ConfigAPIClient = NewTestConfigAPIClient(nil,
					&config_api.DataSetMetaData{},
					nil)
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Not a bearer token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))
			})
			It("Should fail with an Authorization required response when the Authorization bearer token does not match the data set bearer token", func() {
				handlers.ConfigAPIClient = NewTestConfigAPIClient(nil,
					&config_api.DataSetMetaData{BearerToken: "the-bearer-token"},
					nil)
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Not a bearer token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))
			})
		})

		Context("When there are valid Authorization credentials", func() {
			BeforeEach(func() {
				handlers.ConfigAPIClient = NewTestConfigAPIClient(nil,
					&config_api.DataSetMetaData{
						BearerToken: "the-bearer-token",
						Name:        "the-dataset"},
					nil)
			})

			It("Should need a request body", func() {
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Bearer the-bearer-token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusBadRequest))
			})

			It("Should need a JSON request body", func() {
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type",
					strings.NewReader("this is not JSON"))
				req.Header.Add("Authorization", "Bearer the-bearer-token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusBadRequest))
			})

			It("Should persist the update emptying the data set", func() {
				handlers.DataSetStorage = NewTestDataSetStorage(true, nil, true, nil)
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type",
					strings.NewReader(`[]`))
				req.Header.Add("Authorization", "Bearer the-bearer-token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusOK))

				body, err := readResponseBody(response)
				Expect(err).Should(BeNil())
				Expect(body).Should(Equal(`{"message":"the-dataset now contains 0 records","status":"OK"}`))
			})

			It("Should fail to update if the array isn't empty", func() {
				handlers.DataSetStorage = NewTestDataSetStorage(true, nil, true, nil)
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type",
					strings.NewReader(`[
	{"animal":"parrot", "status":"pining"}
]`))
				req.Header.Add("Authorization", "Bearer the-bearer-token")
				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusBadRequest))
				doc := Unmarshal(response.Body)
				errors := doc["errors"].([]interface{})
				Expect(len(errors)).Should(Equal(1))
				error := errors[0].(map[string]interface{})
				Expect(error["detail"]).Should(Equal("Not implemented: you can only pass an empty JSON list"))
			})
		})

	})
})
