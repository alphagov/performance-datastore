package handlers_test

import (
	"fmt"
	"net/http"

	"github.com/jabley/performance-datastore/pkg/config_api"
	"github.com/jabley/performance-datastore/pkg/dataset"
	"github.com/jabley/performance-datastore/pkg/handlers"

	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type TestDataSetStorage struct {
	alive       bool
	lastUpdated *time.Time
}

func (mock *TestDataSetStorage) Alive() bool {
	return mock.alive
}

func (mock *TestDataSetStorage) Create(name string, cappedSize int64) error {
	return nil
}

func (mock *TestDataSetStorage) Exists(name string) bool {
	return false
}

func (mock *TestDataSetStorage) LastUpdated(name string) *time.Time {
	return mock.lastUpdated
}

func NewTestDataSetStorage(alive bool, lastUpdated *time.Time) dataset.DataSetStorage {
	return &TestDataSetStorage{alive, lastUpdated}
}

type TestConfigAPIClient struct {
	Error    error
	MetaData *config_api.DataSetMetaData
	DataSets []config_api.DataSetMetaData
}

func NewTestConfigAPIClient(err error, datasets []config_api.DataSetMetaData) config_api.Client {
	return &TestConfigAPIClient{err, nil, datasets}
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

var _ = Describe("Healthcheck", func() {
	Describe("Status", func() {
		It("responds with a status of OK", func() {
			testServer := testHandlerServer(handlers.StatusHandler)
			defer testServer.Close()

			handlers.DataSetStorage = NewTestDataSetStorage(true, nil)

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

			handlers.DataSetStorage = NewTestDataSetStorage(false, nil)

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
			handlers.DataSetStorage = NewTestDataSetStorage(true, &roughly30DaysAgo)
		})

		It("responds with a status of OK when there are no datasets", func() {
			testServer := testHandlerServer(handlers.DataSetStatusHandler)
			defer testServer.Close()

			handlers.ConfigAPIClient = NewTestConfigAPIClient(nil, nil)

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

			handlers.ConfigAPIClient = NewTestConfigAPIClient(fmt.Errorf("Unable to connect to host"), nil)

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

			handlers.ConfigAPIClient = NewTestConfigAPIClient(nil,
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

			handlers.ConfigAPIClient = NewTestConfigAPIClient(nil,
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
})
