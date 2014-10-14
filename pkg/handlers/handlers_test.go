package handlers_test

import (
	"net/http"

	"github.com/jabley/performance-datastore/pkg/config_api"
	"github.com/jabley/performance-datastore/pkg/dataset"
	"github.com/jabley/performance-datastore/pkg/handlers"

	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type TestDataSetStorage struct {
	IsAlive bool
}

func (mock *TestDataSetStorage) Alive() bool {
	return mock.IsAlive
}

func (mock *TestDataSetStorage) Create(name string, cappedSize int64) error {
	return nil
}

func (mock *TestDataSetStorage) Exists(name string) bool {
	return false
}

func (mock *TestDataSetStorage) LastUpdated(name string) *time.Time {
	return nil
}

func NewTestDataSetStorage(alive bool) dataset.DataSetStorage {
	return &TestDataSetStorage{alive}
}

type TestConfigAPIClient struct {
	Error    error
	MetaData *config_api.DataSetMetaData
	DataSets []config_api.DataSetMetaData
}

func NewTestConfigAPIClient() config_api.Client {
	return &TestConfigAPIClient{}
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

			handlers.DataSetStorage = NewTestDataSetStorage(true)

			response, err := http.Get(testServer.URL)
			Expect(err).To(BeNil())
			Expect(response.StatusCode).To(Equal(http.StatusOK))

			body, err := readResponseBody(response)
			Expect(err).To(BeNil())
			Expect(body).To(Equal(`{"message":"database seems fine","status":"OK"}`))
		})

		It("responds with a status of ruh roh when the storage is down", func() {
			testServer := testHandlerServer(handlers.StatusHandler)
			defer testServer.Close()

			handlers.DataSetStorage = NewTestDataSetStorage(false)

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
			handlers.DataSetStorage = NewTestDataSetStorage(true)
		})

		It("responds with a status of OK when there are no datasets", func() {
			testServer := testHandlerServer(handlers.DataSetStatusHandler)
			defer testServer.Close()

			handlers.ConfigAPIClient = NewTestConfigAPIClient()

			response, err := http.Get(testServer.URL)
			Expect(err).To(BeNil())
			Expect(response.StatusCode).To(Equal(http.StatusOK))

			body, err := readResponseBody(response)
			Expect(err).To(BeNil())
			Expect(body).To(Equal(`{"status":"OK"}`))
		})

	})
})
