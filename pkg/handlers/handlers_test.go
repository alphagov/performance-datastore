package handlers_test

import (
	"net/http"

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

var _ = Describe("Healthcheck", func() {
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
