package handlers

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/Sirupsen/logrus"
	"github.com/go-martini/martini"
	"github.com/quipo/statsd"

	"github.com/alphagov/performance-datastore/pkg/config"
	"github.com/alphagov/performance-datastore/pkg/dataset"

	"reflect"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/onsi/gomega/types"
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
	return mock.error
}

func (mock *TestDataSetStorage) Empty(name string) error {
	return mock.error
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

func (mock *TestDataSetStorage) options(opts ...TestDataSetStorageOption) (previous TestDataSetStorageOption) {
	for _, opt := range opts {
		previous = opt(mock)
	}
	return previous
}

type TestDataSetStorageOption func(*TestDataSetStorage) TestDataSetStorageOption

func Alive(alive bool) TestDataSetStorageOption {
	return func(t *TestDataSetStorage) TestDataSetStorageOption {
		previous := t.alive
		t.alive = alive
		return Alive(previous)
	}
}

func Exists(exists bool) TestDataSetStorageOption {
	return func(t *TestDataSetStorage) TestDataSetStorageOption {
		previous := t.exists
		t.exists = exists
		return Exists(previous)
	}
}

func SomeError(err error) TestDataSetStorageOption {
	return func(t *TestDataSetStorage) TestDataSetStorageOption {
		previous := t.error
		t.error = err
		return SomeError(previous)
	}
}

func LastUpdated(lastUpdated *time.Time) TestDataSetStorageOption {
	return func(t *TestDataSetStorage) TestDataSetStorageOption {
		previous := t.lastUpdated
		t.lastUpdated = lastUpdated
		return LastUpdated(previous)
	}
}

func newTestDataSetStorage(options ...TestDataSetStorageOption) dataset.DataSetStorage {
	result := TestDataSetStorage{}
	result.options(options...)
	return &result
}

type TestConfigAPIClient struct {
	Error    error
	MetaData *config.DataSetMetaData
	DataSets []config.DataSetMetaData
}

type TestConfigAPIClientOption func(*TestConfigAPIClient) TestConfigAPIClientOption

func newTestConfigAPIClient(options ...TestConfigAPIClientOption) config.Client {
	result := TestConfigAPIClient{}
	result.options(options...)
	return &result
}

func (c *TestConfigAPIClient) options(opts ...TestConfigAPIClientOption) (previous TestConfigAPIClientOption) {
	for _, opt := range opts {
		previous = opt(c)
	}
	return previous
}

func (c *TestConfigAPIClient) DataSet(name string) (*config.DataSetMetaData, error) {
	return c.MetaData, c.Error
}

func (c *TestConfigAPIClient) DataType(group string, dataType string) (*config.DataSetMetaData, error) {
	return c.MetaData, c.Error
}

func (c *TestConfigAPIClient) ListDataSets() ([]config.DataSetMetaData, error) {
	return c.DataSets, c.Error
}

func ClientError(e error) TestConfigAPIClientOption {
	return func(c *TestConfigAPIClient) TestConfigAPIClientOption {
		previous := c.Error
		c.Error = e
		return ClientError(previous)
	}
}

func MetaData(metaData *config.DataSetMetaData) TestConfigAPIClientOption {
	return func(c *TestConfigAPIClient) TestConfigAPIClientOption {
		previous := c.MetaData
		c.MetaData = metaData
		return MetaData(previous)
	}
}

func DataSets(dataSets ...config.DataSetMetaData) TestConfigAPIClientOption {
	return func(c *TestConfigAPIClient) TestConfigAPIClientOption {
		previous := c.DataSets
		c.DataSets = dataSets
		return DataSets(previous...)
	}
}

// incOperation captures information about statsd Incr invocations
type incOperation struct {
	stat  string
	count int64
}

type testStatsdClient struct {
	incOps []incOperation
}

func newTestStatsdClient() statsd.Statsd {
	return &testStatsdClient{}
}

func (t *testStatsdClient) Close() error {
	return nil
}

func (t *testStatsdClient) Incr(stat string, count int64) error {
	t.incOps = append(t.incOps, incOperation{stat, count})
	return nil
}

func (t *testStatsdClient) Decr(stat string, count int64) error {
	return nil
}

func (t *testStatsdClient) Timing(stat string, delta int64) error {
	return nil
}

func (t *testStatsdClient) Gauge(stat string, value int64) error {
	return nil
}

func (t *testStatsdClient) Absolute(stat string, value int64) error {
	return nil
}

func (t *testStatsdClient) Total(stat string, value int64) error {
	return nil
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

func newHandler(maxBodySize int) http.Handler {
	logger := logrus.New()
	logger.Level = logrus.WarnLevel
	return NewHandler(maxBodySize, logger)
}

func newErrorAPIResponse(errorDetail string) APIResponse {
	return APIResponse{
		Status:  "error",
		Message: errorDetail,
		Errors: []ErrorInfo{
			ErrorInfo{Detail: errorDetail}}}
}

var _ = Describe("Handlers", func() {

	var testServer *httptest.Server

	BeforeEach(func() {
		testServer = testHandlerServer(newHandler(10000000))
		martini.Env = martini.Test
		StatsdClient = NewStatsDClient("localhost:8125", "datastore.")
	})

	AfterEach(func() {
		testServer.Close()
	})

	Describe("Status", func() {
		Context("With working Storage", func() {

			BeforeEach(func() {
				DataSetStorage = newTestDataSetStorage(Alive(true))
			})

			It("responds with a status of OK", func() {
				response, err := http.Get(testServer.URL + "/_status")
				Expect(err).To(BeNil())
				Expect(response.StatusCode).To(Equal(http.StatusOK))

				Expect(response).To(EqualAPIResponse(APIResponse{
					Status:  "ok",
					Message: "database seems fine"}))
			})

			It("responds to HEAD requests", func() {
				response, err := http.Head(testServer.URL + "/_status")
				Expect(err).To(BeNil())
				Expect(response.StatusCode).To(Equal(http.StatusOK))
			})

			It("does not respond to POST requests", func() {
				response, err := http.Post(testServer.URL+"/_status",
					"application/json",
					strings.NewReader(`{"foo":"foo"}`))
				Expect(err).To(BeNil())
				// This is the preferred implementation but Martini routing doesn't do
				// that – yet!
				// So we've added an explicit route and handler for this
				Expect(response.StatusCode).To(Equal(http.StatusMethodNotAllowed))
			})
		})

		Context("with unavailable storage", func() {
			It("responds with a status of ruh roh when the storage is down", func() {
				DataSetStorage = newTestDataSetStorage(Alive(false))

				response, err := http.Get(testServer.URL + "/_status")
				Expect(err).To(BeNil())
				Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))

				Expect(response).To(EqualAPIResponse(APIResponse{
					Status:  "error",
					Message: "cannot connect to database",
					Errors: []ErrorInfo{
						ErrorInfo{Detail: "cannot connect to database"}}}))
			})
		})
	})

	Describe("DataSets", func() {

		var roughly30DaysAgo time.Time

		BeforeEach(func() {
			roughly30DaysAgo = time.Now().Add(time.Duration(-24*30) * time.Hour)
			DataSetStorage = newTestDataSetStorage(Alive(true), LastUpdated(&roughly30DaysAgo))
		})

		It("responds with a status of OK when there are no datasets", func() {
			testServer := testHandlerServer(DataSetStatusHandler)
			defer testServer.Close()

			ConfigAPIClient = newTestConfigAPIClient()

			response, err := http.Get(testServer.URL)
			Expect(err).To(BeNil())
			Expect(response.StatusCode).To(Equal(http.StatusOK))

			Expect(response).To(EqualAPIResponse(APIResponse{
				Status: "ok"}))
		})

		It("responds with a status of ruh roh when unable to talk to the config API", func() {
			testServer := testHandlerServer(DataSetStatusHandler)
			defer testServer.Close()

			ConfigAPIClient = newTestConfigAPIClient(ClientError(fmt.Errorf("Unable to connect to host")))

			response, err := http.Get(testServer.URL)
			Expect(err).To(BeNil())
			Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))

			Expect(response).To(EqualAPIResponse(APIResponse{Status: "error",
				Message: "Unable to connect to host",
				Errors:  []ErrorInfo{ErrorInfo{Detail: "Unable to connect to host"}}}))
		})

		It("responds with a status of OK when there are no stale datasets", func() {
			testServer := testHandlerServer(DataSetStatusHandler)
			defer testServer.Close()

			ConfigAPIClient = newTestConfigAPIClient(
				DataSets(
					config.DataSetMetaData{},
					config.DataSetMetaData{}))

			response, err := http.Get(testServer.URL)
			Expect(err).To(BeNil())
			Expect(response.StatusCode).To(Equal(http.StatusOK))

			Expect(response).To(EqualAPIResponse(APIResponse{Status: "ok"}))
		})

		It("responds with a status of ruh roh when there is a stale dataset", func() {
			testServer := testHandlerServer(DataSetStatusHandler)
			defer testServer.Close()

			maxExpectedAge := int64(8400)
			stale := config.DataSetMetaData{
				Name:           "the-stale-one",
				Published:      true,
				MaxExpectedAge: &maxExpectedAge}

			ConfigAPIClient = newTestConfigAPIClient(
				DataSets(
					config.DataSetMetaData{},
					stale))

			response, err := http.Get(testServer.URL)
			Expect(err).To(BeNil())
			Expect(response.StatusCode).To(Equal(http.StatusOK))

			Expect(response).To(EqualAPIResponse(APIResponse{Status: "not okay",
				Message: "1 data-set is out of date",
				Errors: []ErrorInfo{
					ErrorInfo{
						Detail: "name: the-stale-one, seconds-out-of-date: 2592000, last-updated: " + roughly30DaysAgo.String() + ", max-age-expected: 8400"}}}))
		})

		It("responds with a status of ruh roh when there are stale datasets", func() {
			testServer := testHandlerServer(DataSetStatusHandler)
			defer testServer.Close()

			maxExpectedAge := int64(8400)
			stale1 := config.DataSetMetaData{
				Name:           "the-stale-one",
				Published:      true,
				MaxExpectedAge: &maxExpectedAge}
			stale2 := config.DataSetMetaData{
				Name:           "the-other-stale-one",
				Published:      true,
				MaxExpectedAge: &maxExpectedAge}

			ConfigAPIClient = newTestConfigAPIClient(
				DataSets(
					config.DataSetMetaData{},
					stale1,
					stale2))

			response, err := http.Get(testServer.URL)
			Expect(err).To(BeNil())
			Expect(response.StatusCode).To(Equal(http.StatusOK))

			Expect(response).To(EqualAPIResponse(APIResponse{Status: "not okay",
				Message: "2 data-sets are out of date",
				Errors: []ErrorInfo{
					ErrorInfo{
						Detail: "name: the-stale-one, seconds-out-of-date: 2592000, last-updated: " + roughly30DaysAgo.String() + ", max-age-expected: 8400"},
					ErrorInfo{
						Detail: "name: the-other-stale-one, seconds-out-of-date: 2592000, last-updated: " + roughly30DaysAgo.String() + ", max-age-expected: 8400"}}}))
		})
	})

	Describe("Creating data", func() {
		var testServer *httptest.Server
		var client *http.Client

		BeforeEach(func() {
			handler := newHandler(10000000)
			testServer = testHandlerServer(handler)
			client = &http.Client{}
		})

		AfterEach(func() {
			defer testServer.Close()
		})

		Context("When there is no valid Authorization credentials", func() {
			It("Should fail with an Authorization required response when there is no Authorization header", func() {
				ConfigAPIClient = newTestConfigAPIClient(
					MetaData(&config.DataSetMetaData{Name: "the-dataset"}))
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type", nil)

				response, err := client.Do(req)

				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))

				Expect(response).To(EqualAPIResponse(newErrorAPIResponse("Expected header of form: Authorization: Bearer token")))
			})

			It("Should fail with an Authorization required response when the Authorization header isn't a valid bearer token", func() {
				ConfigAPIClient = newTestConfigAPIClient(
					MetaData(&config.DataSetMetaData{Name: "the-dataset"}))
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Not a bearer token")

				response, err := client.Do(req)

				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))

				Expect(response).To(EqualAPIResponse(newErrorAPIResponse("Unauthorized: Invalid bearer token '' for 'the-dataset'")))
			})

			It("Should fail with an Authorization required response when the Authorization bearer token does not match the data set bearer token", func() {
				ConfigAPIClient = newTestConfigAPIClient(
					MetaData(
						&config.DataSetMetaData{Name: "the-dataset"}))
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Not a bearer token")

				response, err := client.Do(req)

				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))

				Expect(response).To(EqualAPIResponse(newErrorAPIResponse("Unauthorized: Invalid bearer token '' for 'the-dataset'")))
			})

			It("Should fail with an Authorization required response when the Authorization bearer token does not match the data set bearer token", func() {
				ConfigAPIClient = newTestConfigAPIClient(
					MetaData(&config.DataSetMetaData{BearerToken: "the-bearer-token", Name: "the-dataset"}))
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Not a bearer token")

				response, err := client.Do(req)

				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))

				Expect(response).To(EqualAPIResponse(newErrorAPIResponse("Unauthorized: Invalid bearer token '' for 'the-dataset'")))
			})
		})

		Context("When there are valid Authorization credentials", func() {
			BeforeEach(func() {
				ConfigAPIClient = newTestConfigAPIClient(
					MetaData(&config.DataSetMetaData{BearerToken: "the-bearer-token", Name: "the-dataset"}))
				DataSetStorage = newTestDataSetStorage(Alive(true), Exists(true))
			})

			It("Should need a request body", func() {
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Bearer the-bearer-token")

				response, err := client.Do(req)

				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusBadRequest))

				Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("Expected JSON request body but received 0 bytes")))
			})

			It("Should need a JSON request body", func() {
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type",
					strings.NewReader("this is not JSON"))
				req.Header.Add("Authorization", "Bearer the-bearer-token")

				response, err := client.Do(req)

				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusBadRequest))

				Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("Error parsing JSON: invalid character 'h' in literal true (expecting 'r')")))
			})

			It("Should persist the update for a single object", func() {
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type",
					strings.NewReader(`{"animal":"parrot", "status":"pining"}`))
				req.Header.Add("Authorization", "Bearer the-bearer-token")

				response, err := client.Do(req)

				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusOK))

				Expect(response).Should(EqualAPIResponse(APIResponse{Status: "ok"}))
			})

			It("Should persist the update for an array of objects", func() {
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type",
					strings.NewReader(`[
	{"animal":"parrot", "status":"pining"},
	{"animal":"fish", "status":"slapping"}
]`))
				req.Header.Add("Authorization", "Bearer the-bearer-token")

				response, err := client.Do(req)

				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusOK))

				Expect(response).Should(EqualAPIResponse(APIResponse{Status: "ok"}))
			})

			It("Should fail when provided with invalid data", func() {
				req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type",
					strings.NewReader(`{"_animal":"parrot", "status":"pining"}`))
				req.Header.Add("Authorization", "Bearer the-bearer-token")

				response, err := client.Do(req)

				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusBadRequest))

				Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("_animal is not a recognised internal field")))
			})

			Context("With unavailable storage", func() {
				It("Should propagate failure to persist the updates", func() {
					DataSetStorage = newTestDataSetStorage(Alive(true), Exists(true), SomeError(fmt.Errorf("Mongo connection is down")))
					StatsdClient = newTestStatsdClient()

					req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type",
						strings.NewReader(`{"animal":"parrot", "status":"pining"}`))
					req.Header.Add("Authorization", "Bearer the-bearer-token")

					response, err := client.Do(req)

					Expect(err).Should(BeNil())
					Expect(response.StatusCode).Should(Equal(http.StatusInternalServerError))

					Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("Mongo connection is down")))

					// Check that the dataset was picked out of the context and the correct thing
					// would have been sent to statsd
					testStatsd := StatsdClient.(*testStatsdClient)
					Expect(testStatsd.incOps).Should(HaveLen(1))
					Expect(testStatsd.incOps[0].stat).Should(Equal(`write.error.the-dataset`))
				})
			})

			Context("With compressed requests", func() {
				It("Should fail if the request does not have a Content-Encoding header", func() {
					var b bytes.Buffer
					w := gzip.NewWriter(&b)
					w.Write([]byte(`{"animal":"parrot", "status":"pining"}`))
					w.Close()
					req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type",
						bytes.NewReader(b.Bytes()))
					req.Header.Add("Authorization", "Bearer the-bearer-token")

					response, err := client.Do(req)

					Expect(err).Should(BeNil())
					Expect(response.StatusCode).Should(Equal(http.StatusBadRequest))

					Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("Error parsing JSON: invalid character '\\x1f' looking for beginning of value")))
				})

				It("Should fail if the request does not have a body", func() {
					req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type", nil)
					req.Header.Add("Authorization", "Bearer the-bearer-token")

					response, err := client.Do(req)

					Expect(err).Should(BeNil())
					Expect(response.StatusCode).Should(Equal(http.StatusBadRequest))

					Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("Expected JSON request body but received 0 bytes")))
				})

				It("Should succeed if the request has a Content-Encoding header", func() {
					var b bytes.Buffer
					w := gzip.NewWriter(&b)
					w.Write([]byte(`{"animal":"parrot", "status":"pining"}`))
					w.Close()
					req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type",
						bytes.NewReader(b.Bytes()))
					req.Header.Add("Authorization", "Bearer the-bearer-token")
					req.Header.Add("Content-Encoding", "gzip")

					response, err := client.Do(req)

					Expect(err).Should(BeNil())
					Expect(response.StatusCode).Should(Equal(http.StatusOK))

					Expect(response).Should(EqualAPIResponse(APIResponse{
						Status: "ok"}))
				})

				It("Should fail if the request is too big", func() {
					testServer.Close()
					handler := newHandler(10)
					testServer = testHandlerServer(handler)

					var b bytes.Buffer
					w := gzip.NewWriter(&b)
					w.Write([]byte(`{"animal":"parrot", "status":"pining"}`))
					w.Close()
					req, err := http.NewRequest("POST", testServer.URL+"/data/a-data-group/a-data-type",
						bytes.NewReader(b.Bytes()))
					req.Header.Add("Authorization", "Bearer the-bearer-token")
					req.Header.Add("Content-Encoding", "gzip")

					response, err := client.Do(req)

					Expect(err).Should(BeNil())
					Expect(response.StatusCode).Should(Equal(http.StatusRequestEntityTooLarge))

					Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("Maximum upload size encountered. Treating as a potential zip bomb.")))
				})
			})
		})
	})

	Describe("Updating data", func() {
		var testServer *httptest.Server
		var client *http.Client
		BeforeEach(func() {
			handler := newHandler(10000000)
			testServer = testHandlerServer(handler)
			client = &http.Client{}
		})

		AfterEach(func() {
			defer testServer.Close()
		})

		Context("When there is no valid Authorization credentials", func() {
			It("Should fail with an Authorization required response when there is no Authorization header", func() {
				ConfigAPIClient = newTestConfigAPIClient(
					MetaData(&config.DataSetMetaData{}))
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type", nil)

				response, err := client.Do(req)

				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))

				Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("Expected header of form: Authorization: Bearer token")))
			})

			It("Should fail with an Authorization required response when the Authorization header isn't a valid bearer token", func() {
				ConfigAPIClient = newTestConfigAPIClient(
					MetaData(&config.DataSetMetaData{Name: "the-dataset"}))
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Not a bearer token")

				response, err := client.Do(req)

				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))

				Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("Unauthorized: Invalid bearer token '' for 'the-dataset'")))
			})

			It("Should fail with an Authorization required response when the Authorization bearer token does not match the data set bearer token", func() {
				ConfigAPIClient = newTestConfigAPIClient(
					MetaData(&config.DataSetMetaData{Name: "the-dataset"}))
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Not a bearer token")

				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))

				Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("Unauthorized: Invalid bearer token '' for 'the-dataset'")))
			})

			It("Should fail with an Authorization required response when the Authorization bearer token does not match the data set bearer token", func() {
				ConfigAPIClient = newTestConfigAPIClient(
					MetaData(&config.DataSetMetaData{BearerToken: "the-bearer-token", Name: "the-dataset"}))
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Not a bearer token")

				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusUnauthorized))
				Expect(response.Header.Get("WWW-Authenticate")).Should(Equal("bearer"))

				Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("Unauthorized: Invalid bearer token '' for 'the-dataset'")))
			})
		})

		Context("When there are valid Authorization credentials", func() {
			BeforeEach(func() {
				ConfigAPIClient = newTestConfigAPIClient(
					MetaData(
						&config.DataSetMetaData{
							BearerToken: "the-bearer-token",
							Name:        "the-dataset"}))
				DataSetStorage = newTestDataSetStorage(Alive(true), Exists(true))
			})

			It("Should need a request body", func() {
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type", nil)
				req.Header.Add("Authorization", "Bearer the-bearer-token")

				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusBadRequest))

				Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("Expected JSON request body but received 0 bytes")))
			})

			It("Should need a JSON request body", func() {
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type",
					strings.NewReader("this is not JSON"))
				req.Header.Add("Authorization", "Bearer the-bearer-token")

				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusBadRequest))

				Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("Error parsing JSON: invalid character 'h' in literal true (expecting 'r')")))
			})

			It("Should persist the update emptying the data set", func() {
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type",
					strings.NewReader(`[]`))
				req.Header.Add("Authorization", "Bearer the-bearer-token")

				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusOK))

				Expect(response).Should(EqualAPIResponse(APIResponse{
					Status:  "ok",
					Message: "the-dataset now contains 0 records"}))
			})

			It("Should fail to update if the array isn't empty", func() {
				req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type",
					strings.NewReader(`[
	{"animal":"parrot", "status":"pining"}
]`))
				req.Header.Add("Authorization", "Bearer the-bearer-token")

				response, err := client.Do(req)
				Expect(err).Should(BeNil())
				Expect(response.StatusCode).Should(Equal(http.StatusBadRequest))

				Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("Not implemented: you can only pass an empty JSON list")))
			})

			Context("With unavailable storage", func() {
				It("Should propagate the error if there is a problem emptying the data set", func() {
					DataSetStorage = newTestDataSetStorage(Alive(true), Exists(true), SomeError(fmt.Errorf("Mongo connection is down")))

					req, err := http.NewRequest("PUT", testServer.URL+"/data/a-data-group/a-data-type",
						strings.NewReader(`[]`))
					req.Header.Add("Authorization", "Bearer the-bearer-token")

					response, err := client.Do(req)
					Expect(err).Should(BeNil())
					Expect(response.StatusCode).Should(Equal(http.StatusInternalServerError))

					Expect(response).Should(EqualAPIResponse(newErrorAPIResponse("Mongo connection is down")))
				})
			})

		})
	})
})

// APIResponseMatcher implements gomega.types.GomegaMatcher
type APIResponseMatcher struct {
	expected   APIResponse
	actualBody string
}

// EqualAPIResponse is a GomegaMatcher to look at the response from API calls
func EqualAPIResponse(expected APIResponse) types.GomegaMatcher {
	return &APIResponseMatcher{expected: expected}
}

func (matcher *APIResponseMatcher) Match(actual interface{}) (success bool, err error) {
	response, ok := actual.(*http.Response)
	if !ok {
		return false, fmt.Errorf("EqualAPIResponse matcher expects an http.Response")
	}

	var r APIResponse
	matcher.actualBody, err = readResponseBody(response)

	if err != nil {
		return false, fmt.Errorf("Failed to read response: %s", err.Error())
	}

	err = json.NewDecoder(strings.NewReader(matcher.actualBody)).Decode(&r)

	if err != nil {
		return false, fmt.Errorf("Failed to decode JSON: %s", err.Error())
	}

	return reflect.DeepEqual(r, matcher.expected), nil
}

func (matcher *APIResponseMatcher) FailureMessage(actual interface{}) string {
	return fmt.Sprintf("Expected\n\t%#v\nto contain the JSON representation of\n\t%#v", matcher.actualBody, matcher.expected)
}

func (matcher *APIResponseMatcher) NegatedFailureMessage(actual interface{}) string {
	return fmt.Sprintf("Expected\n\t%#v\nnot to contain the JSON representation of\n\t%#v", matcher.actualBody, matcher.expected)
}
