package config

import (
	"encoding/json"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/alphagov/performance-datastore/pkg/request"
	"reflect"
)

// DataSetMetaData defines the data structure returned by our meta data API
// A future restructure will probably move this into our DataSetStorage interface
// in the dataset package, since it feels like bad data locality.
type DataSetMetaData struct {
	Name            string          `json:"name"`
	DataGroup       string          `json:"data_group"`
	DataType        string          `json:"data_type"`
	AllowRawQueries bool            `json:"raw_queries_allowed"`
	BearerToken     string          `json:"bearer_token"`
	UploadFormat    string          `json:"upload_format"`
	UploadFilters   []string        `json:"upload_filters"`
	AutoIds         []string        `json:"auto_ids"`
	Queryable       bool            `json:"queryable"`
	Realtime        bool            `json:"realtime"`
	CappedSize      int64           `json:"capped_size"`
	MaxExpectedAge  *int64          `json:"max_age_expected"`
	Published       bool            `json:"published"`
	Schema          json.RawMessage `json:"schema"`
}

// Client defines the interface that we need to talk to the meta data API
type Client interface {
	DataSet(name string) (*DataSetMetaData, error)
	DataType(group string, dataType string) (*DataSetMetaData, error)
	ListDataSets() ([]DataSetMetaData, error)
}

type defaultClient struct {
	baseURL     string
	bearerToken string
	logger      *logrus.Logger
}

// NewClient returns a new Client implementation with sensible defaults.
func NewClient(baseURL string, bearerToken string, logger *logrus.Logger) Client {
	return &defaultClient{baseURL, bearerToken, logger}
}

func (c *defaultClient) DataSet(name string) (*DataSetMetaData, error) {
	var holder DataSetMetaData
	err := c.fetch("/data-sets/"+name, &holder)

	if err != nil {
		return nil, err
	}

	return &holder, nil
}

func (c *defaultClient) DataType(group string, dataType string) (result *DataSetMetaData, err error) {
	var holder []DataSetMetaData
	err = c.fetch("/data-sets?data-group="+group+"&data-type="+dataType, &holder)

	if err != nil {
		return nil, err
	}

	return &holder[0], nil
}

func (c *defaultClient) ListDataSets() (result []DataSetMetaData, err error) {
	var holder []DataSetMetaData
	err = c.fetch("/data-sets", &holder)

	if err != nil {
		return nil, err
	}

	return holder, nil
}

func (c *defaultClient) fetch(url string, result interface{}) error {
	res, err := c.get(url)

	if err != nil {
		var message string
		if res != nil {
			message = string(res)
		}
		c.logger.Errorf("%v %v", err, message)
		return err
	}

	switch kind := reflect.TypeOf(result).Kind(); kind {
	case reflect.Ptr:
	default:
		return fmt.Errorf("parameter result should be a pointer, but is %v", kind)
	}

	err = json.Unmarshal(res, result)

	if err != nil {
		return err
	}

	return nil
}

func (c *defaultClient) get(path string) (body []byte, err error) {
	response, requestErr := request.NewRequest(c.baseURL+path, c.bearerToken)
	body, readErr := request.ReadResponseBody(response)

	if requestErr != nil {
		// Can we potentially return a JSON error document?
		if readErr == nil {
			return body, requestErr
		}
		return nil, requestErr
	}

	if readErr != nil {
		return nil, readErr
	}

	return body, nil
}
