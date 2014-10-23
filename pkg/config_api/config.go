package config_api

import (
	"encoding/json"
	"github.com/jabley/performance-datastore/pkg/request"
)

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

type Client interface {
	DataSet(name string) (*DataSetMetaData, error)
	DataType(group string, dataType string) (*DataSetMetaData, error)
	ListDataSets() ([]DataSetMetaData, error)
}

type defaultClient struct {
	baseURL     string
	bearerToken string
}

func NewClient(baseURL string, bearerToken string) Client {
	return &defaultClient{baseURL, bearerToken}
}

func (c *defaultClient) DataSet(name string) (*DataSetMetaData, error) {
	res, err := c.get("/data-sets/" + name)

	if err != nil {
		return nil, err
	}

	d := DataSetMetaData{}

	err = json.Unmarshal(res, &d)

	if err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *defaultClient) DataType(group string, dataType string) (*DataSetMetaData, error) {
	res, err := c.get("/data-sets?data-group=" + group + "&data-type=" + dataType)

	if err != nil {
		return nil, err
	}

	d := []DataSetMetaData{}

	err = json.Unmarshal(res, &d)

	if err != nil {
		return nil, err
	}

	return &d[0], nil
}

func (c *defaultClient) ListDataSets() ([]DataSetMetaData, error) {
	res, err := c.get("/data-sets")

	if err != nil {
		return nil, err
	}

	d := []DataSetMetaData{}

	err = json.Unmarshal(res, &d)

	if err != nil {
		return nil, err
	}

	return d, nil
}

func (c *defaultClient) get(path string) (body []byte, err error) {
	response, err := request.NewRequest(c.baseURL+path, c.bearerToken)

	if err != nil {
		return nil, err
	}

	body, err = request.ReadResponseBody(response)

	if err != nil {
		return nil, err
	}

	return body, nil
}
