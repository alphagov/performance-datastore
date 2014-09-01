package json_response

import (
	"encoding/json"
	"io"
)

func ParseObject(body io.ReadCloser) (map[string]interface{}, error) {
	var v map[string]interface{}
	result, err := ParseJSON(body, v)
	if err != nil {
		return nil, err
	}
	return result.(map[string]interface{}), err
}

func ParseArray(body io.ReadCloser) ([]interface{}, error) {
	var v []interface{}
	result, err := ParseJSON(body, v)
	if err != nil {
		return nil, err
	}
	return result.([]interface{}), err
}

func ParseJSON(body io.ReadCloser, v interface{}) (interface{}, error) {
	defer body.Close()
	dec := json.NewDecoder(body)
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}
