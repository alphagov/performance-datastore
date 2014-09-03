package json_response

import (
	"encoding/json"
	"fmt"
	"io"
)

func ParseObject(body io.ReadCloser) (map[string]interface{}, error) {
	var v map[string]interface{}
	result, err := parseJSON(body, v)
	if err != nil {
		return nil, err
	}
	r, ok := result.(map[string]interface{})

	if !ok {
		return nil, fmt.Errorf("Unable to convert to object")
	}
	return r, nil
}

func ParseArray(body io.ReadCloser) ([]interface{}, error) {
	var v []interface{}
	result, err := parseJSON(body, v)
	if err != nil {
		return nil, err
	}
	r, ok := result.(([]interface{}))

	if !ok {
		return nil, fmt.Errorf("Unable to convert to array")
	}
	return r, nil
}

func parseJSON(body io.ReadCloser, v interface{}) (interface{}, error) {
	defer body.Close()
	dec := json.NewDecoder(body)
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}
