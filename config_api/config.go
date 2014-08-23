package config_api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

var (
	baseURL = "https://stagecraft.dev"
	client  = &http.Client{}
	token   = "A Bearer Token"
	version = "1.0"
)

func DataSet(dataSetName string) (map[string]interface{}, error) {
	return getJSONObject("/data-sets/" + dataSetName)
}

func DataType(dataGroup string, dataType string) (map[string]interface{}, error) {
	res, err := getJSONArray("/data-sets?data-group=" + dataGroup + "&data-type=" + dataType)

	if err != nil {
		return nil, err
	}

	if res != nil && len(res) > 0 {
		return res[0].(map[string]interface{}), nil
	}
	return nil, fmt.Errorf("No such data set")
}

func ListDataSets() ([]interface{}, error) {
	return getJSONArray("/data-sets")
}

func get(path string) (res *http.Response, err error) {
	URL := fmt.Sprintf("%s%s", baseURL, path)
	req, err := http.NewRequest("GET", URL, nil)

	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Add("Accept", "application/json")
	req.Header.Add("User-Agent", fmt.Sprintf("Performance-Platform-Client/%s", version))

	// TODO: Python version currently has exponential backoff with up to 5 tries
	res, err = client.Do(req)

	if err != nil {
		return nil, err
	}

	return res, nil
}

func getJSONArray(path string) ([]interface{}, error) {
	res, err := get(path)

	if err != nil {
		return nil, err
	}

	return parseArray(res)
}

func getJSONObject(path string) (map[string]interface{}, error) {
	res, err := get(path)

	if err != nil {
		return nil, err
	}

	return parseObject(res)
}

func parseObject(res *http.Response) (map[string]interface{}, error) {
	var v map[string]interface{}
	result, err := parseJSON(res, v)
	if err != nil {
		return nil, err
	}
	return result.(map[string]interface{}), err
}

func parseArray(res *http.Response) ([]interface{}, error) {
	var v []interface{}
	result, err := parseJSON(res, v)
	if err != nil {
		return nil, err
	}
	return result.([]interface{}), err
}

func parseJSON(res *http.Response, v interface{}) (interface{}, error) {
	defer res.Body.Close()
	dec := json.NewDecoder(res.Body)
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}
