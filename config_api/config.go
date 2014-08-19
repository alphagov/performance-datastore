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
	defer res.Body.Close()
	dec := json.NewDecoder(res.Body)
	var v map[string]interface{}
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

func parseArray(res *http.Response) ([]interface{}, error) {
	defer res.Body.Close()
	dec := json.NewDecoder(res.Body)
	var v []interface{}
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}
