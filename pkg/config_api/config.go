package config_api

import (
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/jabley/performance-datastore/pkg/json_response"
	"net/http"
	"time"
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

	res, err = tryGet(req)

	if err != nil {
		return nil, err
	}

	return res, nil
}

func tryGet(req *http.Request) (res *http.Response, err error) {
	operation := func() error {
		res, httpErr := client.Do(req)
		if httpErr != nil {
			return httpErr
		}
		switch res.StatusCode {
		case 502, 503:
			return fmt.Errorf("Server unavailable")
		}
		return nil
	}

	expo := backoff.NewExponentialBackOff()
	expo.MaxElapsedTime = (5 * time.Second)
	err = backoff.Retry(operation, expo)
	if err != nil {
		// Operation has failed.
		return nil, err
	}

	return
}

func getJSONArray(path string) ([]interface{}, error) {
	res, err := get(path)

	if err != nil {
		return nil, err
	}

	return json_response.ParseArray(res.Body)
}

func getJSONObject(path string) (map[string]interface{}, error) {
	res, err := get(path)

	if err != nil {
		return nil, err
	}

	return json_response.ParseObject(res.Body)
}
