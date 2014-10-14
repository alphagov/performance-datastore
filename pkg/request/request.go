package request

import (
	"errors"
	// "fmt"
	// "github.com/cenkalti/backoff"
	"io/ioutil"
	"net/http"
	// "time"
)

var (
	NotFoundError error = errors.New("not found")
)

func NewRequest(url, bearerToken string) (*http.Response, error) {
	client := http.Client{}

	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	request.Header.Add("Authorization", "Bearer "+bearerToken)
	request.Header.Add("Accept", "application/json")
	request.Header.Add("User-Agent", "Performance-Platform-Client/1.0")

	response, err := tryGet(client, request)

	if err != nil {
		return nil, err
	}

	if response.StatusCode == http.StatusNotFound {
		return nil, NotFoundError
	}

	return response, err
}

func ReadResponseBody(response *http.Response) ([]byte, error) {
	defer response.Body.Close()
	return ioutil.ReadAll(response.Body)
}

func tryGet(client http.Client, req *http.Request) (res *http.Response, err error) {
	// operation := func() error {
	res, httpErr := client.Do(req)
	// 	if httpErr != nil {
	// 		return httpErr
	// 	}
	// 	switch res.StatusCode {
	// 	case 502, 503:
	// 		return fmt.Errorf("Server unavailable")
	// 	}
	// 	return nil
	// }

	// expo := backoff.NewExponentialBackOff()
	// expo.MaxElapsedTime = (5 * time.Second)
	// err = backoff.Retry(operation, expo)
	// if err != nil {
	// 	// Operation has failed.
	// 	return nil, err
	// }

	return res, httpErr
}
