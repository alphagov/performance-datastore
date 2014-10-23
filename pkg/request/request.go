package request

import (
	"errors"
	"fmt"
	"github.com/cenkalti/backoff"
	"io/ioutil"
	"net/http"
	"time"
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
	// Use a channel to communicate between the goroutines. We use a channel rather
	// than simple variable closure since that's how Go works :)
	c := make(chan *http.Response, 1)

	operation := func() error {
		response, httpError := client.Do(req)
		if httpError != nil {
			return httpError
		}
		switch response.StatusCode {
		case 502, 503:
			// Oh dear, we'll retry that one
			return fmt.Errorf("Server unavailable")
		}

		// We're good, keep the returned response
		c <- response
		return nil
	}

	expo := backoff.NewExponentialBackOff()
	expo.MaxElapsedTime = (5 * time.Second)
	err = backoff.Retry(operation, expo)

	if err != nil {
		// Operation has failed, repeatedly got a problem or server unavailable
		return nil, err
	}

	// Got a good response, take it out of the channel
	res = <-c

	return res, err
}
