package request

import (
	"errors"
	"fmt"
	"github.com/cenkalti/backoff"
	"io/ioutil"
	"net/http"
	"time"
)

// RequestOptions is the container for tweaking how NewRequest functions.
type RequestOptions struct {
	// MaxElapsedTime is the optional duration allowed to try to get a response from the origin server. Defaults to 5s.
	MaxElapsedTime *time.Duration
}

var (
	// ErrNotFound is an error indicating that the server returned a 404.
	ErrNotFound           = errors.New("not found")
	defaultMaxElapsedTime = (5 * time.Second)
	defaultRequestOptions = RequestOptions{MaxElapsedTime: &defaultMaxElapsedTime}
)

// NewRequest tries to make a request to the URL, returning the http.Response if it was successful, or an error if there was a problem.
// An optional RequestOptions argument can be passed to specify contextual behaviour for this request, otherwise defaultOptions will be used.
func NewRequest(url, bearerToken string, options ...RequestOptions) (*http.Response, error) {
	client := http.Client{}

	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	request.Header.Add("Authorization", "Bearer "+bearerToken)
	request.Header.Add("Accept", "application/json")
	request.Header.Add("User-Agent", "Performance-Platform-Client/1.0")

	opts := mergeOptions(options)
	response, err := tryGet(client, request, opts)

	if err != nil {
		return nil, err
	}

	if response.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	return response, err
}

// ReadResponseBody reads the response body stream and returns a byte array, or an error if there was a problem.
func ReadResponseBody(response *http.Response) ([]byte, error) {
	defer response.Body.Close()
	return ioutil.ReadAll(response.Body)
}

func tryGet(client http.Client, req *http.Request, options RequestOptions) (res *http.Response, err error) {
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
	expo.MaxElapsedTime = *options.MaxElapsedTime

	err = backoff.Retry(operation, expo)

	if err != nil {
		// Operation has failed, repeatedly got a problem or server unavailable
		return nil, err
	}

	// Got a good response, take it out of the channel
	res = <-c

	return res, err
}

func mergeOptions(options []RequestOptions) RequestOptions {
	var opts RequestOptions

	if len(options) > 0 {
		opts = options[0]
		// Effectively merge defaults with explicit options
		if opts.MaxElapsedTime == nil {
			opts.MaxElapsedTime = defaultRequestOptions.MaxElapsedTime
		}
	} else {
		opts = defaultRequestOptions
	}

	return opts
}
