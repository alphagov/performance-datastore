package json_response

import (
	"io/ioutil"
	"strings"
	"testing"
)

func TestParsingArray(t *testing.T) {
	jsonText := `
  [
    {"Name": "Ed", "Text": "Knock knock."},
    {"Name": "Sam", "Text": "Who's there?"},
    {"Name": "Ed", "Text": "Go fmt."},
    {"Name": "Sam", "Text": "Go fmt who?"},
    {"Name": "Ed", "Text": "Go fmt yourself!"}
  ]
`
	arr, err := ParseArray(ioutil.NopCloser(strings.NewReader(jsonText)))

	if err != nil {
		t.Error(err)
	}

	assertIntEqual(t, 5, len(arr), "should have 5 elements")
}

func TestParsingObjectAsArray(t *testing.T) {
	jsonText := `
      {"Name": "Ed", "Text": "Knock knock."},
`
	_, err := ParseArray(ioutil.NopCloser(strings.NewReader(jsonText)))

	if err == nil {
		t.Error("Should have failed trying to parse an object as an array")
	}
}

func TestParsingObject(t *testing.T) {
	jsonText := `
      {"Name": "Ed", "Text": "Knock knock."},
`
	obj, err := ParseObject(ioutil.NopCloser(strings.NewReader(jsonText)))
	if err != nil {
		t.Error(err)
	}

	assertStringEqual(t, "Ed", obj["Name"].(string))
}

func TestParsingArrayAsObject(t *testing.T) {
	jsonText := `
  [
    {"Name": "Ed", "Text": "Knock knock."},
    {"Name": "Sam", "Text": "Who's there?"},
    {"Name": "Ed", "Text": "Go fmt."},
    {"Name": "Sam", "Text": "Go fmt who?"},
    {"Name": "Ed", "Text": "Go fmt yourself!"}
  ]
`
	_, err := ParseObject(ioutil.NopCloser(strings.NewReader(jsonText)))

	if err == nil {
		t.Error("Should have failed trying to parse an array as an object")
	}
}

func assertIntEqual(t *testing.T, expected int, actual int, message ...interface{}) {
	if expected != actual {
		t.Error("expected:", expected, "\nactual:", actual, "\n", message)
	}
}

func assertStringEqual(t *testing.T, expected string, actual string, message ...interface{}) {
	if expected != actual {
		t.Error("expected:", expected, "\nactual:", actual, "\n", message)
	}
}
