package validation

import (
	"testing"
)

func TestDateTimeString(t *testing.T) {
	if !isValidDateTime("2012-06-03T13:26:00") {
		t.Error("WAT!")
	}
}

func TestBadlyFormattedStartAtIsDisAllowed(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"i am not a time"}
	err := ValidateRequestArgs(args, false)
	if err == nil {
		t.Error("Should have failed")
	}
}
