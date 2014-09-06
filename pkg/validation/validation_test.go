package validation

import (
	"testing"
	"time"
)

func TestDateTimeString(t *testing.T) {
	if !isValidDateTime("2012-06-03T13:26:00") {
		t.Errorf("WAT!")
	}
}

func TestBadlyFormattedStartAtFails(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"i am not a time"}
	err := ValidateRequestArgs(args, false)
	if err == nil {
		t.Errorf("%v should have failed", args)
	}
}

func TestWellFormattedStartAtIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"2000-02-02T00:02:02 +00:00"}
	err := ValidateRequestArgs(args, false)
	if err != nil {
		t.Errorf("%v should have been okay", args)
	}
}

func TestMultipleStartAtArgsFail(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"2000-02-02T00:02:02 +00:00", "2000-03-02T00:02:02 +00:00"}
	err := ValidateRequestArgs(args, false)
	if err == nil {
		t.Errorf("%v should have failed", args)
	}
}

func TestDaftStartDateFails(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"2000-14-28T00:02:02 +00:00"}
	err := ValidateRequestArgs(args, false)
	if err == nil {
		t.Errorf("%v should have failed", args)
	}
}

func TestDateParsing(t *testing.T) {
	v, err := time.Parse(time.RFC3339, "2000-14-28T00:02:02 +00:00")

	if err == nil {
		t.Errorf("time was parsed as %v", v)
	}
}

func TestBadlyFormattedEndAtFails(t *testing.T) {
	args := make(map[string][]string)
	args["end_at"] = []string{"i am not a time"}
	err := ValidateRequestArgs(args, false)
	if err == nil {
		t.Errorf("%v should have failed", args)
	}
}

func TestWellFormattedEndAtIsAllowed(t *testing.T) {
	args := make(map[string][]string)
	args["end_at"] = []string{"2000-02-02T00:02:02 +00:00"}
	err := ValidateRequestArgs(args, false)
	if err != nil {
		t.Errorf("%v should have failed", args)
	}
}
