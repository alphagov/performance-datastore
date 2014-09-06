package validation

import (
	"fmt"
	"time"
)

type dateTime struct {
	name string
}

var (
	// timePattern  = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`)
	validLayouts = []string{
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05 -07:00",
	}
)

type dateTimeValidator dateTime

func NewDateTimeValidator(name string) Validator {
	return (*dateTimeValidator)(newDateTimeValidator(name))
}

func newDateTimeValidator(name string) *dateTime {
	return &dateTime{
		name: name,
	}
}

func (x *dateTimeValidator) Validate(args map[string][]string) error {
	values, ok := args[x.name]

	if !ok {
		return nil
	}

	if len(values) > 1 {
		return fmt.Errorf("%s is not a valid datetime", x.name)
	}

	if !isValidDateTime(values[0]) {
		return fmt.Errorf("%s is not a valid datetime", x.name)
	}

	return nil
}

func isValidDateTime(candidate string) bool {
	for _, layout := range validLayouts {
		_, err := time.Parse(layout, candidate)
		if err == nil {
			return true
		}
	}

	return false
}
