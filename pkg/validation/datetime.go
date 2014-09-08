package validation

import (
	"fmt"
	"time"
)

type dateTimeValidator struct {
	name string
}

var (
	validLayouts = []string{
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05 -07:00",
	}
)

func NewDateTimeValidator(name string) Validator {
	return &dateTimeValidator{
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
