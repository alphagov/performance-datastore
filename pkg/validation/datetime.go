package validation

import (
	"fmt"
	"regexp"
)

type dateTime struct {
	name string
}

var (
	timePattern = regexp.MustCompile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}")
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

	for _, v := range values {
		if !isValidDateTime(v) {
			return fmt.Errorf("%s is not a valid datetime", x.name)
		}
	}

	return nil
}

func isValidDateTime(candidate string) bool {
	return timePattern.MatchString(candidate)
}
