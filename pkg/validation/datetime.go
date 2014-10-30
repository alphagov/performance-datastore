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

// NewDateTimeValidator validates that the specified named field can be treated as a valid time.Time.
func NewDateTimeValidator(name string) Validator {
	return &dateTimeValidator{
		name: name,
	}
}

func (x *dateTimeValidator) Validate(args map[string][]string) (res interface{}, err error) {
	values, ok := args[x.name]

	if !ok {
		return
	}

	if len(values) > 1 {
		return nil, fmt.Errorf("%s is not a valid datetime", x.name)
	}

	if res = ParseDateTime(values[0]); res == nil {
		return nil, fmt.Errorf("%s is not a valid datetime", x.name)
	}

	return
}

func isValidDateTime(candidate string) bool {
	return ParseDateTime(candidate) != nil
}

// ParseDateTime checks that returns a *time.Time representation of candidate, or nil if not possible.
// The supported formats are defined in validLayouts.
func ParseDateTime(candidate interface{}) *time.Time {
	res, isTime := candidate.(time.Time)

	if isTime {
		return &res
	}

	str, isString := candidate.(string)

	if !isString {
		return nil
	}

	for _, layout := range validLayouts {
		dt, err := time.Parse(layout, str)
		if err == nil {
			return &dt
		}
	}

	return nil
}

type midnightValidator struct {
	name string
}

// NewMidnightValidator validates that we have period that isn't hour, and the relevant date is midnight UTC.
func NewMidnightValidator(name string) Validator {
	return &midnightValidator{name: name}
}

func (x *midnightValidator) Validate(args map[string][]string) (res interface{}, err error) {
	values, ok := args[x.name]

	if !ok {
		return
	}

	if len(values) > 1 {
		return nil, fmt.Errorf("%s is not a valid datetime", x.name)
	}

	period, periodErr := NewPeriodValidator().Validate(args)

	if theDate := ParseDateTime(values[0]); theDate != nil &&
		periodErr == nil &&
		(period != nil && period != "hour") {

		if !isMidnight(theDate.UTC()) {
			return nil, fmt.Errorf("%s must be midnight", x.name)
		}
	}

	return
}

func isMidnight(t time.Time) bool {
	hour, min, sec := t.Clock()
	return (hour == 0 && min == 0 && sec == 0)
}

type timespanValidator struct {
	length int
}

// NewTimespanValidator validates that we have a start_at, end_at and a period that isn't hour.
func NewTimespanValidator(length int) Validator {
	return &timespanValidator{length: length}
}

func (x *timespanValidator) Validate(args map[string][]string) (res interface{}, err error) {
	startAt, _ := NewDateTimeValidator("start_at").Validate(args)
	endAt, _ := NewDateTimeValidator("end_at").Validate(args)
	period, _ := NewPeriodValidator().Validate(args)

	if startAt != nil && endAt != nil && (period != nil && period != "hour") {
		hours := endAt.(*time.Time).UTC().Sub(startAt.(*time.Time).UTC()).Hours()
		if hours < float64(24*7) {
			return nil, fmt.Errorf("The minimum timespan for a query is %v days", x.length)
		}
		res = hours / 24
	}

	return
}

type mondayValidator struct {
	name string
}

// NewMondayValidator returns a Validator implementation which checks that week periods start on a Monday
func NewMondayValidator(name string) Validator {
	return &mondayValidator{name: name}
}

func (x *mondayValidator) Validate(args map[string][]string) (res interface{}, err error) {
	date, _ := NewDateTimeValidator(x.name).Validate(args)
	period, _ := NewPeriodValidator().Validate(args)

	if (period != nil && period == "week") &&
		date != nil &&
		date.(*time.Time).UTC().Weekday() != time.Monday {
		return nil, fmt.Errorf("%v must be a Monday but was %v", x.name, date)
	}

	return
}

type monthValidator struct {
	name string
}

// NewMonthValidator returns a Validator implementation month periods start on the first
func NewMonthValidator(name string) Validator {
	return &monthValidator{name: name}
}

func (x *monthValidator) Validate(args map[string][]string) (res interface{}, err error) {
	date, _ := NewDateTimeValidator(x.name).Validate(args)
	period, _ := NewPeriodValidator().Validate(args)

	if (period != nil && period == "month") &&
		date != nil &&
		date.(*time.Time).UTC().Day() != 1 {
		return nil, fmt.Errorf("%v must be a first of the month but was %v", x.name, date)
	}

	return
}
