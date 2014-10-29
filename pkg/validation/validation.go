package validation

import (
	"regexp"
	"strings"
	"time"
)

// Validator defines a simple function for validating string arguments.
// Implementations MAY choose to return the validated value, and SHOULD
// return an error if there was a problem.
type Validator interface {
	Validate(args map[string][]string) (interface{}, error)
}

// ValidateRequestArgs validates all of the string arguments
func ValidateRequestArgs(values map[string][]string, allowRawQueries bool) error {
	validators := []Validator{
		NewDateTimeValidator("start_at"),
		NewDateTimeValidator("end_at"),
		NewFilterByValidator(),
		NewSortByValidator(),
		NewPositiveIntegerValidator("limit"),
		NewGroupByValidator(),
		NewCollectValidator(),
		NewDurationValidator(),
		NewPositiveIntegerValidator("duration"),
		NewPeriodValidator(),
	}

	if !allowRawQueries {
		validators = append(validators, NewMidnightValidator("start_at"))
		validators = append(validators, NewMidnightValidator("end_at"))
		validators = append(validators, NewTimespanValidator(7))
		validators = append(validators, NewMondayValidator("start_at"))
		validators = append(validators, NewMondayValidator("end_at"))
		validators = append(validators, NewMonthValidator("start_at"))
		validators = append(validators, NewMonthValidator("end_at"))
	}

	for _, v := range validators {
		if _, err := v.Validate(values); err != nil {
			return err
		}
	}

	return nil
}

var (
	validKey = regexp.MustCompile(`^[a-z_][a-z0-9_]+$`)
)

// IsValidKey returns true if the string is a valid key, otherwise false.
func IsValidKey(key string) bool {
	return validKey.MatchString(strings.ToLower(key))
}

// IsInternalKey returns true if the string looks like an internal key, otherwise false.
func IsInternalKey(key string) bool {
	return strings.HasPrefix(key, "_")
}

// IsReservedKey returns true if this is a key reserved for our API implementation, otherwise false.
func IsReservedKey(key string) bool {
	switch key {
	case "_id", "_timestamp":
		return true
	default:
		return false
	}
}

// IsValidID returns true if this looks like a valid ID, otherwise false.
func IsValidID(v interface{}) bool {
	switch v.(type) {
	case string:
		{
			s := v.(string)
			hasSpace, err := regexp.MatchString(`\s`, s)
			return len(s) > 0 && (err == nil && !hasSpace)
		}
	default:
		return false
	}
}

// IsValidValue returns true if the value is one that we handle and store, otherwise false.
func IsValidValue(v interface{}) bool {
	switch v.(type) {
	case int64, float64, string, time.Time:
		{
			return true
		}
	default:
		return v == nil
	}
}
