package validation

import (
	"regexp"
	"strings"
	"time"
)

type Validator interface {
	Validate(args map[string][]string) (error, interface{})
}

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
		if err, _ := v.Validate(values); err != nil {
			return err
		}
	}

	return nil
}

var (
	validKey = regexp.MustCompile(`^[a-z_][a-z0-9_]+$`)
)

func IsValidKey(key string) bool {
	return validKey.MatchString(strings.ToLower(key))
}

func IsInternalKey(key string) bool {
	return strings.HasPrefix(key, "_")
}

func IsReservedKey(key string) bool {
	switch key {
	case "_id", "_timestamp":
		return true
	default:
		return false
	}
}

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
