package validation

import (
	"regexp"
	"strings"
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

func isValidKey(key string) bool {
	return validKey.MatchString(strings.ToLower(key))
}
