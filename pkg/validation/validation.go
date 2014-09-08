package validation

import (
	"regexp"
	"strings"
)

type Validator interface {
	Validate(args map[string][]string) error
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
	}

	for _, v := range validators {
		if err := v.Validate(values); err != nil {
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
