package validation

import (
	"fmt"
	"strings"
)

type filterByValidator struct{}

// NewFilterByValidator returns a Validator that looks at the filter_by argument.
func NewFilterByValidator() Validator {
	return &filterByValidator{}
}

func (x *filterByValidator) Validate(args map[string][]string) (res interface{}, err error) {
	values, ok := args["filter_by"]

	if !ok {
		return
	}

	for _, v := range values {
		if !isValidFilterBy(v) {
			return nil, fmt.Errorf("filter_by is not a valid")
		}
	}

	return
}

func isValidFilterBy(candidate string) bool {
	if strings.Index(candidate, ":") == -1 {
		return false
	}

	if !IsValidKey(strings.Split(candidate, ":")[0]) {
		return false
	}

	if strings.HasPrefix(candidate, "$") {
		return false
	}

	return true
}
