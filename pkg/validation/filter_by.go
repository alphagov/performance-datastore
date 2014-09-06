package validation

import (
	"fmt"
	"regexp"
	"strings"
)

type filterBy struct{}

type filterByValidator filterBy

var (
	validKey = regexp.MustCompile(`^[a-z_][a-z0-9_]+$`)
)

func NewFilterByValidator() Validator {
	return (*filterByValidator)(newFilterByValidator())
}

func newFilterByValidator() *filterBy {
	return &filterBy{}
}

func (x *filterByValidator) Validate(args map[string][]string) error {
	values, ok := args["filter_by"]

	if !ok {
		return nil
	}

	for _, v := range values {
		if !isValidFilterBy(v) {
			return fmt.Errorf("filter_by is not a valid")
		}
	}

	return nil
}

func isValidFilterBy(candidate string) bool {
	if strings.Index(candidate, ":") == -1 {
		return false
	}

	if !validKey.MatchString(strings.Split(candidate, ":")[0]) {
		return false
	}

	if strings.HasPrefix(candidate, "$") {
		return false
	}

	return true
}
