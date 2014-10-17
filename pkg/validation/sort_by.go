package validation

import (
	"fmt"
	// "regexp"
	"strings"
)

type sortByValidator struct{}

func NewSortByValidator() Validator {
	return &sortByValidator{}
}

func (x *sortByValidator) Validate(args map[string][]string) (err error, res interface{}) {
	values, ok := args["sort_by"]

	if !ok {
		return
	}

	if len(values) > 1 {
		return fmt.Errorf("can only sort by one field"), nil
	}

	_, periodOk := args["period"]
	_, groupByOk := args["group_by"]

	if periodOk && !groupByOk {
		return fmt.Errorf(`Cannot sort for period queries without group_by. Period queries are always sorted by time."`), nil
	}

	return validateSortBy(values[0]), nil
}

func validateSortBy(candidate string) error {
	if strings.Index(candidate, ":") == -1 {
		return fmt.Errorf(`sort_by must be a field name and sort direction separated by a colon (:) eg 'authority:ascending'`)
	}

	values := strings.Split(candidate, ":")

	switch values[1] {
	case "ascending", "descending":
	default:
		{
			return fmt.Errorf(`Unrecognised sort direction '%v'. Supported directions include: ascending, descending`, values[1])
		}
	}

	if !IsValidKey(values[0]) {
		return fmt.Errorf("Invalid key <%v>", values[0])
	}

	return nil
}
