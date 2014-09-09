package validation

import (
	"fmt"
	"strings"
)

type groupByValidator struct{}

func NewGroupByValidator() Validator {
	return &groupByValidator{}
}

func (x *groupByValidator) Validate(args map[string][]string) (err error, res interface{}) {
	values, ok := args["group_by"]

	if !ok {
		return
	}

	if len(values) > 1 {
		return fmt.Errorf("Can only have a single value for <group_by>"), nil
	}

	if !isValidKey(values[0]) {
		return fmt.Errorf("Cannot group by an invalid field name"), nil
	}

	if strings.HasPrefix(values[0], "_") {
		return fmt.Errorf("Cannot group by internal fields, internal fields start with an underscore"), nil
	}

	return
}
