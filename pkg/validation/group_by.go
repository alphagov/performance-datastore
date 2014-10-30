package validation

import (
	"fmt"
	"strings"
)

type groupByValidator struct{}

// NewGroupByValidator returns a Validator that looks at the group_by argument.
func NewGroupByValidator() Validator {
	return &groupByValidator{}
}

func (x *groupByValidator) Validate(args map[string][]string) (res interface{}, err error) {
	values, ok := args["group_by"]

	if !ok {
		return
	}

	if len(values) > 1 {
		return nil, fmt.Errorf("Can only have a single value for <group_by>")
	}

	if !IsValidKey(values[0]) {
		return nil, fmt.Errorf("Cannot group by an invalid field name")
	}

	if strings.HasPrefix(values[0], "_") {
		return nil, fmt.Errorf("Cannot group by internal fields, internal fields start with an underscore")
	}

	return
}
