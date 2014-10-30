package validation

import (
	"fmt"
)

type periodValidator struct{}

// NewPeriodValidator returns a Validator that looks at the period argument.
func NewPeriodValidator() Validator {
	return &periodValidator{}
}

func (x *periodValidator) Validate(args map[string][]string) (res interface{}, err error) {
	values, ok := args["period"]

	if !ok {
		return
	}

	if len(values) > 1 {
		return nil, fmt.Errorf("Can only define a single period")
	}

	_, limitOk := args["limit"]
	_, groupByOk := args["group_by"]

	if limitOk && !groupByOk {
		return nil, fmt.Errorf("A period query can only be limited if it also has a group_by clause")
	}

	switch values[0] {
	case "hour", "day", "week", "month", "quarter", "year":
	default:
		return nil, fmt.Errorf("Period value not recognised %v", values[0])
	}

	return values[0], nil
}
