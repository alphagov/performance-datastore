package validation

import (
	"fmt"
)

type periodValidator struct{}

func NewPeriodValidator() Validator {
	return &periodValidator{}
}

func (x *periodValidator) Validate(args map[string][]string) (err error) {
	values, ok := args["period"]

	if !ok {
		return
	}

	if len(values) > 1 {
		return fmt.Errorf("Can only define a single period")
	}

	_, limitOk := args["limit"]
	_, groupByOk := args["group_by"]

	if limitOk && !groupByOk {
		return fmt.Errorf("A period query can only be limited if it also has a group_by clause")
	}

	switch values[0] {
	case "day", "week", "month", "year":
	default:
		return fmt.Errorf("Period value not recognised %v", values[0])
	}

	return
}
