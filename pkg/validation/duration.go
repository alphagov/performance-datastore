package validation

import (
	"fmt"
)

type durationValidator struct{}

// NewDurationValidator is a Validator that checks duration arguments.
func NewDurationValidator() Validator {
	return &durationValidator{}
}

func (x *durationValidator) Validate(args map[string][]string) (res interface{}, err error) {
	values, durationOk := args["duration"]

	_, periodOk := args["period"]
	_, startAtOk := args["start_at"]
	_, endAtOk := args["end_at"]

	if durationOk && startAtOk && endAtOk {
		return nil, fmt.Errorf(`Absolute and relative time cannot be requested at the same time - either ask for 'start_at' and 'end_at', or ask for 'start_at'/'end_at' with 'duration'`)
	}

	if startAtOk && !(durationOk || endAtOk) {
		return nil, fmt.Errorf(`Use of 'start_at' requires 'end_at' or 'duration'`)
	}

	if endAtOk && !(durationOk || startAtOk) {
		return nil, fmt.Errorf(`Use of 'end_at' requires 'start_at' or 'duration'`)
	}

	if durationOk {
		if !periodOk {
			return nil, fmt.Errorf(`If 'duration' is requested (for relative time), 'period' is required - please add a period (like 'day', 'month' etc)`)
		}
		if len(values) > 1 {
			return nil, fmt.Errorf("duration should be a single argument but received %v", len(values))
		}
		if values[0] == "0" {
			return nil, fmt.Errorf("duration must be positive")
		}

	}

	if startAtOk && endAtOk {

	}

	return
}
