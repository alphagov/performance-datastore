package validation

import (
	"fmt"
	"strconv"
)

type positiveIntegerValidator struct {
	name string
}

// NewPositiveIntegerValidator returns a Validator that looks at the named argument to check it is a positive integer.
func NewPositiveIntegerValidator(name string) Validator {
	return &positiveIntegerValidator{name}
}

func (x *positiveIntegerValidator) Validate(args map[string][]string) (res interface{}, err error) {
	values, ok := args[x.name]

	if !ok {
		return
	}

	if len(values) > 1 {
		return nil, fmt.Errorf("Can only have a single value for %v", x.name)
	}

	i, err := strconv.Atoi(values[0])

	if err != nil {
		return nil, fmt.Errorf("expected integer for %v but was %v", x.name, values[0])
	}

	if i < 0 {
		return nil, fmt.Errorf("%v must be a positive integer", x.name)
	}

	return
}
