package validation

import (
	"fmt"
	"strconv"
)

type positiveIntegerValidator struct {
	name string
}

func NewPositiveIntegerValidator(name string) Validator {
	return &positiveIntegerValidator{name}
}

func (x *positiveIntegerValidator) Validate(args map[string][]string) (err error, res interface{}) {
	values, ok := args[x.name]

	if !ok {
		return
	}

	if len(values) > 1 {
		return fmt.Errorf("Can only have a single value for %v", x.name), nil
	}

	i, err := strconv.Atoi(values[0])

	if err != nil {
		return fmt.Errorf("expected integer for %v but was %v", x.name, values[0]), nil
	}

	if i < 0 {
		return fmt.Errorf("%v must be a positive integer", x.name), nil
	}

	return
}
