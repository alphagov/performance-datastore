package validation

import (
	"fmt"
	"strings"
)

type collectValidator struct{}

func NewCollectValidator() Validator {
	return &collectValidator{}
}

func (x *collectValidator) Validate(args map[string][]string) (err error, res interface{}) {
	values, ok := args["collect"]

	if !ok {
		return
	}

	_, periodOk := args["period"]
	groupBy, groupByOk := args["group_by"]

	if !(groupByOk || periodOk) {
		return fmt.Errorf("collect can only be used with either period or group_by"), nil
	}

	for _, v := range values {
		key := v

		if strings.Index(key, ":") != -1 {
			collect := strings.Split(key, ":")
			if len(collect) != 2 {
				return fmt.Errorf("Badly formatted collect <%v>", key), nil
			}
			var operator string
			key, operator = collect[0], collect[1]
			switch operator {
			case "sum", "mean", "count", "set":
			default:
				return fmt.Errorf("Unknown collect method %v", operator), nil
			}
		}

		if !isValidKey(key) {
			return fmt.Errorf("collect isn't a valid key <%v>", key), nil
		}

		if strings.HasPrefix(key, "_") {
			return fmt.Errorf("Cannot collect on an internal field"), nil
		}

		if groupByOk && len(groupBy) == 1 && groupBy[0] == key {
			return fmt.Errorf("Cannot collect on the same field being used for group_by"), nil
		}

	}
	return
}
