package validation

import (
	"fmt"
	"strings"
)

type collectValidator struct{}

// NewCollectValidator returns a Validator implementation for collect params.
func NewCollectValidator() Validator {
	return &collectValidator{}
}

func (x *collectValidator) Validate(args map[string][]string) (res interface{}, err error) {
	values, ok := args["collect"]

	if !ok {
		return
	}

	_, periodOk := args["period"]
	groupBy, groupByOk := args["group_by"]

	if !(groupByOk || periodOk) {
		return nil, fmt.Errorf("collect can only be used with either period or group_by")
	}

	for _, v := range values {
		key := v

		if strings.Index(key, ":") != -1 {
			collect := strings.Split(key, ":")
			if len(collect) != 2 {
				return nil, fmt.Errorf("Badly formatted collect <%v>", key)
			}
			var operator string
			key, operator = collect[0], collect[1]
			switch operator {
			case "sum", "mean", "count", "set":
			default:
				return nil, fmt.Errorf("Unknown collect method %v", operator)
			}
		}

		if !IsValidKey(key) {
			return nil, fmt.Errorf("collect isn't a valid key <%v>", key)
		}

		if strings.HasPrefix(key, "_") {
			return nil, fmt.Errorf("Cannot collect on an internal field")
		}

		if groupByOk && len(groupBy) == 1 && groupBy[0] == key {
			return nil, fmt.Errorf("Cannot collect on the same field being used for group_by")
		}

	}
	return
}
