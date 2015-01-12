package utils

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func Unmarshal(res []byte, result interface{}) error {
	switch kind := reflect.TypeOf(result).Kind(); kind {
	case reflect.Ptr:
	default:
		return fmt.Errorf("parameter result should be a pointer, but is %v", kind)
	}

	err := json.Unmarshal(res, result)

	if err != nil {
		return err
	}

	return nil
}
