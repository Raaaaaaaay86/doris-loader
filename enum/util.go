package enum

import "reflect"

func IsZero(value any) bool {
	if value == nil {
		return true
	}

	return reflect.ValueOf(value).IsZero()
}