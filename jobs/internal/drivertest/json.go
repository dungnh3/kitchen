package drivertest

import (
	"encoding/json"
	"reflect"
)

// jsonEqual compares two JSON documents by value: databases normalise key
// order and spacing.
func jsonEqual(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}
