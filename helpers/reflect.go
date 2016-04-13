package helpers

import "reflect"

//Check to see if an interface is a slice
func IsSlice(s interface{}) bool {
	t := reflect.ValueOf(s).Type()

	return t.Kind() == reflect.Slice
}

//Checks to see if interface is a slice
//If it's not, resolve it to one
//It will always return a interface{}, pointing to a []interface{}
//Returns the slice, its length and if it was a slice
//If the input is nil, returns nil, 0, false
func EnsureSlice(s interface{}) ([]interface{}, int, bool) {
	if s == nil {
		return nil, 0, false
	}
	val := reflect.ValueOf(s)
	t := val.Type()

	if t.Kind() != reflect.Slice {
		var sl []interface{}

		sl = append(sl, s)

		return sl, 1, false

	} else {
		sl := make([]interface{}, val.Len())

		len := val.Len()
		for i := 0; i < len; i++ {
			sl[i] = val.Index(i).Interface()
		}
		return sl, len, true
	}
}

//Checks to see if interface is a slice
//If it is, resolve it to the first element of the underlying interface
//If it's an empty slice, will return nil for this
//Returns the interface, and if it WASN'T a slice
//If input is nil, returns nil false
func EnsureNotSlice(s interface{}) (interface{}, bool) {
	if s == nil {
		return nil, false
	}

	val := reflect.ValueOf(s)
	t := val.Type()

	if t.Kind() != reflect.Slice {
		return s, true
	} else {
		if val.Len() > 0 {
			return val.Index(0).Interface(), false
		} else {
			return nil, false
		}
	}
}
