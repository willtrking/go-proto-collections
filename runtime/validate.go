package runtime

import (
	"fmt"
	"strconv"
)

//Validation helpers, nothing here is required

type WriterError struct {
	Desc  string
	Attr  string
	Index string
}

func (e WriterError) Error() string {
	return e.Desc
}

type Validator struct {
	errorSlice []WriterError
}

//Create our validator, create a channel to take errors
func NewValidator() *Validator {
	return &Validator{}
}

func (v *Validator) RecordIndexedError(attr string, index int, errMsg string, a ...interface{}) {
	v.errorSlice = append(v.errorSlice, WriterError{Attr: attr, Desc: fmt.Sprintf(errMsg, a...), Index: strconv.Itoa(index)})
}

func (v *Validator) RecordError(attr string, errMsg string, a ...interface{}) {
	v.errorSlice = append(v.errorSlice, WriterError{Attr: attr, Desc: fmt.Sprintf(errMsg, a...), Index: ""})
}

//Basic bool validation
func (v *Validator) CheckBool(attr string, c bool, errMsg string, a ...interface{}) {
	if !c {
		v.RecordError(attr, errMsg, a...)
	}
}

func (v *Validator) CheckIndexedBool(attr string, index int, c bool, errMsg string, a ...interface{}) {
	if !c {
		v.RecordIndexedError(attr, index, errMsg, a...)
	}
}

func (v *Validator) Errors() []WriterError {
	return v.errorSlice
}
