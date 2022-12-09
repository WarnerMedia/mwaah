// Copyright (c) Warner Media, LLC. All rights reserved. Licensed under the MIT license.
// See the LICENSE file for license information.
package mwaah

import (
	"reflect"
	"time"
)

const (
	PythonISONoDecimalTimeLayout = "2006-01-02T15:04:05-07:00"
	PythonISODecimalTimeLayout   = "2006-01-02T15:04:05.999999-07:00"
)

// takes in a dst struct{} and fills data in from slice, in order of Field placement
func populate(dst any, src any) {
	v := reflect.ValueOf(dst)
	if v.Type().Kind() != reflect.Pointer {
		panic("dst must be a pointer")
	}
	v = v.Elem()
	if v.Type().Kind() != reflect.Struct {
		panic("dst must be a pointer to struct")
	}

	w := reflect.ValueOf(src)
	if w.Type().Kind() != reflect.Slice {
		panic("src must be a slice")
	}
	for i := 0; i < v.NumField(); i++ {
		// in case you need to support source slices of arbitrary types
		value := w.Index(i)
		if value.Type().Kind() == reflect.Interface {
			value = value.Elem()
		}
		if v.Field(i).Kind() == reflect.Pointer {
			v.Field(i).Set(value.Addr())
		} else {
			v.Field(i).Elem().Set(value)
		}
	}

}

// Used in the stdout from the `airflow dags trigger` cmd
func GetTimePythonISONoDecimal(PythonISONoDecimal string) (time.Time, error) {
	return time.Parse(PythonISONoDecimalTimeLayout, PythonISONoDecimal)
}

func GetTimePythonISODecimal(PythonISONoDecimal string) (time.Time, error) {
	return time.Parse(PythonISODecimalTimeLayout, PythonISONoDecimal)
}
