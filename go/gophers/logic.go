package gophers

import (
	"reflect"
	"strings"
)

// If implements conditional logic similar to PySpark's when.
// It returns fn1 if condition returns true for a row, else fn2.
func If(condition Column, fn1 Column, fn2 Column) Column {
	return func(row map[string]interface{}) interface{} {
		cond, ok := condition(row).(bool)
		if !ok {
			return nil
		}
		if cond {
			return fn1(row)
		}
		return fn2(row)
	}
}

// IsNull returns a new Column that, when applied to a row,
// returns true if the original column value is nil, an empty string, or "null".
func (c Column) IsNull() Column {
	return func(row map[string]interface{}) interface{} {
		val := c(row)
		if val == nil {
			return true
		}
		switch v := val.(type) {
		case string:
			return v == "" || strings.ToLower(v) == "null"
		case *string:
			return v == nil || *v == "" || strings.ToLower(*v) == "null"
		default:
			return false
		}
	}
}

// IsNotNull returns a new Column that, when applied to a row,
// returns true if the original column value is not nil, not an empty string, and not "null".
func (c Column) IsNotNull() Column {
	return func(row map[string]interface{}) interface{} {
		val := c(row)
		if val == nil {
			return false
		}
		switch v := val.(type) {
		case string:
			return !(v == "" || strings.ToLower(v) == "null")
		case *string:
			return !(v == nil || *v == "" || strings.ToLower(*v) == "null")
		default:
			return true
		}
	}
}

// Gt returns a Column that compares the numeric value at col with the given threshold.
// The threshold can be of any numeric type (int, float32, float64, etc.).
func (c Column) Gt(threshold interface{}) Column {
	return func(row map[string]interface{}) interface{} {
		val := c(row)
		fVal, err := toFloat64(val)
		if err != nil {
			return false
		}
		fThreshold, err := toFloat64(threshold)
		if err != nil {
			return false
		}
		return fVal > fThreshold
	}
}

// Ge returns a Column that compares the numeric value at col with the given threshold.
// The threshold can be of any numeric type (int, float32, float64, etc.).
func (c Column) Ge(threshold interface{}) Column {
	return func(row map[string]interface{}) interface{} {
		val := c(row)
		fVal, err := toFloat64(val)
		if err != nil {
			return false
		}
		fThreshold, err := toFloat64(threshold)
		if err != nil {
			return false
		}
		return fVal >= fThreshold
	}
}

// Lt returns a Column that compares the numeric value at col with the given threshold.
// The threshold can be of any numeric type (int, float32, float64, etc.).
func (c Column) Lt(threshold interface{}) Column {
	return func(row map[string]interface{}) interface{} {
		val := c(row)
		fVal, err := toFloat64(val)
		if err != nil {
			return false
		}
		fThreshold, err := toFloat64(threshold)
		if err != nil {
			return false
		}
		return fVal < fThreshold
	}
}

// Le returns a Column that compares the numeric value at col with the given threshold.
// The threshold can be of any numeric type (int, float32, float64, etc.).
func (c Column) Le(threshold interface{}) Column {
	return func(row map[string]interface{}) interface{} {
		val := c(row)
		fVal, err := toFloat64(val)
		if err != nil {
			return false
		}
		fThreshold, err := toFloat64(threshold)
		if err != nil {
			return false
		}
		return fVal <= fThreshold
	}
}

// Eq returns a Column that, when evaluated on a row,
// checks if the value from col is equal (same type and value) to threshold.
func (c Column) Eq(threshold interface{}) Column {
	return func(row map[string]interface{}) interface{} {
		val := c(row)
		// If either is nil, return equality directly.
		if val == nil || threshold == nil {
			return val == threshold
		}
		// Check that both values are of the same type.
		if reflect.TypeOf(val) != reflect.TypeOf(threshold) {
			return false
		}
		// Use Go's native equality.
		return val == threshold
	}
}

// Ne returns a Column that, when evaluated on a row,
// checks if the value from col is NOT equal (diff type or value) to threshold.
func (c Column) Ne(threshold interface{}) Column {
	return func(row map[string]interface{}) interface{} {
		val := c(row)
		// If either is nil, return equality directly.
		if val == nil || threshold == nil {
			return val != threshold
		}
		// Check that both values are of the same type.
		if reflect.TypeOf(val) != reflect.TypeOf(threshold) {
			return true
		}
		// Use Go's native equality.
		return val != threshold
	}
}

// Require - data source check for necessary columns and datatypes to prevent errors in etl process

// Or returns a Column that evaluates to true if either of the two provided Conditions is true.
func Or(c1, c2 Column) Column {
	return func(row map[string]interface{}) interface{} {
		cond1, ok1 := c1(row).(bool)
		cond2, ok2 := c2(row).(bool)
		if !ok1 || !ok2 {
			return false
		}
		return cond1 || cond2
	}
}

// And returns a Column that evaluates to true if both of the two provided Conditions is true.
func And(c1, c2 Column) Column {
	return func(row map[string]interface{}) interface{} {
		cond1, ok1 := c1(row).(bool)
		cond2, ok2 := c2(row).(bool)
		if !ok1 || !ok2 {
			return false
		}
		return cond1 && cond2
	}
}
