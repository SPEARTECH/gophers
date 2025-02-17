package gophers

import (
	"strings"
)

// When implements conditional logic similar to PySpark's when.
// It returns fn1 if condition.cf(row) evaluates to true, else fn2.
func When(condition Column, fn1 Column, fn2 Column) Column {
	return Column{
		cf: func(row map[string]interface{}) interface{} {
			// Ensure the condition evaluates to a boolean.
			cond, ok := condition.cf(row).(bool)
			if !ok {
				return nil
			}
			if cond {
				return fn1.cf(row)
			}
			return fn2.cf(row)
		},
	}
}

func Otherwise(fn ColumnFunc) ColumnFunc {
	return fn
}

// // Filter returns dataframe rows based on logic provided
// func Filter(){

// }

// // Where returns dataframe rows based on logic provided
// func Where(){

// }

// IsNull returns a new Column that, when evaluated with a row,
// returns true if the original column value is nil, an empty string, or "null".
func (c Column) IsNull() Column {
	return Column{
		cf: func(row map[string]interface{}) interface{} {
			value := c.cf(row)
			if value == nil {
				return true
			}
			switch v := value.(type) {
			case string:
				return v == "" || strings.ToLower(v) == "null"
			case *string:
				return v == nil || *v == "" || strings.ToLower(*v) == "null"
			default:
				return false
			}
		},
	}
}

// // NotNull checks if the value returned by the ColumnFunc is null.
// func (cfw ColumnFuncWrapper) NotNull() bool {
// 	value := cfw.cf(cfw.row)
// 	if value != nil {
// 		return true
// 	}

// 	switch v := value.(type) {
// 	case string:
// 		return v != "" || strings.ToLower(v) != "null"
// 	case *string:
// 		return v != nil || *v != "" || strings.ToLower(*v) != "null"
// 	default:
// 		return false
// 	}
// }
