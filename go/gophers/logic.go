package gophers

import (
	"strings"
)

// where

// checks if column value for row is null
func IsNull(col string) ColumnFunc {
	return func(row map[string]interface{}) interface{} {
		if row[col] == nil {
			return true
		} else {
			switch v := row[col].(type) {
			case string:
				if v == "" || strings.ToLower(v) == "null" {
					return true
				} else {
					return false
				}
			}
		}
		return row[col]
	}
}

// notnull
