package gophers

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"strings"
)

// // functions for manipulating dataframes, take in and return dataframe
// // .iloc = select column of the dataframe by name
// func Iloc(df map[string]string, index int) map[string]string {
// 	return df
// }

// // .loc = select columns of the dataframe by index
// func Loc(df map[string]string, index int) map[string]string {
// 	return df
// }

// ColumnFunc is a function type that takes a row and returns a value.
// type Column func(row map[string]interface{}) interface{}
// Column represents a column in the DataFrame.
type Column struct {
	Name string
	Fn   func(row map[string]interface{}) interface{}
}

// // Column represents a column in the DataFrame.
// type Column struct {
// 	cf ColumnFunc
// }

// Col returns a Column for the specified column name.
func Col(name string) Column {
	return Column{
		Name: fmt.Sprintf("Col(%s)", name),
		Fn: func(row map[string]interface{}) interface{} {
			return row[name]
		},
	}
}

// Lit returns a Column that always returns the provided literal value.
func Lit(value interface{}) Column {
	return Column{
		Name: "lit",
		Fn: func(row map[string]interface{}) interface{} {
			return value
		},
	}
}

// CollectList returns a Column that is an array of the given column's values.
func CollectList(name string) Column {
	return Column{
		Name: name,
		Fn: func(row map[string]interface{}) interface{} {
			values := []interface{}{}
			for _, val := range row[name].([]interface{}) {
				values = append(values, val)
			}
			return values
		},
	}
}

// CollectSet returns a Column that is a set of unique values from the given column.
func CollectSet(name string) Column {
	return Column{
		Name: fmt.Sprintf("CollectSet(%s)", name),
		Fn: func(row map[string]interface{}) interface{} {
			valueSet := make(map[interface{}]bool)
			for _, val := range row[name].([]interface{}) {
				valueSet[val] = true
			}
			values := []interface{}{}
			for val := range valueSet {
				values = append(values, val)
			}
			return values
		},
	}
}

// datetime

// epoch

// SHA256 returns a Column that concatenates the values of the specified columns,
// computes the SHA-256 checksum of the concatenated string, and returns it as a string.
func SHA256(cols ...Column) Column {
	return Column{
		Name: "SHA256",
		Fn: func(row map[string]interface{}) interface{} {
			var concatenated string
			for _, col := range cols {
				val := col.Fn(row)
				str, err := toString(val)
				if err != nil {
					str = ""
				}
				concatenated += str
			}
			hash := sha256.Sum256([]byte(concatenated))
			return hex.EncodeToString(hash[:])
		},
	}
}

// SHA512 returns a Column that concatenates the values of the specified columns,
// computes the SHA-512 checksum of the concatenated string, and returns it as a string.
func SHA512(cols ...Column) Column {
	return Column{
		Name: "SHA512",
		Fn: func(row map[string]interface{}) interface{} {
			var concatenated string
			for _, col := range cols {
				val := col.Fn(row)
				str, err := toString(val)
				if err != nil {
					str = ""
				}
				concatenated += str
			}
			hash := sha512.Sum512([]byte(concatenated))
			return hex.EncodeToString(hash[:])
		},
	}
}

// from_json ? *

// Split returns a Column that splits the string value of the specified column by the given delimiter.
func Split(name string, delimiter string) Column {
	return Column{
		Name: fmt.Sprintf("Split(%s, %s)", name, delimiter),
		Fn: func(row map[string]interface{}) interface{} {
			val := row[name]
			str, err := toString(val)
			if err != nil {
				return []string{}
			}
			return strings.Split(str, delimiter)
		},
	}
}

// pivot (row to column) *

// replace

// regexp_replace *

// starts with

// ends with

// contains *

// like - sql %%

// rlike - regex

// regexp

// sort_values()

// map()

// apply()

// query()

// dropna()

// rolling()

// isin()

// astype()

// ToDatetime() *

// DateFormat() ? *

// ToDate() *

// DateDiff() *

// ToEpoch() *

// FromEpoch()

// sql?
