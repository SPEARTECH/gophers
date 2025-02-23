package gophers

// import (
// 	"fmt"
// )

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
type Column func(row map[string]interface{}) interface{}

// // Column represents a column in the DataFrame.
// type Column struct {
// 	cf ColumnFunc
// }

// Col returns a Column for the specified column name.
func Col(col string) Column {
	return func(row map[string]interface{}) interface{} {
		return row[col]
	}
}

// Lit returns a Column that always returns the provided literal value.
// check if same type for column?
func Lit(value interface{}) Column {
	return func(row map[string]interface{}) interface{} {
		return value
	}
}

// datetime

// epoch

// sha256

// sha512

// from_json ?

// split

// pivot (row to column)

// replace

// regexp_replace

// starts with

// ends with

// contains

// like - sql %%

// rlike - regex

// regexp

// corr()

// describe()

// sort_values()

// map()

// apply()

// query()

// dropna()

// rolling()

// isin()

// astype()

// ToDatetime()

// DateFormat() ?

// ToDate()

// DateDiff()

// ToEpoch()

// FromEpoch()

// sql?
