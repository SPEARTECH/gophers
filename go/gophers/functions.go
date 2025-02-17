package gophers

// import (
// 	"fmt"
// )

// functions for manipulating dataframes, take in and return dataframe
// .iloc = select column of the dataframe by name
func Iloc(df map[string]string, index int) map[string]string {
	return df
}

// .loc = select columns of the dataframe by index
func Loc(df map[string]string, index int) map[string]string {
	return df
}

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
func Lit(value interface{}) Column {
	return func(row map[string]interface{}) interface{} {
		return value
	}
}

// is in

// starts with

// ends with

// contains

// like - sql %%

// rlike - regex

// groupy by

// join

// joins

// merge

// concat

// append dataframes
// union
// union_all

// drop rows

// drop columns

// corr()

// mean()

// drop_duplicates()

// sort_values()

// map()

// apply()

// where()

// query()

// filter()

// dropna()

// rolling()

// isin()

// astype()

// casting?

// to_datetime()

// sql?
