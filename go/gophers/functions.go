package gophers

// import (
// 	"fmt"
// )

// functions for manipulating dataframes, take in and return dataframe
// .iloc = select column of the dataframe by name
func ILOC(df map[string]string, index int) map[string]string {
	return df
}

// .loc = select columns of the dataframe by index
func LOC(df map[string]string, index int) map[string]string {
	return df
}

// ColumnFunc is a function type that takes a row and returns a value.
type ColumnFunc func(row map[string]interface{}) interface{}

// Example function to calculate new column values.
func Values(col string) ColumnFunc {
	return func(row map[string]interface{}) interface{} {
		return row[col]
	}
}

// lit
func Lit(str string) string {
	return str
}

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
