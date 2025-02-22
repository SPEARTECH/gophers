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

// append dataframes
// union
// union_all

// drop rows

// drop columns

// corr()

// mean()

// median()

// mode()

// describe()

// sort_values()

// map()

// apply()

// query()

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

// dropna()

// rolling()

// isin()

// astype()

// to_datetime()

// sql?
