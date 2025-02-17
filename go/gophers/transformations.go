package gophers

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Column adds or modifies a column in the DataFrame using a Column.
// This version accepts a Column (whose underlying function is applied to each row).
func (df *DataFrame) Column(column string, col Column) *DataFrame {
	values := make([]interface{}, df.Rows)
	for i := 0; i < df.Rows; i++ {
		row := make(map[string]interface{})
		for _, c := range df.Cols {
			row[c] = df.Data[c][i]
		}
		// Use the underlying ColumnFunc.
		values[i] = col.cf(row)
	}

	// Add or modify the column.
	df.Data[column] = values

	// Add the column to the list of columns if it doesn't already exist.
	exists := false
	for _, c := range df.Cols {
		if c == column {
			exists = true
			break
		}
	}
	if !exists {
		df.Cols = append(df.Cols, column)
	}

	return df
}

// withColumnRenamed
// rename column
func (df *DataFrame) Rename(column string, newcol string) *DataFrame {
	newcols := make([]string, len(df.Cols))
	for i, col := range df.Cols {
		if col == column {
			newcols[i] = newcol
		} else {
			newcols[i] = col
		}
	}

	newDF := &DataFrame{
		Cols: newcols,
		Data: make(map[string][]interface{}),
		Rows: df.Rows,
	}

	// Copy the data from the original DataFrame to the new DataFrame.
	for _, col := range df.Cols {
		if col == column {
			newDF.Data[newcol] = df.Data[col]
		} else {
			newDF.Data[col] = df.Data[col]
		}
	}

	return newDF
}

// fillna
func (df *DataFrame) FillNA(replacement string) *DataFrame {
	quotedReplacement := fmt.Sprintf("%q", replacement)
	for col, values := range df.Data {
		for i, value := range values {
			if value == nil {
				df.Data[col][i] = quotedReplacement
			} else {
				switch v := value.(type) {
				case string:
					if v == "" || strings.ToLower(v) == "null" {
						df.Data[col][i] = quotedReplacement
					}
				}
			}
		}
	}
	return df
}

// dropduplicates
// DropDuplicates removes duplicate rows from the DataFrame.
func (df *DataFrame) DropDuplicates() *DataFrame {
	seen := make(map[string]bool)
	newData := make(map[string][]interface{})
	for _, col := range df.Cols {
		newData[col] = []interface{}{}
	}

	for i := 0; i < df.Rows; i++ {
		row := make(map[string]interface{})
		for _, col := range df.Cols {
			row[col] = df.Data[col][i]
		}

		// Convert the row to a JSON string to use as a key in the map.
		rowBytes, _ := json.Marshal(row)
		rowStr := string(rowBytes)

		if !seen[rowStr] {
			seen[rowStr] = true
			for _, col := range df.Cols {
				newData[col] = append(newData[col], df.Data[col][i])
			}
		}
	}

	// Update the DataFrame with the new data.
	df.Data = newData
	df.Rows = len(newData[df.Cols[0]])

	return df
}

// select
func (df *DataFrame) Select(columns ...string) *DataFrame {
	newDF := &DataFrame{
		Cols: columns,
		Data: make(map[string][]interface{}),
		Rows: df.Rows,
	}

	for _, col := range columns {
		if data, exists := df.Data[col]; exists {
			newDF.Data[col] = data
		} else {
			newDF.Data[col] = make([]interface{}, df.Rows)
		}
	}

	return newDF
}

// concat

// concat_ws

// filter

// explode

// cast

// groupby

// agg

// orderby

// join

// datetime

// epoch

// sha256

// sha512

// from_json ?

// split

// pivot (row to column)

// replace

//
