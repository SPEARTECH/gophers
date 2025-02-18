package gophers

import (
	"encoding/json"
	"fmt"
	"strconv"
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
		values[i] = col(row)
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

// alias
// func (c Column) Alias(newname string) Column{
// 	return
// }

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

// Concat returns a Column that, when applied to a row,
// concatenates the string representations of the provided Columns.
// It converts each value to a string using toString.
// If conversion fails, it uses an empty string.
func Concat(cols ...Column) Column {
	return func(row map[string]interface{}) interface{} {
		var parts []string
		for _, col := range cols {
			val := col(row)
			str, err := toString(val)
			if err != nil {
				str = ""
			}
			parts = append(parts, str)
		}
		// Customize the delimiter as needed.
		return strings.Join(parts, "")
	}
}

// Concat_ws returns a Column that, when applied to a row,
// concatenates the string representations of the provided Columns using the specified delimiter.
// It converts each value to a string using toString. If conversion fails for a value, it uses an empty string.
func Concat_ws(delim string, cols ...Column) Column {
	return func(row map[string]interface{}) interface{} {
		var parts []string
		for _, col := range cols {
			val := col(row)
			str, err := toString(val)
			if err != nil {
				str = ""
			}
			parts = append(parts, str)
		}
		return strings.Join(parts, delim)
	}
}

// Filter returns a new DataFrame containing only the rows for which
// the condition (a Column that evaluates to a bool) is true.
func (df *DataFrame) Filter(condition Column) *DataFrame {
	// Create new DataFrame with the same columns.
	newDF := &DataFrame{
		Cols: df.Cols,
		Data: make(map[string][]interface{}),
	}
	for _, col := range df.Cols {
		newDF.Data[col] = []interface{}{}
	}

	// Iterate over each row.
	for i := 0; i < df.Rows; i++ {
		// Build a row (as a map) for evaluation.
		row := make(map[string]interface{})
		for _, col := range df.Cols {
			row[col] = df.Data[col][i]
		}
		// Evaluate the condition.
		cond := condition(row)
		if b, ok := cond.(bool); ok && b {
			// If true, append data from this row to newDF.
			for _, col := range df.Cols {
				newDF.Data[col] = append(newDF.Data[col], row[col])
			}
		}
	}

	// Set new row count.
	if len(df.Cols) > 0 {
		newDF.Rows = len(newDF.Data[df.Cols[0]])
	}

	return newDF
}

// explode

// Cast takes in an existing Column and a desired datatype ("int", "float", "string"),
// and returns a new Column that casts the value returned by the original Column to that datatype.
func Cast(col Column, datatype string) Column {
	return func(row map[string]interface{}) interface{} {
		val := col(row)
		switch datatype {
		case "int":
			casted, err := toInt(val)
			if err != nil {
				fmt.Printf("cast to int error: %v\n", err)
				return nil
			}
			return casted
		case "float":
			casted, err := toFloat64(val)
			if err != nil {
				fmt.Printf("cast to float error: %v\n", err)
				return nil
			}
			return casted
		case "string":
			casted, err := toString(val)
			if err != nil {
				fmt.Printf("cast to string error: %v\n", err)
				return nil
			}
			return casted
		default:
			fmt.Printf("unsupported cast type: %s\n", datatype)
			return nil
		}
	}
}

// toFloat64 attempts to convert an interface{} to a float64.
func toFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf("unsupported numeric type: %T", val)
	}
}

// toInt tries to convert the provided value to an int.
// It supports int, int32, int64, float32, float64, and string.
func toInt(val interface{}) (int, error) {
	switch v := val.(type) {
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case float32:
		return int(v), nil
	case float64:
		return int(v), nil
	case string:
		i, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string %q to int: %v", v, err)
		}
		return i, nil
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

// toString attempts to convert an interface{} to a string.
// It supports string, int, int32, int64, float32, and float64.
func toString(val interface{}) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case int:
		return strconv.Itoa(v), nil
	case int32:
		return strconv.Itoa(int(v)), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	default:
		return "", fmt.Errorf("unsupported type %T", val)
	}
}

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
