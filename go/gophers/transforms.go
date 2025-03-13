package gophers

import (
	"encoding/json"
	"fmt"
	"sort"
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
		// Use the underlying Column function.
		values[i] = col.Fn(row)
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

// Concat returns a Column that, when applied to a row,
// concatenates the string representations of the provided Columns using the specified delimiter.
// It converts each value to a string using toString. If conversion fails for a value, it uses an empty string.
func Concat(delim string, cols ...Column) Column {
	return Column{
		Name: "concat_ws",
		Fn: func(row map[string]interface{}) interface{} {
			var parts []string
			for _, col := range cols {
				val := col.Fn(row)
				str, err := toString(val)
				if err != nil {
					str = ""
				}
				parts = append(parts, str)
			}
			return strings.Join(parts, delim)
		},
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
		cond := condition.Fn(row)
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

// Explode creates a new DataFrame where each value in the specified columns' slices becomes a separate row.
func (df *DataFrame) Explode(columns ...string) *DataFrame {
	for _, column := range columns {
		df = df.explodeSingleColumn(column)
	}
	return df
}

// explodeSingleColumn creates a new DataFrame where each value in the specified column's slice becomes a separate row.
func (df *DataFrame) explodeSingleColumn(column string) *DataFrame {
	newCols := df.Cols
	newData := make(map[string][]interface{})

	// Initialize newData with empty slices for each column.
	for _, col := range newCols {
		newData[col] = []interface{}{}
	}

	// Iterate over each row in the DataFrame.
	for i := 0; i < df.Rows; i++ {
		// Get the value of the specified column.
		val := df.Data[column][i]

		// Check if the value is a slice.
		if slice, ok := val.([]interface{}); ok {
			// Create a new row for each value in the slice.
			for _, item := range slice {
				for _, col := range newCols {
					if col == column {
						newData[col] = append(newData[col], item)
					} else {
						newData[col] = append(newData[col], df.Data[col][i])
					}
				}
			}
		} else {
			// If the value is not a slice, just copy the row as is.
			for _, col := range newCols {
				newData[col] = append(newData[col], df.Data[col][i])
			}
		}
	}

	return &DataFrame{
		Cols: newCols,
		Data: newData,
		Rows: len(newData[newCols[0]]),
	}
}

// Cast takes in an existing Column and a desired datatype ("int", "float", "string"),
// and returns a new Column that casts the value returned by the original Column to that datatype.
func Cast(col Column, datatype string) Column {
	return Column{
		Name: col.Name + "_cast",
		Fn: func(row map[string]interface{}) interface{} {
			val := col.Fn(row)
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
		},
	}
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

// DropDuplicates removes duplicate rows from the DataFrame.
// If one or more columns are provided, only those columns are used to determine uniqueness.
// If no columns are provided, the entire row (all columns) is used.
func (df *DataFrame) DropDuplicates(columns ...string) *DataFrame {
	// If no columns are specified, use all columns.
	uniqueCols := columns
	if len(uniqueCols) == 0 {
		uniqueCols = df.Cols
	}

	seen := make(map[string]bool)
	newData := make(map[string][]interface{})
	for _, col := range df.Cols {
		newData[col] = []interface{}{}
	}

	for i := 0; i < df.Rows; i++ {
		// Build a subset row only with the uniqueCols.
		rowSubset := make(map[string]interface{})
		for _, col := range uniqueCols {
			rowSubset[col] = df.Data[col][i]
		}

		// Convert the subset row to a JSON string to use as a key.
		rowBytes, err := json.Marshal(rowSubset)
		if err != nil {
			// If marshalling fails, skip this row.
			continue
		}
		rowStr := string(rowBytes)

		if !seen[rowStr] {
			seen[rowStr] = true
			// Append the full row (all columns) to the new data.
			for _, col := range df.Cols {
				newData[col] = append(newData[col], df.Data[col][i])
			}
		}
	}

	// Update the DataFrame with the new data.
	df.Data = newData
	if len(df.Cols) > 0 {
		df.Rows = len(newData[df.Cols[0]])
	} else {
		df.Rows = 0
	}

	return df
}

// Select returns a new DataFrame containing only the specified columns.
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

// GroupBy groups the DataFrame rows by the value produced by groupCol.
// For each group, it applies each provided Aggregation on the values
// from the corresponding column.
// The new DataFrame has a "group" column for the grouping key and one column per Aggregation.
func (df *DataFrame) GroupBy(groupcol string, aggs ...Aggregation) *DataFrame {
	// Build groups. The key is the groupCol result, and the value is a map: column → slice of values.
	groups := make(map[interface{}]map[string][]interface{})

	// Iterate over each row and group them.
	for i := 0; i < df.Rows; i++ {
		// Build the row as a map.
		row := make(map[string]interface{})
		for _, col := range df.Cols {
			row[col] = df.Data[col][i]
		}
		key := row[groupcol]
		if _, ok := groups[key]; !ok {
			groups[key] = make(map[string][]interface{})
			// Initialize slices for each aggregation target.
			for _, agg := range aggs {
				groups[key][agg.ColumnName] = []interface{}{}
			}
		}
		// Append each aggregation target value.
		for _, agg := range aggs {
			val, ok := row[agg.ColumnName]
			if ok {
				groups[key][agg.ColumnName] = append(groups[key][agg.ColumnName], val)
			}
		}
	}

	// Prepare the new DataFrame.
	newCols := []string{groupcol}
	// Use the target column names for aggregated data.
	for _, agg := range aggs {
		newCols = append(newCols, agg.ColumnName)
	}

	newData := make(map[string][]interface{})
	for _, col := range newCols {
		newData[col] = []interface{}{}
	}

	// Generate one aggregated row per group.
	for key, groupValues := range groups {
		newData[groupcol] = append(newData[groupcol], key)
		for _, agg := range aggs {
			aggregatedValue := agg.Fn(groupValues[agg.ColumnName])
			newData[agg.ColumnName] = append(newData[agg.ColumnName], aggregatedValue)
		}
	}

	return &DataFrame{
		Cols: newCols,
		Data: newData,
		Rows: len(newData[groupcol]),
	}
}

// Join performs a join between the receiver (left DataFrame) and the provided right DataFrame.
// leftOn is the join key column in the left DataFrame and rightOn is the join key column in the right DataFrame.
// joinType can be "inner", "left", "right", or "outer". It returns a new joined DataFrame.
func (left *DataFrame) Join(right *DataFrame, leftOn, rightOn, joinType string) *DataFrame {
	// Build new column names: left columns plus right columns (skipping duplicate join key from right).
	newCols := make([]string, 0)
	newCols = append(newCols, left.Cols...)
	for _, col := range right.Cols {
		if col == rightOn {
			continue
		}
		newCols = append(newCols, col)
	}

	// Initialize new data structure.
	newData := make(map[string][]interface{})
	for _, col := range newCols {
		newData[col] = []interface{}{}
	}

	// Build index maps:
	// leftIndex: maps join key -> slice of row indices in left.
	leftIndex := make(map[interface{}][]int)
	for i := 0; i < left.Rows; i++ {
		key := left.Data[leftOn][i]
		leftIndex[key] = append(leftIndex[key], i)
	}
	// rightIndex: maps join key -> slice of row indices in right.
	rightIndex := make(map[interface{}][]int)
	for j := 0; j < right.Rows; j++ {
		key := right.Data[rightOn][j]
		rightIndex[key] = append(rightIndex[key], j)
	}

	// A helper to add a combined row.
	// If lIdx or rIdx is nil, the respective values are set to nil.
	addRow := func(lIdx *int, rIdx *int) {
		// Append values from left.
		for _, col := range left.Cols {
			var val interface{}
			if lIdx != nil {
				val = left.Data[col][*lIdx]
			} else {
				val = nil
			}
			newData[col] = append(newData[col], val)
		}
		// Append values from right (skip join key since already added from left).
		for _, col := range right.Cols {
			if col == rightOn {
				continue
			}
			var val interface{}
			if rIdx != nil {
				val = right.Data[col][*rIdx]
			} else {
				val = nil
			}
			newData[col] = append(newData[col], val)
		}
	}

	// Perform join based on joinType.
	switch joinType {
	case "inner", "left", "outer":
		// Process all keys from left.
		for key, leftRows := range leftIndex {
			rightRows, exists := rightIndex[key]
			if exists {
				// For matching keys, add all combinations.
				for _, li := range leftRows {
					for _, ri := range rightRows {
						addRow(&li, &ri)
					}
				}
			} else {
				// No matching right rows.
				if joinType == "left" || joinType == "outer" {
					for _, li := range leftRows {
						addRow(&li, nil)
					}
				}
			}
		}
		// For "outer" join, add rows from right that weren't matched by left.
		if joinType == "outer" {
			for key, rightRows := range rightIndex {
				if _, exists := leftIndex[key]; !exists {
					for _, ri := range rightRows {
						addRow(nil, &ri)
					}
				}
			}
		}
	case "right":
		// Process all keys from right.
		for key, rightRows := range rightIndex {
			leftRows, exists := leftIndex[key]
			if exists {
				for _, li := range leftRows {
					for _, ri := range rightRows {
						addRow(&li, &ri)
					}
				}
			} else {
				for _, ri := range rightRows {
					addRow(nil, &ri)
				}
			}
		}
	default:
		fmt.Printf("Unsupported join type: %s\n", joinType)
		return nil
	}

	// Determine joined row count.
	nRows := 0
	if len(newCols) > 0 {
		nRows = len(newData[newCols[0]])
	}

	return &DataFrame{
		Cols: newCols,
		Data: newData,
		Rows: nRows,
	}
}

// Union appends the rows of the other DataFrame to the receiver.
// It returns a new DataFrame that contains the union (vertical concatenation)
// of rows. Columns missing in one DataFrame are filled with nil.
func (df *DataFrame) Union(other *DataFrame) *DataFrame {
	// Build the union of columns.
	colSet := make(map[string]bool)
	newCols := []string{}
	// Add columns from the receiver.
	for _, col := range df.Cols {
		if !colSet[col] {
			newCols = append(newCols, col)
			colSet[col] = true
		}
	}
	// Add columns from the other DataFrame.
	for _, col := range other.Cols {
		if !colSet[col] {
			newCols = append(newCols, col)
			colSet[col] = true
		}
	}

	// Initialize new data map.
	newData := make(map[string][]interface{})
	for _, col := range newCols {
		newData[col] = []interface{}{}
	}

	// Helper to append a row from a given DataFrame.
	appendRow := func(source *DataFrame, rowIndex int) {
		for _, col := range newCols {
			// If the source DataFrame has this column, use its value.
			if sourceVal, ok := source.Data[col]; ok {
				newData[col] = append(newData[col], sourceVal[rowIndex])
			} else {
				// Otherwise, fill with nil.
				newData[col] = append(newData[col], nil)
			}
		}
	}

	// Append rows from the receiver.
	for i := 0; i < df.Rows; i++ {
		appendRow(df, i)
	}
	// Append rows from the other DataFrame.
	for j := 0; j < other.Rows; j++ {
		appendRow(other, j)
	}

	// Total rows is the sum of both dataframes' row counts.
	nRows := df.Rows + other.Rows

	return &DataFrame{
		Cols: newCols,
		Data: newData,
		Rows: nRows,
	}
}

// Drop removes the specified columns from the DataFrame.
func (df *DataFrame) Drop(columns ...string) *DataFrame {
	// Create a set for quick lookup of columns to drop.
	dropSet := make(map[string]bool)
	for _, col := range columns {
		dropSet[col] = true
	}

	// Build new column slice and data map containing only non-dropped columns.
	newCols := []string{}
	newData := make(map[string][]interface{})
	for _, col := range df.Cols {
		if !dropSet[col] {
			newCols = append(newCols, col)
			newData[col] = df.Data[col]
		}
	}

	// Update DataFrame.
	df.Cols = newCols
	df.Data = newData

	return df
}

// OrderBy sorts the DataFrame by the specified column.
// If asc is true, the sort is in ascending order; otherwise, descending.
// It returns a pointer to the modified DataFrame.
func (df *DataFrame) OrderBy(column string, asc bool) *DataFrame {
	// Check that the column exists.
	colData, ok := df.Data[column]
	if !ok {
		fmt.Printf("column %q does not exist\n", column)
		return df
	}

	// Build a slice of row indices.
	indices := make([]int, df.Rows)
	for i := 0; i < df.Rows; i++ {
		indices[i] = i
	}

	// Sort the indices based on the values in the target column.
	sort.Slice(indices, func(i, j int) bool {
		a := colData[indices[i]]
		b := colData[indices[j]]

		// Attempt type assertion for strings.
		aStr, aOk := a.(string)
		bStr, bOk := b.(string)
		if aOk && bOk {
			if asc {
				return aStr < bStr
			}
			return aStr > bStr
		}

		// Try converting to float64.
		aFloat, errA := toFloat64(a)
		bFloat, errB := toFloat64(b)
		if errA != nil || errB != nil {
			// Fallback to string comparison if conversion fails.
			aFallback := fmt.Sprintf("%v", a)
			bFallback := fmt.Sprintf("%v", b)
			if asc {
				return aFallback < bFallback
			}
			return aFallback > bFallback
		}

		if asc {
			return aFloat < bFloat
		}
		return aFloat > bFloat
	})

	// Reorder each column according to the sorted indices.
	newData := make(map[string][]interface{})
	for _, col := range df.Cols {
		origVals := df.Data[col]
		sortedVals := make([]interface{}, df.Rows)
		for i, idx := range indices {
			sortedVals[i] = origVals[idx]
		}
		newData[col] = sortedVals
	}

	// Update the DataFrame.
	df.Data = newData

	return df
}
