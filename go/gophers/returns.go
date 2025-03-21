package gophers

import "encoding/json"

func (df *DataFrame) Columns() []string {
	return df.Cols
}

// schema of json ?

// count
func (df *DataFrame) Count() int {
	return df.Rows
}

// CountDuplicates returns the count of duplicate rows in the DataFrame.
// If one or more columns are provided, only those columns are used to determine uniqueness.
// If no columns are provided, the entire row (all columns) is used.
func (df *DataFrame) CountDuplicates(columns ...string) int {
	// If no columns are specified, use all columns.
	uniqueCols := columns
	if len(uniqueCols) == 0 {
		uniqueCols = df.Cols
	}

	seen := make(map[string]bool)
	duplicateCount := 0

	for i := 0; i < df.Rows; i++ {
		// Build a subset row only with the uniqueCols.
		rowSubset := make(map[string]interface{})
		for _, col := range uniqueCols {
			rowSubset[col] = df.Data[col][i]
		}

		// Convert the subset row to a JSON string to use as a key.
		rowBytes, _ := json.Marshal(rowSubset)
		rowStr := string(rowBytes)

		if seen[rowStr] {
			duplicateCount++
		} else {
			seen[rowStr] = true
		}
	}

	return duplicateCount
}

// CountDistinct returns the count of unique values in given column(s)
func (df *DataFrame) CountDistinct(columns ...string) int {
	newDF := &DataFrame{
		Cols: columns,
		Data: make(map[string][]interface{}),
		Rows: df.Rows,
	}
	for _, col := range newDF.Cols {
		if data, exists := df.Data[col]; exists {
			newDF.Data[col] = data
		} else {
			newDF.Data[col] = make([]interface{}, df.Rows)
		}
	}
	dups := newDF.CountDuplicates()
	count := newDF.Rows - dups

	return count
}

func (df *DataFrame) Collect(c string) []interface{} {
	if values, exists := df.Data[c]; exists {
		return values
	}
	return []interface{}{}
}
