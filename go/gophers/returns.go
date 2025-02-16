package gophers

import "encoding/json"

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

func (df *DataFrame) Columns() []string {
	return df.Cols
}

// dataframe example function used with pyspark flattener (all available values)

// schema of json ?

// count
func (df *DataFrame) Count() int {
	return df.Rows
}

// CountDuplicates returns the count of duplicate rows in the DataFrame.
func (df *DataFrame) CountDuplicates() int {
	seen := make(map[string]bool)
	duplicateCount := 0

	for i := 0; i < df.Rows; i++ {
		row := make(map[string]interface{})
		for _, col := range df.Cols {
			row[col] = df.Data[col][i]
		}

		// Convert the row to a JSON string to use as a key in the map.
		rowBytes, _ := json.Marshal(row)
		rowStr := string(rowBytes)

		if seen[rowStr] {
			duplicateCount++
		} else {
			seen[rowStr] = true
		}
	}

	return duplicateCount
}
