package gophers

import (
	"encoding/json"
	"log"
	"strings"
	"fmt"
)
// DataFrame represents a very simple dataframe structure.
type DataFrame struct {
	Columns []string
	Data    map[string][]interface{}
	Rows    int
}

// Create dataframe function
func DATAFRAME(rows []map[string]interface{}) *DataFrame{
	df := &DataFrame{
		Data: make(map[string][]interface{}),
		Rows: len(rows),
	}

	// Collect unique column names.
	columnsSet := make(map[string]bool)
	for _, row := range rows {
		for key := range row {
			columnsSet[key] = true
		}
	}
	// Build a slice of column names (order is arbitrary).
	for col := range columnsSet {
		df.Columns = append(df.Columns, col)
	}

	// Initialize each column with a slice sized to the number of rows.
	for _, col := range df.Columns {
		df.Data[col] = make([]interface{}, df.Rows)
	}

	// Fill the DataFrame with data.
	for i, row := range rows {
		for _, col := range df.Columns {
			val, ok := row[col]
			if ok {
				// Example conversion:
				// JSON unmarshals numbers as float64 by default.
				// If the float64 value is a whole number, convert it to int.
				if f, isFloat := val.(float64); isFloat {
					if f == float64(int(f)) {
						val = int(f)
					}
				}
				df.Data[col][i] = val
			} else {
				// If a column is missing in a row, set it to nil.
				df.Data[col][i] = nil
			}
		}
	}
	return df
}

// Functions for intaking data and returning dataframe
// Read csv and output dataframe
func READCSV(csv string) map[string]string {
	df := map[string]string{}
	return df
}
// Read json and output dataframe
func READJSON(jsonStr string) *DataFrame{
	// Unmarshal the JSON into a slice of maps.
	var rows []map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &rows); err != nil {
		log.Fatal("Error unmarshalling JSON:", err)
	}
	
	return DATAFRAME(rows)
}
// Read newline deliniated json and output dataframe
func READNDJSON(jsonStr string) *DataFrame {
	var rows []map[string]interface{}

	// Split the string by newline.
	lines := strings.Split(jsonStr, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			// Skip empty lines.
			continue
		}

		var row map[string]interface{}
		if err := json.Unmarshal([]byte(trimmed), &row); err != nil {
			log.Fatalf("Error unmarshalling JSON on line %d: %v", i+1, err)
		}
		rows = append(rows, row)
	}

	return DATAFRAME(rows)
}
// Read parquet and output dataframe
func READPARQUET(jsonStr string) map[string]string {
	df := map[string]string{}
	return df
}

// Print displays the DataFrame in a simple tabular format.
func (df *DataFrame) SHOW() {
	// Print header.
	for _, col := range df.Columns {
		fmt.Printf("%-15s", col)
	}
	fmt.Println()

	// Print each row.
	for i := 0; i < df.Rows; i++ {
		for _, col := range df.Columns {
			fmt.Printf("%-15v", df.Data[col][i])
		}
		fmt.Println()
	}
}

func (df *DataFrame) HEAD(){
	// Print header.
	for _, col := range df.Columns {
		fmt.Printf("%-15s", col)
	}
	fmt.Println()

	// Print each row.
	for i := 0; i < 5 && i < df.Rows; i++ {
		for _, col := range df.Columns {
			fmt.Printf("%-15v", df.Data[col][i])
		}
		fmt.Println()
	}
}

func (df *DataFrame) TAIL(){
	// Print header.
	for _, col := range df.Columns {
		fmt.Printf("%-15s", col)
	}
	fmt.Println()

	// Print each row.
	for i := 0; i < df.Rows && i > df.Rows - 5 ; i++ {
		for _, col := range df.Columns {
			fmt.Printf("%-15v", df.Data[col][i])
		}
		fmt.Println()
	}
}

func (df *DataFrame) COLUMNS() []string {
	return df.Columns
}

// display()
