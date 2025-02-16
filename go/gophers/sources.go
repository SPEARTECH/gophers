package gophers

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	// "github.com/xitongsys/parquet-go/ParquetFile"
	// "github.com/xitongsys/parquet-go/Writer"
)

// DataFrame represents a very simple dataframe structure.
type DataFrame struct {
	Cols []string
	Data map[string][]interface{}
	Rows int
}

// Create dataframe function
func Dataframe(rows []map[string]interface{}) *DataFrame {
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
		df.Cols = append(df.Cols, col)
	}

	// Initialize each column with a slice sized to the number of rows.
	for _, col := range df.Cols {
		df.Data[col] = make([]interface{}, df.Rows)
	}

	// Fill the DataFrame with data.
	for i, row := range rows {
		for _, col := range df.Cols {
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
				// if s, isString := val.(string); isString {
				// 	val = fmt.Sprintf("%q", s)
				// }
				// fmt.Println(val)
				// if val == "" {
				// 	val = "-"
				// }
				df.Data[col][i] = val
			} else {
				// If a column is missing in a row, set it to nil.
				df.Data[col][i] = nil
			}
		}
	}
	return df
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// Functions for intaking data and returning dataframe
// Read csv and output dataframe
func ReadCSV(csvFile string) *DataFrame {
	if fileExists(csvFile) {
		bytes, err := os.ReadFile(csvFile)
		if err != nil {
			fmt.Println(err)
		}
		csvFile = string(bytes)
	}

	file, err := os.Open(csvFile)
	if err != nil {
		log.Fatalf("Error opening CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		log.Fatalf("Error reading CSV headers: %v", err)
	}

	var rows []map[string]interface{}
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		row := make(map[string]interface{})
		for i, header := range headers {
			row[header] = record[i]
		}
		rows = append(rows, row)
	}

	return Dataframe(rows)
}

// Read json and output dataframe
func ReadJSON(jsonStr string) *DataFrame {
	if fileExists(jsonStr) {
		bytes, err := os.ReadFile(jsonStr)
		if err != nil {
			fmt.Println(err)
		}
		jsonStr = string(bytes)
	}
	fmt.Println(jsonStr)
	// Unmarshal the JSON into a slice of maps.
	var rows []map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &rows); err != nil {
		log.Fatal("Error unmarshalling JSON:", err)
	}

	return Dataframe(rows)
}

// Read newline deliniated json and output dataframe
func ReadNDJSON(jsonStr string) *DataFrame {
	if fileExists(jsonStr) {
		bytes, err := os.ReadFile(jsonStr)
		if err != nil {
			fmt.Println(err)
		}
		jsonStr = string(bytes)
	}
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

	return Dataframe(rows)
}

// Read parquet and output dataframe
func ReadParquet(jsonStr string) *DataFrame {
	if fileExists(jsonStr) {
		bytes, err := os.ReadFile(jsonStr)
		if err != nil {
			fmt.Println(err)
		}
		jsonStr = string(bytes)
	}

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

	return Dataframe(rows)

}

// read delta table?

// read iceberg table?

//
