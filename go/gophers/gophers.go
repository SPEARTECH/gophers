package gophers

import (
	"encoding/json"
	"log"
	"strings"
	"fmt"
	"strconv"

)

// DataFrame represents a very simple dataframe structure.
type DataFrame struct {
	Cols    []string
	Data    map[string][]interface{}
	Rows    int
}

// Create dataframe function
func Dataframe(rows []map[string]interface{}) *DataFrame{
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
func ReadCSV(csv string) map[string]string {
	df := map[string]string{}
	return df
}
// Read json and output dataframe
func ReadJSON(jsonStr string) *DataFrame{
	// Unmarshal the JSON into a slice of maps.
	var rows []map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &rows); err != nil {
		log.Fatal("Error unmarshalling JSON:", err)
	}
	
	return Dataframe(rows)
}
// Read newline deliniated json and output dataframe
func ReadNDJSON(jsonStr string) *DataFrame {
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
func read_parquet(jsonStr string) map[string]string {
	df := map[string]string{}
	return df
}

// Print displays the DataFrame in a simple tabular format.
func (df *DataFrame) Show(chars int, record_count ...int) {
	var records int
	if len(record_count) > 0{
		records = record_count[0]
	} else {
		records = df.Rows
	}
	if chars <= 0{
		chars = 25
	}

	for _, col := range df.Cols {
		if len(col) >= chars{
			fmt.Printf("%-15s", col[:chars-3],"...")
		} else {
			fmt.Printf("%-15s", col)
		}
	}
	fmt.Println()

	// Print each row.
	for i := 0; i < df.Rows; i++ {
		for _, col := range df.Cols {
			if len(df.Data[col][i]) >= chars{
				fmt.Printf("%-15v", df.Data[col][i][:chars-3], "...")
			} else {
				fmt.Printf("%-15v", df.Data[col][i])
			}
		}
		fmt.Println()
	}
}

func (df *DataFrame) Head(chars int){
	// Print header.
	for _, col := range df.Cols {
		fmt.Printf("%-15s", col)
	}
	fmt.Println()

	// Print each row.
	for i := 0; i < 5 && i < df.Rows; i++ {
		for _, col := range df.Cols {
			fmt.Printf("%-15v", df.Data[col][i])
		}
		fmt.Println()
	}
}

func (df *DataFrame) Tail(chars_count ...int){
	var chars int
	if len(chars_count) > 0{
		chars = chars_count[0]
	} else {
		chars = 5
	}
	if chars <= 0{
		chars = 5
	}

	for _, col := range df.Cols {
		fmt.Printf("%-15s", col)
	}
	fmt.Println()

	// Print each row.
	for i := 0; i < df.Rows && i > df.Rows - 5 ; i++ {
		for _, col := range df.Cols {
			fmt.Printf("%-15v", df.Data[col][i])
		}
		fmt.Println()
	}
}

func (df *DataFrame) Vertical(chars int, record_count ...int){
	var records int
	if len(record_count) > 0{
		records = record_count[0]
	} else {
		records = df.Rows
	}
	if chars <= 0{
		chars = 25
	}
	count := 0
	max_len := 0
	for count < df.Rows && count < records{
		fmt.Println("------------", "Record", count, "------------")
		for _, col := range df.Cols{
			if len(col) > max_len{
				max_len = len(col)
			}
		}
		for _, col := range df.Cols{
			values, exists := df.Data[col]
			if !exists{
				fmt.Println("Column not found:",col)
				continue
			}
			
			if count < len(values){
				var item1 string
				if chars >= len(col){
					item1 = col
				} else {
					item1 = fmt.Sprint(col[:chars-3],"...")
				}
				var item2 string
				switch v := values[count].(type){
				case int: 
					item2 = strconv.Itoa(v)
				case float64:
					item2 = strconv.FormatFloat(v, 'f', 2, 64)
				case bool:
					item2 = strconv.FormatBool(v)
				case string:
					item2 = v
				default:
					item2 = fmt.Sprintf("%v", v)
				}
				if chars < len(item2){
					item2 = item2[:chars]
				}	
				space := "\t"
				var num int
				num = (max_len - len(item1))/5
				if num > 0{
					for i := 0; i < num; i++{ //fix math
						// if 
						space += "\t"
					}
				}
				fmt.Println(item1, space, item2)
			}
		}
		count++
	}
}

func (df *DataFrame) Columns() []string {
	return df.Cols
}


// DisplayHTML returns a value that gophernotes recognizes as rich HTML output.
func DisplayHTML(html string) map[string]interface{} {
	return map[string]interface{}{
		"text/html": html,
	}
}
