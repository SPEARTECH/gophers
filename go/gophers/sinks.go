package gophers

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
)

// dataframe to csv file
func (df *DataFrame) ToCSVFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the column headers.
	if err := writer.Write(df.Cols); err != nil {
		return err
	}

	// Write the rows of data.
	for i := 0; i < df.Rows; i++ {
		row := make([]string, len(df.Cols))
		for j, col := range df.Cols {
			value := df.Data[col][i]
			row[j] = fmt.Sprintf("%v", value)
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

// dataframe to json file
func (df *DataFrame) ToJSONFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a slice of maps to hold the rows of data.
	rows := make([]map[string]interface{}, df.Rows)
	for i := 0; i < df.Rows; i++ {
		row := make(map[string]interface{})
		for _, col := range df.Cols {
			row[col] = df.Data[col][i]
		}
		rows[i] = row
	}

	// Marshal the rows into JSON format.
	jsonData, err := json.Marshal(rows)
	if err != nil {
		return err
	}

	// Write the JSON data to the file.
	_, err = file.Write(jsonData)
	if err != nil {
		return err
	}

	return nil
}

// dataframe to json string
func (df *DataFrame) ToJSON() string {

	// Create a slice of maps to hold the rows of data.
	rows := make([]map[string]interface{}, df.Rows)
	for i := 0; i < df.Rows; i++ {
		row := make(map[string]interface{})
		for _, col := range df.Cols {
			row[col] = df.Data[col][i]
		}
		rows[i] = row
	}

	// Marshal the rows into JSON format.
	jsonData, err := json.Marshal(rows)
	if err != nil {
		log.Fatalf("Error marshalling JSON: %v", err)
	}

	return string(jsonData)
}

// dataframe to ndjson file
func (df *DataFrame) ToNDJSONFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write each row as a separate JSON object on a new line.
	for i := 0; i < df.Rows; i++ {
		row := make(map[string]interface{})
		for _, col := range df.Cols {
			row[col] = df.Data[col][i]
		}

		// Marshal the row into JSON format.
		jsonData, err := json.Marshal(row)
		if err != nil {
			return err
		}

		// Write the JSON data to the file, followed by a newline character.
		_, err = file.Write(jsonData)
		if err != nil {
			return err
		}
		_, err = file.WriteString("\n")
		if err != nil {
			return err
		}
	}

	return nil
}

// write to parquet?
func (df *DataFrame) ToParquetFile(filename string) error {
	// fw, err := ParquetFile.NewLocalFileWriter(filename)
	// if err != nil {
	// 	return err
	// }
	// defer fw.Close()

	// pw, err := Writer.NewParquetWriter(fw, new(map[string]interface{}), 4)
	// if err != nil {
	// 	return err
	// }
	// defer pw.WriteStop()

	// for i := 0; i < df.Rows; i++ {
	// 	row := make(map[string]interface{})
	// 	for _, col := range df.Cols {
	// 		row[col] = df.Data[col][i]
	// 	}
	// 	if err := pw.Write(row); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

// write to table? (mongo, postgres, mysql, sqlite, etc)
// JDBC?
