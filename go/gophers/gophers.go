package gophers

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
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

func mapToString(data map[string][]interface{}) string {
	var builder strings.Builder

	builder.WriteString("{")
	first := true
	for key, values := range data {
		if !first {
			builder.WriteString(", ")
		}
		first = false

		builder.WriteString(fmt.Sprintf("%q: [", key))
		for i, value := range values {
			if i > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(fmt.Sprintf("%v", value))
		}
		builder.WriteString("]")
	}
	builder.WriteString("}")

	return builder.String()
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

// Print displays the DataFrame in a simple tabular format.
func (df *DataFrame) Show(chars int, record_count ...int) {
	var records int
	if len(record_count) > 0 {
		records = record_count[0]
	} else {
		records = df.Rows
	}
	if chars <= 0 {
		chars = 25
	} else if chars < 5 {
		chars = 5
	}

	for _, col := range df.Cols {
		if len(col) >= chars {
			fmt.Printf("%-15s", col[:chars-3]+"...")
		} else {
			fmt.Printf("%-15s", col)
		}
	}
	fmt.Println()

	// Print each row.
	for i := 0; i < records; i++ {
		for _, col := range df.Cols {
			value := df.Data[col][i]
			var strvalue string
			switch v := value.(type) {
			case int:
				strvalue = strconv.Itoa(v)
			case float64:
				strvalue = strconv.FormatFloat(v, 'f', 2, 64)
			case bool:
				strvalue = strconv.FormatBool(v)
			case string:
				strvalue = v
			default:
				strvalue = fmt.Sprintf("%v", v)
			}

			if len(strvalue) > chars {
				fmt.Printf("%-15v", strvalue[:chars-3]+"...")
			} else {
				fmt.Printf("%-15v", strvalue)
			}
		}
		fmt.Println()
	}
}

func (df *DataFrame) Head(chars int) {
	var records int
	if df.Rows < 5 {
		records = df.Rows
	} else {
		records = 5
	}
	if chars <= 0 {
		chars = 25
	} else if chars < 5 {
		chars = 5
	}

	for _, col := range df.Cols {
		if len(col) >= chars {
			fmt.Printf("%-15s", col[:chars-3]+"...")
		} else {
			fmt.Printf("%-15s", col)
		}
	}
	fmt.Println()

	// Print each row.
	for i := 0; i < records; i++ {
		for _, col := range df.Cols {
			value := df.Data[col][i]
			var strvalue string
			switch v := value.(type) {
			case int:
				strvalue = strconv.Itoa(v)
			case float64:
				strvalue = strconv.FormatFloat(v, 'f', 2, 64)
			case bool:
				strvalue = strconv.FormatBool(v)
			case string:
				strvalue = v
			default:
				strvalue = fmt.Sprintf("%v", v)
			}

			if len(strvalue) > chars {
				fmt.Printf("%-15v", strvalue[:chars-3]+"...")
			} else {
				fmt.Printf("%-15v", strvalue)
			}
		}
		fmt.Println()
	}
}

func (df *DataFrame) Tail(chars int) {
	var records int
	if df.Rows < 5 {
		records = df.Rows
	} else {
		records = 5
	}
	if chars <= 0 {
		chars = 25
	} else if chars < 5 {
		chars = 5
	}

	for _, col := range df.Cols {
		if len(col) >= chars {
			fmt.Printf("%-15s", col[:chars-3]+"...")
		} else {
			fmt.Printf("%-15s", col)
		}
	}
	fmt.Println()

	// Print each row.
	for i := df.Rows - records; i < df.Rows; i++ {
		for _, col := range df.Cols {
			value := df.Data[col][i]
			var strvalue string
			switch v := value.(type) {
			case int:
				strvalue = strconv.Itoa(v)
			case float64:
				strvalue = strconv.FormatFloat(v, 'f', 2, 64)
			case bool:
				strvalue = strconv.FormatBool(v)
			case string:
				strvalue = v
			default:
				strvalue = fmt.Sprintf("%v", v)
			}

			if len(strvalue) > chars {
				fmt.Printf("%-15v", strvalue[:chars-3]+"...")
			} else {
				fmt.Printf("%-15v", strvalue)
			}
		}
		fmt.Println()
	}

}

func (df *DataFrame) Vertical(chars int, record_count ...int) {
	var records int
	if len(record_count) > 0 {
		records = record_count[0]
	} else {
		records = df.Rows
	}
	if chars <= 0 {
		chars = 25
	}
	count := 0
	max_len := 0
	for count < df.Rows && count < records {
		fmt.Println("------------", "Record", count, "------------")
		for _, col := range df.Cols {
			if len(col) > max_len {
				max_len = len(col)
			}
		}
		for _, col := range df.Cols {
			values, exists := df.Data[col]
			if !exists {
				fmt.Println("Column not found:", col)
				continue
			}

			if count < len(values) {
				var item1 string
				if chars >= len(col) {
					item1 = col
				} else {
					item1 = fmt.Sprint(col[:chars-3], "...")
				}
				var item2 string
				switch v := values[count].(type) {
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
				if chars < len(item2) {
					item2 = item2[:chars]
				}
				space := "\t"
				var num int
				num = (max_len - len(item1)) / 5
				if num > 0 {
					for i := 0; i < num; i++ { //fix math
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

// dataframe example function used with pyspark flattener (all available values)

func (df *DataFrame) Columns() []string {
	return df.Cols
}

// DisplayHTML returns a value that gophernotes recognizes as rich HTML output.
func DisplayHTML(html string) map[string]interface{} {
	return map[string]interface{}{
		"text/html": html,
	}
}

func (df *DataFrame) Display() map[string]interface{} {
	// display an html table of the dataframe for analysis, filtering, sorting, etc
	html := `
<!DOCTYPE html>
<html>
	<head>
		<script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
		<link href="https://cdn.jsdelivr.net/npm/daisyui@4.7.2/dist/full.min.css" rel="stylesheet" type="text/css" />
		<script src="https://cdn.tailwindcss.com"></script>
		<script src="https://code.highcharts.com/highcharts.js"></script>
		<script src="https://code.highcharts.com/modules/boost.js"></script>
		<script src="https://code.highcharts.com/modules/exporting.js"></script>
		<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200" />
		<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, minimal-ui">
	</head>
	<body>
		<div id="app" style="text-align: center;">
			<table>
				<thead>
					<tr>
						<th v-for="col in cols">[[ col ]]</th>
					</tr>
				</thead>
				<tbody>
					<tr v-for="i = 0; i < range(` + strconv.Itoa(df.Rows) + `); i++">
						<th>[[ i ]]</th>
						<td> v-for="col in cols">[[ data[col][i] ]]</td>
					</tr>
				</tbody>
			</table>
		</div>
	</body>
	<script>
		const { createApp } = Vue
		createApp({
		delimiters : ['[[', ']]'],
			data(){
				return {
					cols: ` + strings.Join(df.Cols, ",") + `
					data: ` + mapToString(df.Data) + `,
				}
			},
			methods: {

			},
			watch: {

			},
			created(){

			},

			mounted() {

			},
			computed:{

			}

		}).mount('#app')
	</script>
</html>	
`
	return map[string]interface{}{
		"text/html": html,
	}
}

// dataframe to csv
func (df *DataFrame) ToCSV(filename string) error {
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

// dataframe to json
func (df *DataFrame) ToJSON(filename string) error {
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

// dataframe to ndjson
func (df *DataFrame) ToNDJSON(filename string) error {
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
func (df *DataFrame) ToParquet(filename string) error {
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

// dataframe to json

// schema of json ?

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

// withColumn
//

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

// dropduplicates

// concat

// concat_ws

// filter

// explode

// cast

// groupby

// agg

// orderby

// limit

// join

//
