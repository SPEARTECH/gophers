package main

/*
#include <stdlib.h>
*/
import (
	"C"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
)
import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"os/exec"
	"runtime"
)

// DataFrame represents a very simple dataframe structure.
type DataFrame struct {
	Cols []string
	Data map[string][]interface{}
	Rows int
}

// ColumnFunc is a function type that takes a row and returns a value.
// type Column func(row map[string]interface{}) interface{}
// Column represents a column in the DataFrame.
type Column struct {
	Name string
	Fn   func(row map[string]interface{}) interface{}
}

// SOURCES --------------------------------------------------

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

//export ReadCSV
func ReadCSV(csvFile *C.char) *C.char {
	goCsvFile := C.GoString(csvFile)
	if fileExists(goCsvFile) {
		bytes, err := os.ReadFile(goCsvFile)
		if err != nil {
			fmt.Println(err)
		}
		goCsvFile = string(bytes)
	}

	file, err := os.Open(goCsvFile)
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

	df := Dataframe(rows)
	jsonBytes, err := json.Marshal(df)
	if err != nil {
		log.Fatalf("Error marshalling DataFrame to JSON: %v", err)
	}

	return C.CString(string(jsonBytes))
}

//export ReadJSON
func ReadJSON(jsonStr *C.char) *C.char {
	goJsonStr := C.GoString(jsonStr)
	if fileExists(goJsonStr) {
		bytes, err := os.ReadFile(goJsonStr)
		if err != nil {
			fmt.Println(err)
		}
		goJsonStr = string(bytes)
	}

	var rows []map[string]interface{}
	if err := json.Unmarshal([]byte(goJsonStr), &rows); err != nil {
		log.Fatal("Error unmarshalling JSON:", err)
	}

	df := Dataframe(rows)
	jsonBytes, err := json.Marshal(df)
	if err != nil {
		log.Fatalf("Error marshalling DataFrame to JSON: %v", err)
	}

	return C.CString(string(jsonBytes))
}

//export ReadNDJSON
func ReadNDJSON(jsonStr *C.char) *C.char {
	goJsonStr := C.GoString(jsonStr)
	if fileExists(goJsonStr) {
		bytes, err := os.ReadFile(goJsonStr)
		if err != nil {
			fmt.Println(err)
		}
		goJsonStr = string(bytes)
	}

	var rows []map[string]interface{}

	lines := strings.Split(goJsonStr, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		var row map[string]interface{}
		if err := json.Unmarshal([]byte(trimmed), &row); err != nil {
			log.Fatalf("Error unmarshalling JSON on line %d: %v", i+1, err)
		}
		rows = append(rows, row)
	}

	df := Dataframe(rows)
	jsonBytes, err := json.Marshal(df)
	if err != nil {
		log.Fatalf("Error marshalling DataFrame to JSON: %v", err)
	}

	return C.CString(string(jsonBytes))
}

// ReadParquetWrapper is a c-shared exported function that wraps ReadParquet.
// It accepts a C string representing the path (or content) of a parquet file,
// calls ReadParquet, marshals the resulting DataFrame back to JSON, and returns it as a C string.
//
//export ReadParquetWrapper
func ReadParquetWrapper(parquetPath *C.char) *C.char {
	goPath := C.GoString(parquetPath)
	df := ReadParquet(goPath)
	jsonBytes, err := json.Marshal(df)
	if err != nil {
		log.Fatalf("ReadParquetWrapper: error marshalling DataFrame: %v", err)
	}
	return C.CString(string(jsonBytes))
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

//export GetAPIJSON
func GetAPIJSON(endpoint *C.char, headers *C.char, queryParams *C.char) *C.char {
	goEndpoint := C.GoString(endpoint)
	goHeaders := C.GoString(headers)
	goQueryParams := C.GoString(queryParams)

	parsedURL, err := url.Parse(goEndpoint)
	if err != nil {
		log.Fatalf("failed to parse endpoint url: %v", err)
	}

	q := parsedURL.Query()
	for _, param := range strings.Split(goQueryParams, "&") {
		parts := strings.SplitN(param, "=", 2)
		if len(parts) == 2 {
			q.Add(parts[0], parts[1])
		}
	}
	parsedURL.RawQuery = q.Encode()

	req, err := http.NewRequest("GET", parsedURL.String(), nil)
	if err != nil {
		log.Fatalf("failed to create request: %v", err)
	}

	for _, header := range strings.Split(goHeaders, "\n") {
		parts := strings.SplitN(header, ":", 2)
		if len(parts) == 2 {
			req.Header.Set(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
		}
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("failed to execute request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("bad status: %s", resp.Status)
	}

	jsonBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("failed to read response: %v", err)
	}

	var result interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		log.Fatalf("Error unmarshalling JSON: %v\n", err)
	}

	jsonStr, err := json.Marshal(result)
	if err != nil {
		log.Fatalf("Error re-marshalling JSON: %v", err)
	}

	return ReadJSON(C.CString(string(jsonStr)))
}

// DISPLAYS --------------------------------------------------

// Print displays the DataFrame in a simple tabular format.
//
//export Show
func Show(dfJson *C.char, chars C.int, record_count C.int) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("Error unmarshalling DataFrame JSON: %v", err)
	}

	// Use the lesser of record_count and df.Rows.
	var records int
	if record_count > 0 && int(record_count) < df.Rows {
		records = int(record_count)
	} else {
		records = df.Rows
	}

	if chars <= 0 {
		chars = 25
	} else if chars < 5 {
		chars = 5
	}

	var builder strings.Builder

	// Print column headers.
	for _, col := range df.Cols {
		if len(col) > int(chars) {
			builder.WriteString(fmt.Sprintf("%-15s", col[:chars-3]+"..."))
		} else {
			builder.WriteString(fmt.Sprintf("%-15s", col))
		}
	}
	builder.WriteString("\n")

	// Print each row.
	for i := 0; i < records; i++ {
		for _, col := range df.Cols {
			if i >= len(df.Data[col]) {
				log.Fatalf("Index out of range: row %d, column %s", i, col)
			}
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

			if len(strvalue) > int(chars) {
				builder.WriteString(fmt.Sprintf("%-15v", strvalue[:chars-3]+"..."))
			} else {
				builder.WriteString(fmt.Sprintf("%-15v", strvalue))
			}
		}
		builder.WriteString("\n")
	}

	return C.CString(builder.String())
}

//export Head
func Head(dfJson *C.char, chars C.int) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("Error unmarshalling DataFrame JSON in Head: %v", err)
	}

	// Show top 5 rows (or fewer if less available)
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

	var builder bytes.Buffer

	// Print headers
	for _, col := range df.Cols {
		if len(col) >= int(chars) {
			builder.WriteString(fmt.Sprintf("%-15s", col[:int(chars)-3]+"..."))
		} else {
			builder.WriteString(fmt.Sprintf("%-15s", col))
		}
	}
	builder.WriteString("\n")

	// Print each row of top records.
	for i := 0; i < records; i++ {
		for _, col := range df.Cols {
			if i >= len(df.Data[col]) {
				log.Fatalf("Index out of range in Head: row %d, column %s", i, col)
			}
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
			if len(strvalue) > int(chars) {
				builder.WriteString(fmt.Sprintf("%-15v", strvalue[:int(chars)-3]+"..."))
			} else {
				builder.WriteString(fmt.Sprintf("%-15v", strvalue))
			}
		}
		builder.WriteString("\n")
	}

	return C.CString(builder.String())
}

//export Tail
func Tail(dfJson *C.char, chars C.int) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("Error unmarshalling DataFrame JSON in Tail: %v", err)
	}

	// Show bottom 5 rows, or fewer if df.Rows < 5.
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

	var builder bytes.Buffer

	// Print headers.
	for _, col := range df.Cols {
		if len(col) >= int(chars) {
			builder.WriteString(fmt.Sprintf("%-15s", col[:int(chars)-3]+"..."))
		} else {
			builder.WriteString(fmt.Sprintf("%-15s", col))
		}
	}
	builder.WriteString("\n")

	// Print each row of the bottom records.
	start := df.Rows - records
	for i := start; i < df.Rows; i++ {
		for _, col := range df.Cols {
			if i >= len(df.Data[col]) {
				log.Fatalf("Index out of range in Tail: row %d, column %s", i, col)
			}
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
			if len(strvalue) > int(chars) {
				builder.WriteString(fmt.Sprintf("%-15v", strvalue[:int(chars)-3]+"..."))
			} else {
				builder.WriteString(fmt.Sprintf("%-15v", strvalue))
			}
		}
		builder.WriteString("\n")
	}

	return C.CString(builder.String())
}

//export Vertical
func Vertical(dfJson *C.char, chars C.int, record_count C.int) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("Error unmarshalling DataFrame JSON in Vertical: %v", err)
	}

	var records int
	if record_count > 0 && int(record_count) < df.Rows {
		records = int(record_count)
	} else {
		records = df.Rows
	}
	if chars <= 0 {
		chars = 25
	}

	var builder bytes.Buffer
	count := 0

	// For vertical display, iterate through records up to records
	for count < df.Rows && count < records {
		builder.WriteString(fmt.Sprintf("------------ Record %d ------------\n", count))
		// Determine maximum header length for spacing
		maxLen := 0
		for _, col := range df.Cols {
			if len(col) > maxLen {
				maxLen = len(col)
			}
		}

		for _, col := range df.Cols {
			values, exists := df.Data[col]
			if !exists {
				builder.WriteString(fmt.Sprintf("Column not found: %s\n", col))
				continue
			}
			if count < len(values) {
				var item1 string
				if len(col) > int(chars) {
					item1 = col[:int(chars)-3] + "..."
				} else {
					item1 = col
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
				if len(item2) > int(chars) {
					item2 = item2[:int(chars)]
				}
				// You can adjust spacing if desired. Here we use a tab.
				builder.WriteString(fmt.Sprintf("%s:\t%s\n", item1, item2))
			}
		}
		builder.WriteString("\n")
		count++
	}

	return C.CString(builder.String())
}

// DisplayBrowserWrapper is an exported function that wraps the DisplayBrowser method.
// It takes a JSON-string representing the DataFrame, calls DisplayBrowser, and
// returns an empty string on success or an error message on failure.
//
//export DisplayBrowserWrapper
func DisplayBrowserWrapper(dfJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("DisplayBrowserWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	if err := df.DisplayBrowser(); err != nil {
		errStr := fmt.Sprintf("DisplayBrowserWrapper: error displaying in browser: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	// Return an empty string to denote success.
	return C.CString("")
}

// QuoteArray returns a string representation of a Go array with quotes around the values.
func QuoteArray(arr []string) string {
	quoted := make([]string, len(arr))
	for i, v := range arr {
		quoted[i] = fmt.Sprintf("%q", v)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// mapToString converts the DataFrame data to a JSON-like string with quoted values.
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
			switch v := value.(type) {
			case int, float64, bool:
				builder.WriteString(fmt.Sprintf("%v", v))
			case string:
				builder.WriteString(fmt.Sprintf("%q", v))
			default:
				builder.WriteString(fmt.Sprintf("%q", fmt.Sprintf("%v", v)))
			}
		}
		builder.WriteString("]")
	}
	builder.WriteString("}")

	return builder.String()
}

// DisplayHTML returns a value that gophernotes recognizes as rich HTML output.
func (df *DataFrame) DisplayBrowser() error {
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
			<div id="app" style="text-align: center;" class="overflow-x-auto">
				<table class="table table-xs">
	  				<thead>
						<tr>
							<th></th>
							<th v-for="col in cols"><a class="btn btn-sm btn-ghost justify justify-start">[[ col ]]<span class="material-symbols-outlined">arrow_drop_down</span></a></th>
						</tr>
					</thead>
					<tbody>
					<tr v-for="i in Array.from({length:` + strconv.Itoa(df.Rows) + `}).keys()" :key="i">
							<th class="pl-5">[[ i ]]</th>
							<td v-for="col in cols" :key="col" class="pl-5">[[ data[col][i] ]]</td>
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
						cols: ` + QuoteArray(df.Cols) + `,
						data: ` + mapToString(df.Data) + `,
						selected_col: {},
						page: 1,
						pages: [],
						total_pages: 0
					}
				},
				methods: {

				},
				watch: {

				},
				created(){
					this.total_pages = Math.ceil(Object.keys(this.data).length / 100)
				},

				mounted() {

				},
				computed:{

				}

			}).mount('#app')
		</script>
	</html>
	`
	// Create a temporary file
	tmpFile, err := os.CreateTemp(os.TempDir(), "temp-*.html")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %v", err)
	}
	defer tmpFile.Close()

	// Write the HTML string to the temporary file
	if _, err := tmpFile.Write([]byte(html)); err != nil {
		return fmt.Errorf("failed to write to temporary file: %v", err)
	}

	// Open the temporary file in the default web browser
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("cmd", "/c", "start", tmpFile.Name())
	case "darwin":
		cmd = exec.Command("open", tmpFile.Name())
	default: // "linux", "freebsd", "openbsd", "netbsd"
		cmd = exec.Command("xdg-open", tmpFile.Name())
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to open file in browser: %v", err)
	}

	return nil
}

// FUNCTIONS --------------------------------------------------

// ColumnOp applies an operation (identified by opName) to the columns
// specified in colsJson (a JSON array of strings) and stores the result in newCol.
// The supported opName cases here are "SHA256" and "SHA512". You can add more operations as needed.
//
//export ColumnOp
func ColumnOp(dfJson *C.char, newCol *C.char, opName *C.char, colsJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("Error unmarshalling DataFrame JSON in ColumnOp: %v", err)
	}

	op := C.GoString(opName)
	var srcCols []string
	if err := json.Unmarshal([]byte(C.GoString(colsJson)), &srcCols); err != nil {
		log.Fatalf("Error unmarshalling columns JSON in ColumnOp: %v", err)
	}

	// Create a slice of Columns from the source column names.
	var compCols []Column
	for _, s := range srcCols {
		compCols = append(compCols, Col(s))
	}

	// Depending on the operation, create the Column specification.
	var colSpec Column
	switch op {
	case "SHA256":
		colSpec = SHA256(compCols...)
	case "SHA512":
		colSpec = SHA512(compCols...)
	case "Col":
		colSpec = Col(srcCols[0])
	case "Lit":
		colSpec = Lit(srcCols[0])
	case "CollectList":
		colSpec = CollectList(srcCols[0])
	case "CollectSet":
		colSpec = CollectSet(srcCols[0])
	default:
		log.Fatalf("Unsupported operation: %s", op)
	}

	newDF := df.Column(C.GoString(newCol), colSpec)
	newJSON, err := json.Marshal(newDF)
	if err != nil {
		log.Fatalf("Error marshalling new DataFrame in ColumnOp: %v", err)
	}
	return C.CString(string(newJSON))
}

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

// Col returns a Column for the specified column name.
func Col(name string) Column {
	return Column{
		Name: fmt.Sprintf("Col(%s)", name),
		Fn: func(row map[string]interface{}) interface{} {
			return row[name]
		},
	}
}

// Lit returns a Column that always returns the provided literal value.
func Lit(value interface{}) Column {
	return Column{
		Name: "lit",
		Fn: func(row map[string]interface{}) interface{} {
			return value
		},
	}
}

// SHA256 returns a Column that concatenates the values of the specified columns,
// computes the SHA-256 checksum of the concatenated string, and returns it as a string.
func SHA256(cols ...Column) Column {
	return Column{
		Name: "SHA256",
		Fn: func(row map[string]interface{}) interface{} {
			var concatenated string
			for _, col := range cols {
				val := col.Fn(row)
				str, err := toString(val)
				if err != nil {
					str = ""
				}
				concatenated += str
			}
			hash := sha256.Sum256([]byte(concatenated))
			return hex.EncodeToString(hash[:])
		},
	}
}

// SHA512 returns a Column that concatenates the values of the specified columns,
// computes the SHA-512 checksum of the concatenated string, and returns it as a string.
func SHA512(cols ...Column) Column {
	return Column{
		Name: "SHA512",
		Fn: func(row map[string]interface{}) interface{} {
			var concatenated string
			for _, col := range cols {
				val := col.Fn(row)
				str, err := toString(val)
				if err != nil {
					str = ""
				}
				concatenated += str
			}
			hash := sha512.Sum512([]byte(concatenated))
			return hex.EncodeToString(hash[:])
		},
	}
}

// ColumnCollectList applies CollectList on the specified source column
// and creates a new column.
//
//export ColumnCollectList
func ColumnCollectList(dfJson *C.char, newCol *C.char, source *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("ColumnCollectList: unmarshal error: %v", err)
	}
	newName := C.GoString(newCol)
	src := C.GoString(source)
	newDF := df.Column(newName, CollectList(src))
	newJSON, err := json.Marshal(newDF)
	if err != nil {
		log.Fatalf("ColumnCollectList: marshal error: %v", err)
	}
	return C.CString(string(newJSON))
}

// ColumnCollectSet applies CollectSet on the specified source column
// and creates a new column.
//
//export ColumnCollectSet
func ColumnCollectSet(dfJson *C.char, newCol *C.char, source *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("ColumnCollectSet: unmarshal error: %v", err)
	}
	newName := C.GoString(newCol)
	src := C.GoString(source)
	newDF := df.Column(newName, CollectSet(src))
	newJSON, err := json.Marshal(newDF)
	if err != nil {
		log.Fatalf("ColumnCollectSet: marshal error: %v", err)
	}
	return C.CString(string(newJSON))
}

// ColumnSplit applies Split on the specified source column with the given delimiter
// and creates a new column.
//
//export ColumnSplit
func ColumnSplit(dfJson *C.char, newCol *C.char, source *C.char, delim *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("ColumnSplit: unmarshal error: %v", err)
	}
	newName := C.GoString(newCol)
	src := C.GoString(source)
	delimiter := C.GoString(delim)
	newDF := df.Column(newName, Split(src, delimiter))
	newJSON, err := json.Marshal(newDF)
	if err != nil {
		log.Fatalf("ColumnSplit: marshal error: %v", err)
	}
	return C.CString(string(newJSON))
}

// CollectList returns a Column that is an array of the given column's values.
func CollectList(name string) Column {
	return Column{
		Name: name,
		Fn: func(row map[string]interface{}) interface{} {
			values := []interface{}{}
			values = append(values, row[name])

			return values
		},
	}
}

// CollectSet returns a Column that is a set of unique values from the given column.
func CollectSet(name string) Column {
	return Column{
		Name: fmt.Sprintf("CollectSet(%s)", name),
		Fn: func(row map[string]interface{}) interface{} {
			valueSet := make(map[interface{}]bool)
			for _, val := range row[name].([]interface{}) {
				valueSet[val] = true
			}
			values := []interface{}{}
			for val := range valueSet {
				values = append(values, val)
			}
			return values
		},
	}
}

// Split returns a Column that splits the string value of the specified column by the given delimiter.
func Split(name string, delimiter string) Column {
	return Column{
		Name: fmt.Sprintf("Split(%s, %s)", name, delimiter),
		Fn: func(row map[string]interface{}) interface{} {
			val := row[name]
			str, err := toString(val)
			if err != nil {
				return []string{}
			}
			return strings.Split(str, delimiter)
		},
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

// RETURNS --------------------------------------------------

// DFColumns returns the DataFrame columns as a JSON array.

//export DFColumns
func DFColumns(dfJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("DFColumns: error unmarshalling DataFrame: %v", err)
	}
	cols := df.Columns()
	colsJSON, err := json.Marshal(cols)
	if err != nil {
		log.Fatalf("DFColumns: error marshalling columns: %v", err)
	}
	return C.CString(string(colsJSON))
}

// DFCount returns the number of rows in the DataFrame.
//
//export DFCount
func DFCount(dfJson *C.char) C.int {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("DFCount: error unmarshalling DataFrame: %v", err)
	}
	return C.int(df.Count())
}

// DFCountDuplicates returns the count of duplicate rows.
// It accepts a JSON array of column names (or an empty array to use all columns).
//
//export DFCountDuplicates
func DFCountDuplicates(dfJson *C.char, colsJson *C.char) C.int {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("DFCountDuplicates: error unmarshalling DataFrame: %v", err)
	}

	var cols []string
	if err := json.Unmarshal([]byte(C.GoString(colsJson)), &cols); err != nil {
		// if not provided or invalid, use all columns
		cols = df.Cols
	}
	dups := df.CountDuplicates(cols...)
	return C.int(dups)
}

// DFCountDistinct returns the count of unique rows (or unique values in the provided columns).
// Accepts a JSON array of column names (or an empty array to use all columns).
//
//export DFCountDistinct
func DFCountDistinct(dfJson *C.char, colsJson *C.char) C.int {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("DFCountDistinct: error unmarshalling DataFrame: %v", err)
	}

	var cols []string
	if err := json.Unmarshal([]byte(C.GoString(colsJson)), &cols); err != nil {
		cols = df.Cols
	}
	distinct := df.CountDistinct(cols...)
	return C.int(distinct)
}

// DFCollect returns the collected values from a specified column as a JSON-array.
//
//export DFCollect
func DFCollect(dfJson *C.char, colName *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("DFCollect: error unmarshalling DataFrame: %v", err)
	}
	col := C.GoString(colName)
	collected := df.Collect(col)
	result, err := json.Marshal(collected)
	if err != nil {
		log.Fatalf("DFCollect: error marshalling collected values: %v", err)
	}
	return C.CString(string(result))
}

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

func main() {}
