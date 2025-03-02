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
	"sort"
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

type Chart struct {
	Htmlpreid  string
	Htmldivid  string
	Htmlpostid string
	Jspreid    string
	Jspostid   string
}

// AggregatorFn defines a function that aggregates a slice of values.
type AggregatorFn func([]interface{}) interface{}

// Aggregation holds a target column name and the aggregation function to apply.
type Aggregation struct {
    ColumnName string
    Fn         AggregatorFn
}

type SimpleAggregation struct {
	ColumnName string
}

// Dashboard object for adding html pages, charts, and inputs for a single html output
type Dashboard struct {
	Top           string
	Primary       string
	Secondary     string
	Accent        string
	Neutral       string
	Base100       string
	Info          string
	Success       string
	Warning       string
	Err           string
	Htmlheading   string
	Title         string
	Htmlelements  string
	Scriptheading string
	Scriptmiddle  string
	Bottom        string
	Pageshtml     map[string]interface{}
	Pagesjs       map[string]interface{}
}

func (dash *Dashboard) init() {
	if dash.Pageshtml == nil {
		dash.Pageshtml = make(map[string]interface{})
	}
	if dash.Pagesjs == nil {
		dash.Pagesjs = make(map[string]interface{})
	}
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
    if filename == "" {
        return false
    }
    // If the input starts with "{" or "[", assume it is JSON and not a file path.
    if strings.HasPrefix(filename, "{") || strings.HasPrefix(filename, "[") {
        return false
    }
    info, err := os.Stat(filename)
    if err != nil {
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
    if jsonStr == nil {
        log.Fatalf("Error: jsonStr is nil")
        return C.CString("")
    }

    goJsonStr := C.GoString(jsonStr)
    log.Printf("ReadJSON: Input string: %s", goJsonStr) // Log the input string

    var rows []map[string]interface{}
    var jsonContent string

    // Check if the input is a file path.
    if fileExists(goJsonStr) {
        bytes, err := os.ReadFile(goJsonStr)
        if err != nil {
            log.Fatalf("Error reading file: %v", err)
        }
        jsonContent = string(bytes)
    } else {
        jsonContent = goJsonStr
    }

    // Unmarshal the JSON string into rows.
    if err := json.Unmarshal([]byte(jsonContent), &rows); err != nil {
        log.Fatalf("Error unmarshalling JSON: %v", err)
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

// DisplayWrapper is an exported function that wraps the Display method.
// It takes a JSON-string representing the DataFrame, calls Display, and
// returns the HTML string on success or an error message on failure.
//
//export DisplayWrapper
func DisplayWrapper(dfJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("DisplayWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	displayResult := df.Display()
	html, ok := displayResult["text/html"].(string)
	if !ok {
		errStr := "DisplayWrapper: error displaying dataframe: invalid HTML content"
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(html)
}

// Display an html table of the data
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
		<div id="table" style="text-align: center;" class="overflow-x-auto">
			<table class="table">
				<thead>
					<tr>
						<th></th>
						<th v-for="col in cols">[[ col ]]</th>
					</tr>
				</thead>
				<tbody>
				<tr v-for="i in Array.from({length:` + strconv.Itoa(df.Rows) + `}).keys()" :key="i">
						<th>[[ i ]]</th>
						<td v-for="col in cols">[[ data[col][i] ]]</td>
					</tr>
				</tbody>
			</table>
		</div>
	</body>
	<script>
		const { createApp } = Vue
		createApp({
		delimiters :  ["[[", "]]"],
			data(){
				return {
					cols: ` + QuoteArray(df.Cols) + `,
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

		}).mount("#table")
	</script>
</html>	
`
	return map[string]interface{}{
		"text/html": html,
	}
}

// DisplayToFile
// DisplayToFileWrapper is an exported function that wraps the DisplayToFile method.
// It takes a JSON-string representing the DataFrame and a file path, calls DisplayToFile,
// and returns an empty string on success or an error message on failure.
//
//export DisplayToFileWrapper
func DisplayToFileWrapper(dfJson *C.char, filePath *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("DisplayToFileWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	path := C.GoString(filePath)
	if err := df.DisplayToFile(path); err != nil {
		errStr := fmt.Sprintf("DisplayToFileWrapper: error writing to file: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	// Return an empty string to denote success.
	return C.CString("")
}

// write an html display, chart, or dashboard to a file
func (df *DataFrame) DisplayToFile(path string) error {
	// Ensure the path ends with .html
	if !strings.HasSuffix(path, ".html") {
		path += ".html"
	}
	html := df.Display()["text/html"].(string)

	// Write the HTML string to the specified file path
	err := os.WriteFile(path, []byte(html), 0644)
	if err != nil {
		return fmt.Errorf("failed to write to file: %v", err)
	}

	return nil
}

// DisplayChartWrapper is an exported function that wraps the DisplayChart function.
// It takes a JSON-string representing the Chart, calls DisplayChart, and
// returns the HTML string on success or an error message on failure.
//
//export DisplayChartWrapper
func DisplayChartWrapper(chartJson *C.char) *C.char {
	var chart Chart
	if err := json.Unmarshal([]byte(C.GoString(chartJson)), &chart); err != nil {
		errStr := fmt.Sprintf("DisplayChartWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	displayChart := DisplayChart(chart)
	html, ok := displayChart["text/html"].(string)
	if !ok {
		errStr := "DisplayChartWrapper: error displaying chart"
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(html)
}
func DisplayChart(chart Chart) map[string]interface{} {
	html := chart.Htmlpreid + chart.Htmldivid + chart.Htmlpostid + chart.Jspreid + chart.Htmldivid + chart.Jspostid
	return map[string]interface{}{
		"text/html": html,
	}
}

// DisplayHTMLWrapper is an exported function that wraps the DisplayHTML function.
// It takes a string representing the HTML content and returns the HTML content as a C string.
//
//export DisplayHTMLWrapper
func DisplayHTMLWrapper(html *C.char) *C.char {
	htmlContent := C.GoString(html)
	displayHTML := DisplayHTML(htmlContent)
	htmlResult, ok := displayHTML["text/html"].(string)
	if !ok {
		errStr := "DisplayHTMLWrapper: error displaying HTML content"
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(htmlResult)
}

// DisplayHTML returns a value that gophernotes recognizes as rich HTML output.
func DisplayHTML(html string) map[string]interface{} {
	return map[string]interface{}{
		"text/html": html,
	}
}

// CHARTS --------------------------------------------------

// BarChart returns Bar Chart HTML for the DataFrame.
// It takes a title, subtitle, group column, and one or more aggregations.
func (df *DataFrame) BarChart(title string, subtitle string, groupcol string, aggs []Aggregation) Chart {
	// Group the DataFrame by the specified column and apply the aggregations.
	df = df.GroupBy(groupcol, aggs...)
	// df.Show(25)

	// Extract categories and series data.
	categories := []string{}
	for _, val := range df.Data[groupcol] {
		categories = append(categories, fmt.Sprintf("%v", val))
	}

	series := []map[string]interface{}{}
	for _, agg := range aggs {
		data := []interface{}{}
		for _, val := range df.Data[agg.ColumnName] {
			data = append(data, val)
		}
		series = append(series, map[string]interface{}{
			"name": agg.ColumnName,
			"data": data,
		})
	}

	// Convert categories and series to JSON.
	categoriesJSON, _ := json.Marshal(categories)
	seriesJSON, _ := json.Marshal(series)

	// Build the HTML and JavaScript for the chart.
	Htmlpreid := `<div id="`
	Htmldivid := `barchart`
	Htmlpostid := `" class="flex justify-center mx-auto p-4"></div>`
	Jspreid := `Highcharts.chart('`
	Jspostid := fmt.Sprintf(`', {
    chart: {
        type: 'bar'
    },
    title: {
        text: '%s'
    },
    subtitle: {
        text: '%s'
    },
    xAxis: {
        categories: %s,
        title: {
            text: '%s'
        },
        gridLineWidth: 1,
        lineWidth: 0
    },
    yAxis: {
        min: 0,
        title: {
            text: '',
            align: 'middle'
        },
        labels: {
            overflow: 'justify'
        },
        gridLineWidth: 0
    },
    tooltip: {
        valueSuffix: ''
    },
    plotOptions: {
        bar: {
            borderRadius: '50%%',
            dataLabels: {
                enabled: true
            },
            groupPadding: 0.1
        }
    },
    credits: {
        enabled: false
    },
    series: %s
});`, title, subtitle, categoriesJSON, groupcol, seriesJSON)

	newChart := Chart{Htmlpreid, Htmldivid, Htmlpostid, Jspreid, Jspostid}
	return newChart
}

// ColumnChart returns Column Chart HTML for the DataFrame.
// It takes a title, subtitle, group column, and one or more aggregations.
func (df *DataFrame) ColumnChart(title string, subtitle string, groupcol string, aggs []Aggregation) Chart {
	// Group the DataFrame by the specified column and apply the aggregations.
	df = df.GroupBy(groupcol, aggs...)
	// df.Show(25)

	// Extract categories and series data.
	categories := []string{}
	for _, val := range df.Data[groupcol] {
		categories = append(categories, fmt.Sprintf("%v", val))
	}

	series := []map[string]interface{}{}
	for _, agg := range aggs {
		data := []interface{}{}
		for _, val := range df.Data[agg.ColumnName] {
			data = append(data, val)
		}
		series = append(series, map[string]interface{}{
			"name": agg.ColumnName,
			"data": data,
		})
	}

	// Convert categories and series to JSON.
	categoriesJSON, _ := json.Marshal(categories)
	seriesJSON, _ := json.Marshal(series)

	// Build the HTML and JavaScript for the chart.
	Htmlpreid := `<div id="`
	Htmldivid := `columnchart`
	Htmlpostid := `" class="flex justify-center mx-auto p-4"></div>`
	Jspreid := `Highcharts.chart('`
	Jspostid := fmt.Sprintf(`', {
    chart: {
        type: 'column'
    },
    title: {
        text: '%s'
    },
    subtitle: {
        text: '%s'
    },
    xAxis: {
        categories: %s,
        title: {
            text: '%s'
        },
        gridLineWidth: 1,
        lineWidth: 0
    },
    yAxis: {
        min: 0,
        title: {
            text: '',
            align: 'middle'
        },
        labels: {
            overflow: 'justify'
        },
        gridLineWidth: 0
    },
    tooltip: {
        valueSuffix: ''
    },
    plotOptions: {
        bar: {
            borderRadius: '50%%',
            dataLabels: {
                enabled: true
            },
            groupPadding: 0.1
        }
    },
    credits: {
        enabled: false
    },
    series: %s
});`, title, subtitle, categoriesJSON, groupcol, seriesJSON)

	newChart := Chart{Htmlpreid, Htmldivid, Htmlpostid, Jspreid, Jspostid}
	return newChart
}

// StackedBarChart returns Stacked Bar Chart HTML for the DataFrame.
// It takes a title, subtitle, group column, and one or more aggregations.
func (df *DataFrame) StackedBarChart(title string, subtitle string, groupcol string, aggs []Aggregation) Chart {
	// Group the DataFrame by the specified column and apply the aggregations.
	df = df.GroupBy(groupcol, aggs...)
	// df.Show(25)

	// Extract categories and series data.
	categories := []string{}
	for _, val := range df.Data[groupcol] {
		categories = append(categories, fmt.Sprintf("%v", val))
	}

	series := []map[string]interface{}{}
	for _, agg := range aggs {
		data := []interface{}{}
		for _, val := range df.Data[agg.ColumnName] {
			data = append(data, val)
		}
		series = append(series, map[string]interface{}{
			"name": agg.ColumnName,
			"data": data,
		})
	}

	// Convert categories and series to JSON.
	categoriesJSON, _ := json.Marshal(categories)
	seriesJSON, _ := json.Marshal(series)

	// Build the HTML and JavaScript for the chart.
	Htmlpreid := `<div id="`
	Htmldivid := `stackedbarchart`
	Htmlpostid := `" class="flex justify-center mx-auto p-4"></div>`
	Jspreid := `Highcharts.chart('`
	Jspostid := fmt.Sprintf(`', {
    chart: {
        type: 'bar'
    },
    title: {
        text: '%s'
    },
    subtitle: {
        text: '%s'
    },
    xAxis: {
        categories: %s,
        title: {
            text: '%s'
        },
        gridLineWidth: 1,
        lineWidth: 0
    },
    yAxis: {
        min: 0,
        title: {
            text: '',
            align: 'middle'
        },
    },
    plotOptions: {
        series: {
            stacking: 'normal',
            dataLabels: {
                enabled: true
            }
        }
    },
    series: %s
});`, title, subtitle, categoriesJSON, groupcol, seriesJSON)

	newChart := Chart{Htmlpreid, Htmldivid, Htmlpostid, Jspreid, Jspostid}
	return newChart
}

// StackedPercentChart returns Stacked Percent Column Chart HTML for the DataFrame.
// It takes a title, subtitle, group column, and one or more aggregations.
func (df *DataFrame) StackedPercentChart(title string, subtitle string, groupcol string, aggs []Aggregation) Chart {
	// Group the DataFrame by the specified column and apply the aggregations.
	df = df.GroupBy(groupcol, aggs...)
	// df.Show(25)

	// Extract categories and series data.
	categories := []string{}
	for _, val := range df.Data[groupcol] {
		categories = append(categories, fmt.Sprintf("%v", val))
	}

	series := []map[string]interface{}{}
	for _, agg := range aggs {
		data := []interface{}{}
		for _, val := range df.Data[agg.ColumnName] {
			data = append(data, val)
		}
		series = append(series, map[string]interface{}{
			"name": agg.ColumnName,
			"data": data,
		})
	}

	// Convert categories and series to JSON.
	categoriesJSON, _ := json.Marshal(categories)
	seriesJSON, _ := json.Marshal(series)

	// Build the HTML and JavaScript for the chart.
	Htmlpreid := `<div id="`
	Htmldivid := `stackedpercentchart`
	Htmlpostid := `" class="flex justify-center mx-auto p-4"></div>`
	Jspreid := `Highcharts.chart('`
	Jspostid := fmt.Sprintf(`', {
    chart: {
        type: 'column'
    },
    title: {
        text: '%s'
    },
    subtitle: {
        text: '%s'
    },
    xAxis: {
        categories: %s,
        title: {
            text: '%s'
        },
        gridLineWidth: 1,
        lineWidth: 0
    },
    yAxis: {
        min: 0,
        title: {
            text: 'Percent',
            align: 'middle'
        },
    },
    tooltip: {
        pointFormat: '<span style="color:{series.color}">{series.name}</span>' +
            ': <b>{point.y}</b> ({point.percentage:.0f}%%)<br/>',
        shared: true
    },
    plotOptions: {
        column: {
            stacking: 'percent',
            dataLabels: {
                enabled: true,
                format: '{point.percentage:.0f}%%'
            }
        }
    },
    series: %s
});`, title, subtitle, categoriesJSON, groupcol, seriesJSON)

	newChart := Chart{Htmlpreid, Htmldivid, Htmlpostid, Jspreid, Jspostid}
	return newChart
}

// BarChartWrapper is an exported function that wraps the BarChart function.
//
//export BarChartWrapper
func BarChartWrapper(dfJson *C.char, title *C.char, subtitle *C.char, groupcol *C.char, aggsJson *C.char) *C.char {
    var df DataFrame
    if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
        errStr := fmt.Sprintf("BarChartWrapper: unmarshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    }

    var simpleAggs []SimpleAggregation
    if err := json.Unmarshal([]byte(C.GoString(aggsJson)), &simpleAggs); err != nil {
        errStr := fmt.Sprintf("BarChartWrapper: unmarshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    }

    // Reconstruct the Aggregation structs
    var aggs []Aggregation
    for _, simpleAgg := range simpleAggs {
        // Directly use the aggregation functions instead of trying to wrap them
        switch simpleAgg.ColumnName {
        case "Sum":
            aggs = append(aggs, Sum(simpleAgg.ColumnName))
        case "Max":
            aggs = append(aggs, Max(simpleAgg.ColumnName))
        case "Min":
            aggs = append(aggs, Min(simpleAgg.ColumnName))
        case "Mean":
            aggs = append(aggs, Mean(simpleAgg.ColumnName))
        case "Median":
            aggs = append(aggs, Median(simpleAgg.ColumnName))
        case "Mode":
            aggs = append(aggs, Mode(simpleAgg.ColumnName))
        case "Unique":
            aggs = append(aggs, Unique(simpleAgg.ColumnName))
        case "First":
            aggs = append(aggs, First(simpleAgg.ColumnName))
        default:
            aggs = append(aggs, Sum(simpleAgg.ColumnName))
        }
    }

    chart := df.BarChart(C.GoString(title), C.GoString(subtitle), C.GoString(groupcol), aggs)
    // displayChart := DisplayChart(chart)
    // html, ok := displayChart["text/html"].(string)
    // if !ok {
    //     errStr := "BarChartWrapper: error displaying chart"
    //     log.Fatal(errStr)
    //     return C.CString(errStr)
    // }
	chartJson, err := json.Marshal(chart)
	fmt.Println("printing chartJson...")
	fmt.Println(string(chartJson))
	if err != nil {
		errStr := fmt.Sprintf("BarChartWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
    return C.CString(string(chartJson))
}
// ColumnChartWrapper is an exported function that wraps the ColumnChart function.
// It takes a JSON-string representing the DataFrame and chart parameters, calls ColumnChart, and
// returns the HTML string on success or an error message on failure.
//
//export ColumnChartWrapper
func ColumnChartWrapper(dfJson *C.char, title *C.char, subtitle *C.char, groupcol *C.char, aggsJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("ColumnChartWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	var aggs []Aggregation
	if err := json.Unmarshal([]byte(C.GoString(aggsJson)), &aggs); err != nil {
		errStr := fmt.Sprintf("ColumnChartWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	chart := df.ColumnChart(C.GoString(title), C.GoString(subtitle), C.GoString(groupcol), aggs)
	displayChart := DisplayChart(chart)
	html, ok := displayChart["text/html"].(string)
	if !ok {
		errStr := "ColumnChartWrapper: error displaying chart"
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(html)
}

// StackedBarChartWrapper is an exported function that wraps the StackedBarChart function.
// It takes a JSON-string representing the DataFrame and chart parameters, calls StackedBarChart, and
// returns the HTML string on success or an error message on failure.
//
//export StackedBarChartWrapper
func StackedBarChartWrapper(dfJson *C.char, title *C.char, subtitle *C.char, groupcol *C.char, aggsJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("StackedBarChartWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	var aggs []Aggregation
	if err := json.Unmarshal([]byte(C.GoString(aggsJson)), &aggs); err != nil {
		errStr := fmt.Sprintf("StackedBarChartWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	chart := df.StackedBarChart(C.GoString(title), C.GoString(subtitle), C.GoString(groupcol), aggs)
	displayChart := DisplayChart(chart)
	html, ok := displayChart["text/html"].(string)
	if !ok {
		errStr := "StackedBarChartWrapper: error displaying chart"
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(html)
}

// StackedPercentChartWrapper is an exported function that wraps the StackedPercentChart function.
// It takes a JSON-string representing the DataFrame and chart parameters, calls StackedPercentChart, and
// returns the HTML string on success or an error message on failure.
//
//export StackedPercentChartWrapper
func StackedPercentChartWrapper(dfJson *C.char, title *C.char, subtitle *C.char, groupcol *C.char, aggsJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("StackedPercentChartWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	var aggs []Aggregation
	if err := json.Unmarshal([]byte(C.GoString(aggsJson)), &aggs); err != nil {
		errStr := fmt.Sprintf("StackedPercentChartWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	chart := df.StackedPercentChart(C.GoString(title), C.GoString(subtitle), C.GoString(groupcol), aggs)
	displayChart := DisplayChart(chart)
	html, ok := displayChart["text/html"].(string)
	if !ok {
		errStr := "StackedPercentChartWrapper: error displaying chart"
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(html)
}

// PieChart

// AreaChart

// DataTable

// ScatterPlot

// BubbleChart

// TreeMap

// LineChart

// DASHBOARDS --------------------------------------------------

// dashboard create
func (df *DataFrame) CreateDashboard(title string) *Dashboard {
	HTMLTop := `
	<!DOCTYPE html>
	<html>
		<head>
			<script>
			tailwind.config = {
				theme: {
				extend: {
					colors: {`
	HTMLHeading := `	
				}
			}
		}
		</script>
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

	`
	ScriptHeading := `
			</div>
		</div>
	</body>
	<script>
		const { createApp } = Vue
		createApp({
		delimiters : ['[[', ']]'],
			data(){
				return {
					page: `

	ScriptMiddle := `
          }
        },
        methods: {

        },
        watch: {

        },
        created(){
		},
		  mounted() {
`

	HTMLBottom := `
        },
        computed:{

        }

    }).mount('#app')
  </script>
</html>
`

	newDashboard := &Dashboard{
		Top:           HTMLTop,
		Primary:       `primary: "#0000ff",`,
		Secondary:     `secondary: "#00aaff",`,
		Accent:        `accent: "#479700",`,
		Neutral:       `neutral: "#250e0d",`,
		Base100:       `"base-100": "#fffaff",`,
		Info:          `info: "#00c8ff",`,
		Success:       `success: "#00ec6a",`,
		Warning:       `warning: "#ffb900",`,
		Err:           `error: "#f00027",`,
		Htmlheading:   HTMLHeading,
		Title:         title,
		Htmlelements:  "",
		Scriptheading: ScriptHeading,
		Scriptmiddle:  ScriptMiddle,
		Bottom:        HTMLBottom,
		Pageshtml:     make(map[string]interface{}),
		Pagesjs:       make(map[string]interface{}),
	}
	fmt.Println("CreateDashboard: Initialized dashboard:", newDashboard)
	return newDashboard
}

// Open - open the dashboard in browser
func (dash *Dashboard) Open() error {
	// add html element for page
	html := dash.Top +
		dash.Primary +
		dash.Secondary +
		dash.Accent +
		dash.Neutral +
		dash.Base100 +
		dash.Info +
		dash.Success +
		dash.Warning +
		dash.Err +
		dash.Htmlheading
	if len(dash.Pageshtml) > 1 {
		html += `
        <div id="app"  style="text-align: center;" class="drawer w-full lg:drawer-open">
            <input id="my-drawer-2" type="checkbox" class="drawer-toggle" />
            <div class="drawer-content flex flex-col">
                <!-- Navbar -->
                <div class="w-full navbar bg-neutral text-neutral-content shadow-lg ">
            ` +
			fmt.Sprintf(`<div class="flex-1 px-2 mx-2 btn btn-sm btn-neutral normal-case text-xl shadow-none hover:bg-neutral hover:border-neutral flex content-center"><a class="ml-14 text-4xl">%s</a></div>`, dash.Title) +
			`
                <div class="flex-none lg:hidden">
                    <label for="my-drawer-2" class="btn btn-neutral btn-square shadow-lg hover:shadow-xl hover:-translate-y-0.5 no-animation">
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"
                        class="inline-block w-6 h-6 stroke-current">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"></path>
                    </svg>
                    </label>
                    </div>
                </div>
                <!-- content goes here! -->
                <div  class="w-full lg:w-3/4 md:w-3/4 sm:w-5/6 mx-auto flex-col justify-self-center">
            `

	} else {
		html += `
        <div id="app"  style="text-align: center;">
            <!-- Navbar -->
            <div class="w-full navbar bg-neutral text-neutral-content shadow-lg ">
        ` +
			fmt.Sprintf(`<div class="flex-1 px-2 mx-2 btn btn-sm btn-neutral normal-case text-xl shadow-none hover:bg-neutral hover:border-neutral flex content-center"><a class=" text-4xl">%s</a></div>
            </div>`, dash.Title) +
			`<div  class="w-full lg:w-3/4 md:w-3/4 sm:w-5/6 mx-auto flex-col justify-self-center">`

	}
	// iterate over pageshtml and add each stored HTML snippet
	for _, elem := range dash.Pageshtml {
		if pageMap, ok := elem.(map[string]interface{}); ok {
			// iterate in order
			for i := 0; i < len(pageMap); i++ {
				if htmlString, ok := pageMap[strconv.Itoa(i)].(string); ok {
					html += htmlString
				}
			}
		} else {
			html += fmt.Sprintf("%v", elem)
		}
	}
	if len(dash.Pageshtml) > 1 {
		html += `
            </div>
        </div>
        <!-- <br> -->
        <div class="drawer-side">
            <label for="my-drawer-2" class="drawer-overlay bg-neutral"></label>
            <ul class="menu p-4 w-80 bg-neutral h-full overflow-y-auto min-h-screen text-base-content shadow-none space-y-2 ">
            <div class="card w-72 bg-base-100 shadow-xl">
                <div class="card-body">
                    <div class="flex space-x-6 place-content-center">
                        <h2 class="card-title black-text-shadow-sm flex justify">Pages</h2>
                    </div>
                <div class="flex flex-col w-full h-1px">
                    <div class="divider"></div>
                </div>
                <div class="space-y-4">
        `
		for page, _ := range dash.Pageshtml {
			html += fmt.Sprintf(`
            <button v-if="page == '%s' " @click="page = '%s' " class="btn btn-block btn-sm btn-neutral text-white bg-neutral shadow-lg  hover:shadow-xl hover:-translate-y-0.5 no-animation " >%s</button>
            <button v-else @click="page = '%s' " class="btn btn-block btn-sm bg-base-100 btn-outline btn-neutral hover:text-white shadow-lg hover:shadow-xl hover:-translate-y-0.5 no-animation " >%s</button>
            
            `, page, page, page, page, page)
		}
	} else {
		html += `
            </div>
        </div>
        `
	}
	html += dash.Scriptheading
	pages := `pages: [`
	count := 0
	for page, _ := range dash.Pageshtml {
		if count == 0 {
			html += fmt.Sprintf("%q", page) + ","
		}
		pages += fmt.Sprintf("%q", page) + ", "
		count++
	}
	pages = strings.TrimSuffix(pages, ", ") + `],`
	html += pages
	html += dash.Scriptmiddle
	// iterate over pagesjs similarly
	for _, elem := range dash.Pagesjs {
		if jsMap, ok := elem.(map[string]interface{}); ok {
			for i := 0; i < len(jsMap); i++ {
				if jsString, ok := jsMap[strconv.Itoa(i)].(string); ok {
					html += jsString
				}
			}
		} else {
			html += fmt.Sprintf("%v", elem)
		}
	}
	html += dash.Bottom

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

// Save - save dashboard to html file
func (dash *Dashboard) Save(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// add html element for page
	html := dash.Top +
		dash.Primary +
		dash.Secondary +
		dash.Accent +
		dash.Neutral +
		dash.Base100 +
		dash.Info +
		dash.Success +
		dash.Warning +
		dash.Err +
		dash.Htmlheading

	// iterate over pageshtml and add each stored HTML snippet
	for _, elem := range dash.Pageshtml {
		if pageMap, ok := elem.(map[string]string); ok {
			// iterate in order
			for i := 0; i < len(pageMap); i++ {
				html += pageMap[strconv.Itoa(i)]
			}
		} else {
			html += fmt.Sprintf("%v", elem)
		}
	}
	html += dash.Scriptheading
	pages := `pages: [`
	count := 0
	for page, _ := range dash.Pageshtml {
		if count == 0 {
			html += fmt.Sprintf("%q", page) + ","
			count++
		}
		pages += fmt.Sprintf("%q", page) + ", "
	}
	pages = strings.TrimSuffix(pages, ", ") + `],`
	html += pages
	html += dash.Scriptmiddle
	// iterate over pagesjs similarly
	for _, elem := range dash.Pagesjs {
		if jsMap, ok := elem.(map[string]string); ok {
			for i := 0; i < len(jsMap); i++ {
				html += jsMap[strconv.Itoa(i)]
			}
		} else {
			html += fmt.Sprintf("%v", elem)
		}
	}
	html += dash.Bottom

	// Write the HTML string to the file
	if _, err := file.Write([]byte(html)); err != nil {
		return fmt.Errorf("failed to write to file: %v", err)
	}

	return nil
}

// AddPage adds a new page to the dashboard.
func (dash *Dashboard) AddPage(name string) {
	// Check if the page already exists.
	if _, exists := dash.Pageshtml[name]; exists {
		fmt.Println("Page already exists.")
		return
	}

	// Add the page to the dashboard.
	dash.Pageshtml[name] = map[string]string{}
	pageHTML, ok := dash.Pageshtml[name].(map[string]string)
	if !ok {
		fmt.Println("Invalid type for pageshtml[page]")
		return
	}

	html := `<h1 v-if="page == '` + name + `' " class="text-8xl pt-24 pb-24"> ` + name + `</h1>` // Page Title at top of page
	pageHTML[strconv.Itoa(len(pageHTML))] = html
	dash.Pagesjs[name] = map[string]string{}

	fmt.Println("AddPage: Added page:", name)
	fmt.Println("AddPage: Updated pageshtml:", dash.Pageshtml)
}

// spacing for stuff? card or no card? background?

// add text input

// add slider

// add dropdown (array of selections)

// add iframe
// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (dash *Dashboard) AddHTML(page string, text string) {
	// add html to page map
	if _, exists := dash.Pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPage()")
		return
	}
	// Assert the type of the value in pageshtml and pagesjs
	pageHTML, ok := dash.Pageshtml[page].(map[string]string)
	if !ok {
		fmt.Println("Invalid type for pageshtml[page]")
		return
	}
	texthtml := `<iframe v-if="page == '` + page + `' " class="p-8 flex justify-self-center sm:w-7/8 w-3/4" srcdoc='` + text + `'></iframe>`
	pageHTML[strconv.Itoa(len(pageHTML))] = texthtml
	// Update the maps with the new values
	dash.Pageshtml[page] = pageHTML

}

// add df (paginate + filter + sort)
func (dash *Dashboard) AddDataframe(page string, df *DataFrame) {
	text := df.Display()["text/html"].(string)
	// add html to page map
	if _, exists := dash.Pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPage()")
		return
	}
	dash.AddHTML("page2", text)
}

// add chart to dashboard page
func (dash *Dashboard) AddChart(page string, chart Chart) {
    // add html to page map
    if _, exists := dash.Pageshtml[page]; !exists {
        fmt.Println("Page does not exist. Use AddPage()")
        return
    }
    
    // Try to get the page contents 
    pageHTMLAny, exists := dash.Pageshtml[page]
    if !exists {
        fmt.Println("Page content doesn't exist")
        return
    }
    
    // Create a new map if needed
    var pageHTML map[string]string
    var pageJS map[string]string
    
    // Check the type and convert if necessary for HTML
    switch typedMap := pageHTMLAny.(type) {
    case map[string]string:
        pageHTML = typedMap
    case map[int]string:
        // Convert int keys to string keys
        pageHTML = make(map[string]string)
        for k, v := range typedMap {
            pageHTML[strconv.Itoa(k)] = v
        }
    case map[interface{}]interface{}:
        // Handle generic maps (common when deserializing from JSON)
        pageHTML = make(map[string]string)
        for k, v := range typedMap {
            if str, ok := v.(string); ok {
                pageHTML[fmt.Sprintf("%v", k)] = str
            }
        }
    default:
        // Initialize a new map if nothing else works
        fmt.Println("Creating new map for page HTML")
        pageHTML = make(map[string]string)
    }
    
    // Do the same for JS
    pageJSAny, exists := dash.Pagesjs[page]
    if !exists {
        pageJS = make(map[string]string)
        dash.Pagesjs[page] = pageJS
    } else {
        switch typedMap := pageJSAny.(type) {
        case map[string]string:
            pageJS = typedMap
        case map[int]string:
            // Convert int keys to string keys
            pageJS = make(map[string]string)
            for k, v := range typedMap {
                pageJS[strconv.Itoa(k)] = v
            }
        case map[interface{}]interface{}:
            // Handle generic maps
            pageJS = make(map[string]string)
            for k, v := range typedMap {
                if str, ok := v.(string); ok {
                    pageJS[fmt.Sprintf("%v", k)] = str
                }
            }
        default:
            // Initialize a new map if nothing else works
            pageJS = make(map[string]string)
        }
    }
    
	// Fix: Explicitly construct the chart HTML properly
	idx := strconv.Itoa(len(pageHTML))
	chartId := chart.Htmldivid + idx
		
	// Debug print to verify what's being generated
	fmt.Printf("Chart ID: %s\n", chartId)
	fmt.Printf("HTML postid: %s\n", chart.Htmlpostid)
		
	// Ensure we have necessary HTML parts - add fallbacks if missing
	if chart.Htmlpostid == "" {
		chart.Htmlpostid = `" class="flex justify-center mx-auto p-4"></div>`
	}
		
	// Properly format the HTML with explicit quotes and structure
	html := fmt.Sprintf(`<div v-show="page == '%s'" id="%s%s`, 
						page, chartId, chart.Htmlpostid)
    
    // Construct JS properly too
    js := fmt.Sprintf(`%s%s%s`, chart.Jspreid, chartId, chart.Jspostid)
    
    // Debug the final HTML
    fmt.Printf("Generated HTML: %s\n", html)
    
    pageHTML[idx] = html
    pageJS[idx] = js
    
    // Update the maps with the new values
    dash.Pageshtml[page] = pageHTML
    dash.Pagesjs[page] = pageJS
    
    fmt.Printf("Added chart to page %s at index %s\n", page, idx)
}

// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (dash *Dashboard) AddHeading(page string, heading string, size int) {
	// add html to page map
	if _, exists := dash.Pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPage()")
		return
	}
	// Assert the type of the value in pageshtml and pagesjs
	pageHTML, ok := dash.Pageshtml[page].(map[string]string)
	if !ok {
		fmt.Println("Invalid type for pageshtml[page]")
		return
	}
	var text_size string
	if size == 1 {
		text_size = "text-6xl"
	} else if size == 2 {
		text_size = "text-5xl"
	} else if size == 3 {
		text_size = "text-4xl"
	} else if size == 4 {
		text_size = "text-3xl"
	} else if size == 5 {
		text_size = "text-2xl"
	} else if size == 6 {
		text_size = "text-xl"
	} else if size == 7 {
		text_size = "text-lg"
	} else if size == 8 {
		text_size = "text-md"
	} else if size == 9 {
		text_size = "text-sm"
	} else if size == 10 {
		text_size = "text-xs"
	} else {
		text_size = "text-md"
	}
	html := `<h1 v-if="page == '` + page + fmt.Sprintf(`' " class="%s p-8 flex justify-start"> `, text_size) + heading + `</h1>`
	pageHTML[strconv.Itoa(len(pageHTML))] = html
	// Update the maps with the new values
	dash.Pageshtml[page] = pageHTML

}

// AddText function fix
func (dash *Dashboard) AddText(page string, text string) {
    // add html to page map
    if _, exists := dash.Pageshtml[page]; !exists {
        fmt.Println("Page does not exist. Use AddPage()")
        return
    }
    
    // Try to get the page contents as map[string]string
    pageHTMLAny, exists := dash.Pageshtml[page]
    if !exists {
        fmt.Println("Page content doesn't exist")
        return
    }
    
    // Create a new map if needed
    var pageHTML map[string]string
    
    // Check the type and convert if necessary
    switch typedMap := pageHTMLAny.(type) {
    case map[string]string:
        pageHTML = typedMap
    case map[int]string:
        // Convert int keys to string keys
        pageHTML = make(map[string]string)
        for k, v := range typedMap {
            pageHTML[strconv.Itoa(k)] = v
        }
    case map[interface{}]interface{}:
        // Handle generic maps (common when deserializing from JSON)
        pageHTML = make(map[string]string)
        for k, v := range typedMap {
            if str, ok := v.(string); ok {
                pageHTML[fmt.Sprintf("%v", k)] = str
            }
        }
    default:
        // Initialize a new map if nothing else works
        fmt.Println("Creating new map for page HTML")
        pageHTML = make(map[string]string)
    }
    
    // Add the new text element
    text_size := "text-md"
    html := `<h1 v-if="page == '` + page + fmt.Sprintf(`' " class="%s pl-12 pr-12 flex justify-start text-left"> `, text_size) + text + `</h1>`
    idx := strconv.Itoa(len(pageHTML))
    pageHTML[idx] = html
    
    // Update the page HTML
    dash.Pageshtml[page] = pageHTML
    fmt.Printf("Added text to page %s at index %s\n", page, idx)
}

// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (dash *Dashboard) AddSubText(page string, text string) {
	// add html to page map
	if _, exists := dash.Pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPage()")
		return
	}
	// Assert the type of the value in pageshtml and pagesjs
	pageHTML, ok := dash.Pageshtml[page].(map[string]string)
	if !ok {
		fmt.Println("Invalid type for pageshtml[page]")
		return
	}
	text_size := "text-sm"
	html := `<h1 v-if="page == '` + page + fmt.Sprintf(`' " class="%s pl-12 pr-12 pb-8 flex justify-center"> `, text_size) + text + `</h1>`
	pageHTML[strconv.Itoa(len(pageHTML))] = html
	// Update the maps with the new values
	dash.Pageshtml[page] = pageHTML

}

// add bullet list
// add html to page map
// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (dash *Dashboard) AddBullets(page string, text ...string) {
	// add html to page map
	if _, exists := dash.Pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPage()")
		return
	}
	// Assert the type of the value in pageshtml and pagesjs
	pageHTML, ok := dash.Pageshtml[page].(map[string]string)
	if !ok {
		fmt.Println("Invalid type for pageshtml[page]")
		return
	}
	text_size := "text-md"
	html := `<ul v-if="page == '` + page + `' " class="list-disc flex-col justify-self-start pl-24 pr-12 py-2"> `
	for _, bullet := range text {
		html += fmt.Sprintf(`<li class="text-left %s">`, text_size) + bullet + `</li>`
	}
	html += `</ul>`
	pageHTML[strconv.Itoa(len(pageHTML))] = html
	// Update the maps with the new values
	dash.Pageshtml[page] = pageHTML

}

// CreateDashboardWrapper is an exported function that wraps the CreateDashboard method.
//
//export CreateDashboardWrapper
func CreateDashboardWrapper(dfJson *C.char, title *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("CreateDashboardWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	fmt.Printf("printing dfjson:%s", []byte(C.GoString(dfJson)))
	fmt.Println("")
	dashboard := df.CreateDashboard(C.GoString(title))
	fmt.Printf("printing dashboard:%s", dashboard)
	dashboardJson, err := json.Marshal(dashboard)
	fmt.Printf("printing dashboardJson:%s", dashboardJson)
	fmt.Printf("printing stringed dashboardJson:%s", dashboardJson)
	if err != nil {
		errStr := fmt.Sprintf("CreateDashboardWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	dashboardJsonStr := string(dashboardJson)
	fmt.Println("CreateDashboardWrapper: Created dashboard JSON:", dashboardJsonStr)
	fmt.Println("printing dashboardJson stringed:", dashboardJsonStr)
	return C.CString(dashboardJsonStr)
}

// OpenDashboardWrapper is an exported function that wraps the Open method.
//
//export OpenDashboardWrapper
func OpenDashboardWrapper(dashboardJson *C.char) *C.char {
	var dashboard Dashboard
	if err := json.Unmarshal([]byte(C.GoString(dashboardJson)), &dashboard); err != nil {
		errStr := fmt.Sprintf("OpenDashboardWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	if err := dashboard.Open(); err != nil {
		errStr := fmt.Sprintf("OpenDashboardWrapper: open error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString("success")
}

// SaveDashboardWrapper is an exported function that wraps the Save method.
//
//export SaveDashboardWrapper
func SaveDashboardWrapper(dashboardJson *C.char, filename *C.char) *C.char {
	var dashboard Dashboard
	if err := json.Unmarshal([]byte(C.GoString(dashboardJson)), &dashboard); err != nil {
		errStr := fmt.Sprintf("SaveDashboardWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	if err := dashboard.Save(C.GoString(filename)); err != nil {
		errStr := fmt.Sprintf("SaveDashboardWrapper: save error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return nil
}

// AddPageWrapper is an exported function that wraps the AddPage method.
//
//export AddPageWrapper
func AddPageWrapper(dashboardJson *C.char, name *C.char) *C.char {
	var dashboard Dashboard
	if err := json.Unmarshal([]byte(C.GoString(dashboardJson)), &dashboard); err != nil {
		errStr := fmt.Sprintf("AddPageWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// dashboard.init() // Initialize the maps
	dashboard.AddPage(C.GoString(name))
	fmt.Println("AddPageWrapper: Dashboard after adding page:", dashboard)
	dashboardJsonBytes, err := json.Marshal(dashboard)
	if err != nil {
		errStr := fmt.Sprintf("AddPageWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	fmt.Println("AddPageWrapper: Updated dashboard JSON:", string(dashboardJsonBytes))
	return C.CString(string(dashboardJsonBytes))
}

// AddHTMLWrapper is an exported function that wraps the AddHTML method.
//
//export AddHTMLWrapper
func AddHTMLWrapper(dashboardJson *C.char, page *C.char, text *C.char) *C.char {
	var dashboard Dashboard
	if err := json.Unmarshal([]byte(C.GoString(dashboardJson)), &dashboard); err != nil {
		errStr := fmt.Sprintf("AddHTMLWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// dashboard.init() // Initialize the maps
	dashboard.AddHTML(C.GoString(page), C.GoString(text))
	dashboardJsonBytes, err := json.Marshal(dashboard)
	if err != nil {
		errStr := fmt.Sprintf("AddHTMLWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(dashboardJsonBytes))
}

// AddDataframeWrapper is an exported function that wraps the AddDataframe method.
//
//export AddDataframeWrapper
func AddDataframeWrapper(dashboardJson *C.char, page *C.char, dfJson *C.char) *C.char {
	var dashboard Dashboard
	if err := json.Unmarshal([]byte(C.GoString(dashboardJson)), &dashboard); err != nil {
		errStr := fmt.Sprintf("AddDataframeWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("AddDataframeWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// dashboard.init() // Initialize the maps
	dashboard.AddDataframe(C.GoString(page), &df)
	dashboardJsonBytes, err := json.Marshal(dashboard)
	if err != nil {
		errStr := fmt.Sprintf("AddDataframeWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(dashboardJsonBytes))
}

// AddChartWrapper is an exported function that wraps the AddChart method.
//
//export AddChartWrapper
func AddChartWrapper(dashboardJson *C.char, page *C.char, chartJson *C.char) *C.char {
    var dashboard Dashboard
    if err := json.Unmarshal([]byte(C.GoString(dashboardJson)), &dashboard); err != nil {
        errStr := fmt.Sprintf("AddChartWrapper: unmarshal error: %v", err)
        return C.CString(errStr)
    }

    var chart Chart
    if err := json.Unmarshal([]byte(C.GoString(chartJson)), &chart); err != nil {
        errStr := fmt.Sprintf("AddChartWrapper: unmarshal error: %v", err)
        return C.CString(errStr)
    }
    // dashboard.init() // Initialize the maps
	fmt.Println("adding chart to page...")
	fmt.Println("chart:", chart)

    dashboard.AddChart(C.GoString(page), chart)

    dashboardJsonBytes, err := json.Marshal(dashboard)
    if err != nil {
        errStr := fmt.Sprintf("AddChartWrapper: marshal error: %v", err)
        return C.CString(errStr)
    }

    return C.CString(string(dashboardJsonBytes))
}
// AddHeadingWrapper is an exported function that wraps the AddHeading method.
//
//export AddHeadingWrapper
func AddHeadingWrapper(dashboardJson *C.char, page *C.char, heading *C.char, size C.int) *C.char {
	var dashboard Dashboard
	if err := json.Unmarshal([]byte(C.GoString(dashboardJson)), &dashboard); err != nil {
		errStr := fmt.Sprintf("AddHeadingWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// dashboard.init() // Initialize the maps
	dashboard.AddHeading(C.GoString(page), C.GoString(heading), int(size))
	dashboardJsonBytes, err := json.Marshal(dashboard)
	if err != nil {
		errStr := fmt.Sprintf("AddHeadingWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(dashboardJsonBytes))
}

// AddTextWrapper is an exported function that wraps the AddText method.
//
//export AddTextWrapper
func AddTextWrapper(dashboardJson *C.char, page *C.char, text *C.char) *C.char {
	var dashboard Dashboard
	if err := json.Unmarshal([]byte(C.GoString(dashboardJson)), &dashboard); err != nil {
		errStr := fmt.Sprintf("AddTextWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// dashboard.init() // Initialize the maps
	dashboard.AddText(C.GoString(page), C.GoString(text))
	dashboardJsonBytes, err := json.Marshal(dashboard)
	if err != nil {
		errStr := fmt.Sprintf("AddTextWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(dashboardJsonBytes))
}

// AddSubTextWrapper is an exported function that wraps the AddSubText method.
//
//export AddSubTextWrapper
func AddSubTextWrapper(dashboardJson *C.char, page *C.char, text *C.char) *C.char {
	var dashboard Dashboard
	if err := json.Unmarshal([]byte(C.GoString(dashboardJson)), &dashboard); err != nil {
		errStr := fmt.Sprintf("AddSubTextWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// dashboard.init() // Initialize the maps
	dashboard.AddSubText(C.GoString(page), C.GoString(text))
	dashboardJsonBytes, err := json.Marshal(dashboard)
	if err != nil {
		errStr := fmt.Sprintf("AddSubTextWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(dashboardJsonBytes))
}

// AddBulletsWrapper is an exported function that wraps the AddBullets method.
//
//export AddBulletsWrapper
func AddBulletsWrapper(dashboardJson *C.char, page *C.char, bulletsJson *C.char) *C.char {
	var dashboard Dashboard
	if err := json.Unmarshal([]byte(C.GoString(dashboardJson)), &dashboard); err != nil {
		errStr := fmt.Sprintf("AddBulletsWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	var bullets []string
	if err := json.Unmarshal([]byte(C.GoString(bulletsJson)), &bullets); err != nil {
		errStr := fmt.Sprintf("AddBulletsWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// dashboard.init() // Initialize the maps
	dashboard.AddBullets(C.GoString(page), bullets...)
	dashboardJsonBytes, err := json.Marshal(dashboard)
	if err != nil {
		errStr := fmt.Sprintf("AddBulletsWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(dashboardJsonBytes))
}

// AGGREGATES --------------------------------------------------

// SumWrapper is an exported function that returns an Aggregation struct for the Sum function.
//
//export SumWrapper
func SumWrapper(name *C.char) *C.char {
    colName := C.GoString(name)
    // Create a JSON object with the column name and function name
    aggJson, err := json.Marshal(map[string]string{"ColumnName": colName, "Fn": "Sum"})
    if err != nil {
        errStr := fmt.Sprintf("SumWrapper: marshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    }
    return C.CString(string(aggJson))
}

// AggWrapper is an exported function that converts multiple Column functions to a slice of Aggregation structs.
//
//export AggWrapper
func AggWrapper(colsJson *C.char) *C.char {
	var cols []Column
	if err := json.Unmarshal([]byte(C.GoString(colsJson)), &cols); err != nil {
		errStr := fmt.Sprintf("AggWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	aggs := Agg(cols...)
	simpleAggs := make([]SimpleAggregation, len(aggs))
	for i, agg := range aggs {
		simpleAggs[i] = SimpleAggregation{
			ColumnName: agg.ColumnName,
		}
	}

	aggsJson, err := json.Marshal(simpleAggs)
	if err != nil {
		errStr := fmt.Sprintf("AggWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(aggsJson))
}

// MaxWrapper is an exported function that wraps the Max function.
//
//export MaxWrapper
func MaxWrapper(name *C.char) *C.char {
    agg := Max(C.GoString(name))
    aggJson, err := json.Marshal(map[string]string{"ColumnName": agg.ColumnName, "Fn": "Max"})
    if err != nil {
        errStr := fmt.Sprintf("MaxWrapper: marshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    }
    return C.CString(string(aggJson))
}

// MinWrapper is an exported function that wraps the Min function.
//
//export MinWrapper
func MinWrapper(name *C.char) *C.char {
    agg := Min(C.GoString(name))
    aggJson, err := json.Marshal(map[string]string{"ColumnName": agg.ColumnName, "Fn": "Min"})
    if err != nil {
        errStr := fmt.Sprintf("MinWrapper: marshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    }
    return C.CString(string(aggJson))
}

// MedianWrapper is an exported function that wraps the Median function.
//
//export MedianWrapper
func MedianWrapper(name *C.char) *C.char {
    agg := Median(C.GoString(name))
    aggJson, err := json.Marshal(map[string]string{"ColumnName": agg.ColumnName, "Fn": "Median"})
    if err != nil {
        errStr := fmt.Sprintf("MedianWrapper: marshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    }
    return C.CString(string(aggJson))
}

// MeanWrapper is an exported function that wraps the Mean function.
//
//export MeanWrapper
func MeanWrapper(name *C.char) *C.char {
    agg := Mean(C.GoString(name))
    aggJson, err := json.Marshal(map[string]string{"ColumnName": agg.ColumnName, "Fn": "Mean"})
    if err != nil {
        errStr := fmt.Sprintf("MeanWrapper: marshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    }
    return C.CString(string(aggJson))
}

// ModeWrapper is an exported function that wraps the Mode function.
//
//export ModeWrapper
func ModeWrapper(name *C.char) *C.char {
    agg := Mode(C.GoString(name))
    aggJson, err := json.Marshal(map[string]string{"ColumnName": agg.ColumnName, "Fn": "Mode"})
    if err != nil {
        errStr := fmt.Sprintf("ModeWrapper: marshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    }
    return C.CString(string(aggJson))
}
// UniqueWrapper is an exported function that wraps the Unique function.
//
//export UniqueWrapper
func UniqueWrapper(name *C.char) *C.char {
    agg := Unique(C.GoString(name))
    aggJson, err := json.Marshal(map[string]string{"ColumnName": agg.ColumnName, "Fn": "Unique"})
    if err != nil {
        errStr := fmt.Sprintf("UniqueWrapper: marshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    }
    return C.CString(string(aggJson))
}

// FirstWrapper is an exported function that wraps the First function.
//
//export FirstWrapper
func FirstWrapper(name *C.char) *C.char {
    agg := First(C.GoString(name))
    aggJson, err := json.Marshal(map[string]string{"ColumnName": agg.ColumnName, "Fn": "First"})
    if err != nil {
        errStr := fmt.Sprintf("FirstWrapper: marshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    } 
    return C.CString(string(aggJson))
}
// Agg converts multiple Column functions to a slice of Aggregation structs for use in aggregation.
func Agg(cols ...Column) []Aggregation {
    aggs := []Aggregation{}
    for _, col := range cols {
        colName := col.Name
        agg := Aggregation{
            ColumnName: colName,
            Fn: func(vals []interface{}) interface{} {
                // Create a map with the column name as key and the first value
                // We're using a dummy map just to match the expected type
                dummyRow := make(map[string]interface{})
                // Put all values in the map under the column name
                dummyRow[colName] = vals[0] // Use just the first value for simplicity
                
                // Call the Column's function with this map
                return col.Fn(dummyRow)
            },
        }
        aggs = append(aggs, agg)
    }
    return aggs
}
// Sum returns an Aggregation that sums numeric values from the specified column.
func Sum(name string) Aggregation {
    return Aggregation{
        ColumnName: name,
        Fn: func(vals []interface{}) interface{} {
            sum := 0.0
            for _, v := range vals {
                fVal, err := toFloat64(v)
                if err != nil {
                    fmt.Printf("sum conversion error: %v\n", err)
                    continue
                }
                sum += fVal
            }
            return sum
        },
    }
}

// Max returns an Aggregation that finds the maximum numeric value from the specified column.
func Max(name string) Aggregation {
    return Aggregation{
        ColumnName: name,
        Fn: func(vals []interface{}) interface{} {
            maxSet := false
            var max float64
            for _, v := range vals {
                fVal, err := toFloat64(v)
                if err != nil {
                    fmt.Printf("max conversion error: %v\n", err)
                    continue
                }
                if !maxSet || fVal > max {
                    max = fVal
                    maxSet = true
                }
            }
            if !maxSet {
                return nil
            }
            return max
        },
    }
}

// Min returns an Aggregation that finds the minimum numeric value from the specified column.
func Min(name string) Aggregation {
    return Aggregation{
        ColumnName: name,
        Fn: func(vals []interface{}) interface{} {
            minSet := false
            var min float64
            for _, v := range vals {
                fVal, err := toFloat64(v)
                if err != nil {
                    fmt.Printf("min conversion error: %v\n", err)
                    continue
                }
                if !minSet || fVal < min {
                    min = fVal
                    minSet = true
                }
            }
            if !minSet {
                return nil
            }
            return min
        },
    }
}

// Median returns an Aggregation that finds the median numeric value from the specified column.
func Median(name string) Aggregation {
    return Aggregation{
        ColumnName: name,
        Fn: func(vals []interface{}) interface{} {
            var nums []float64
            for _, v := range vals {
                fVal, err := toFloat64(v)
                if err != nil {
                    fmt.Printf("median conversion error: %v\n", err)
                    continue
                }
                nums = append(nums, fVal)
            }

            n := len(nums)
            if n == 0 {
                return nil
            }

            // Sort the numbers.
            sort.Float64s(nums)

            if n%2 == 1 {
                // Odd count; return middle element.
                return nums[n/2]
            }
            // Even count; return average of two middle elements.
            median := (nums[n/2-1] + nums[n/2]) / 2.0
            return median
        },
    }
}

// Mean returns an Aggregation that calculates the mean (average) of numeric values from the specified column.
func Mean(name string) Aggregation {
    return Aggregation{
        ColumnName: name,
        Fn: func(vals []interface{}) interface{} {
            sum := 0.0
            count := 0
            for _, v := range vals {
                fVal, err := toFloat64(v)
                if err != nil {
                    fmt.Printf("mean conversion error: %v\n", err)
                    continue
                }
                sum += fVal
                count++
            }
            if count == 0 {
                return nil
            }
            return sum / float64(count)
        },
    }
}
// Mode returns an Aggregation that finds the mode (most frequent value) among the numeric values from the specified column.
func Mode(name string) Aggregation {
    return Aggregation{
        ColumnName: name,
        Fn: func(vals []interface{}) interface{} {
            // Use a map to count frequencies.
            freq := make(map[float64]int)
            var mode float64
            maxCount := 0

            for _, v := range vals {
                fVal, err := toFloat64(v)
                if err != nil {
                    fmt.Printf("mode conversion error: %v\n", err)
                    continue
                }
                freq[fVal]++
                if freq[fVal] > maxCount {
                    maxCount = freq[fVal]
                    mode = fVal
                }
            }
            // If no valid values, return nil.
            if maxCount == 0 {
                return nil
            }
            return mode
        },
    }
}
// Unique returns an Aggregation that counts the number of unique values from the specified column.
func Unique(name string) Aggregation {
    return Aggregation{
        ColumnName: name,
        Fn: func(vals []interface{}) interface{} {
            uniqueSet := make(map[interface{}]bool)
            for _, v := range vals {
                uniqueSet[v] = true
            }
            return len(uniqueSet)
        },
    }
}

// First returns an Aggregation that gets the first value from the specified column.
func First(name string) Aggregation {
    return Aggregation{
        ColumnName: name,
        Fn: func(vals []interface{}) interface{} {
            if len(vals) == 0 {
                return nil
            }
            return vals[0]
        },
    }
}

// LOGIC --------------------------------------------------

// If

// IsNull

// IsNotNull

// Gt

// Ge

// Lt

// Le

// Or

// And

// Eq

// Ne

// TRANSFORMS --------------------------------------------------

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

// Concat

// Concat_WS

// Filter

// Explode

// Cast

// Rename

// FillNA

// - ToFloat64
// - ToInt
// - ToString

// DropDuplicates

// Select

// GroupByWrapper is an exported function that wraps the GroupBy method.
// It takes a JSON-string representing the DataFrame, the group column, and a JSON-string representing the aggregations.
// It returns the resulting DataFrame as a JSON string.
//
//export GroupByWrapper
func GroupByWrapper(dfJson *C.char, groupCol *C.char, aggsJson *C.char) *C.char {
    var df DataFrame
    if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
        errStr := fmt.Sprintf("GroupByWrapper: unmarshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    }

    var aggCols []map[string]string
    if err := json.Unmarshal([]byte(C.GoString(aggsJson)), &aggCols); err != nil {
        errStr := fmt.Sprintf("GroupByWrapper: unmarshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    }

    // Extract column names and function names from the aggregation JSON
    var aggregations []Aggregation
    for _, agg := range aggCols {
        colName := agg["ColumnName"]
        fnName := agg["Fn"]
        switch fnName {
        case "Sum":
            aggregations = append(aggregations, Sum(colName))
        case "Max":
            aggregations = append(aggregations, Max(colName))
        case "Min":
            aggregations = append(aggregations, Min(colName))
        case "Mean":
            aggregations = append(aggregations, Mean(colName))
        case "Median":
            aggregations = append(aggregations, Median(colName))
        case "Mode":
            aggregations = append(aggregations, Mode(colName))
        case "Unique":
            aggregations = append(aggregations, Unique(colName))
        case "First":
            aggregations = append(aggregations, First(colName))
        }
    }

    groupedDF := df.GroupBy(C.GoString(groupCol), aggregations...)
    resultJson, err := json.Marshal(groupedDF)
    if err != nil {
        errStr := fmt.Sprintf("GroupByWrapper: marshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    }

    return C.CString(string(resultJson))
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

// FUNCTIONS --------------------------------------------------

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

// END --------------------------------------------------

func main() {}
