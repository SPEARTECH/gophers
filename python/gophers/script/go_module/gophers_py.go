//go:build cshared && cgo
// +build cshared,cgo

package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"unsafe"

	g "github.com/SPEARTECH/gophers/go/gophers"
	_ "github.com/mattn/go-sqlite3"
)

// Type aliases to use core types without rewriting code
type (
	DataFrame         = g.DataFrame
	ColumnExpr        = g.ColumnExpr
	Aggregation       = g.Aggregation
	AggregatorFn      = g.AggregatorFn
	Chart             = g.Chart
	Report            = g.Report
	SimpleAggregation = g.SimpleAggregation
	Column            = g.Column
)

//export Free
func Free(p *C.char) {
	if p != nil {
		C.free(unsafe.Pointer(p))
	}
}

// // MarshalJSON custom marshaller to exclude the function field.
// func (c Column) MarshalJSON() ([]byte, error) {
// 	return json.Marshal(struct {
// 		Name string `json:"Name"`
// 	}{
// 		Name: c.Name,
// 	})
// }

// // UnmarshalJSON custom unmarshaller to handle the function field.
// func (c *Column) UnmarshalJSON(data []byte) error {
// 	var aux struct {
// 		Name string `json:"Name"`
// 	}
// 	if err := json.Unmarshal(data, &aux); err != nil {
// 		return err
// 	}
// 	c.Name = aux.Name
// 	// Note: The function field cannot be unmarshalled from JSON.
// 	return nil
// }

// SOURCES --------------------------------------------------

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
func ReadCSV(csvData *C.char) *C.char {
	df := g.ReadCSV(C.GoString(csvData))
	js, err := json.Marshal(df)
	if err != nil {
		errStr := fmt.Sprintf("ReadCSV wrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(js))
}

//export ReadJSON
func ReadJSON(jsonStr *C.char) *C.char {
	if jsonStr == nil {
		log.Fatalf("Error: jsonStr is nil")
		return C.CString("")
	}
	df := g.ReadJSON(C.GoString(jsonStr))
	jsonBytes, err := json.Marshal(df)
	if err != nil {
		log.Fatalf("ReadJSON: marshal: %v", err)
	}
	return C.CString(string(jsonBytes))
}

//export ReadNDJSON
func ReadNDJSON(ndjson *C.char) *C.char {
	df := g.ReadNDJSON(C.GoString(ndjson))
	b, err := json.Marshal(df)
	if err != nil {
		log.Fatalf("ReadNDJSON wrapper: marshal error: %v", err)
	}
	return C.CString(string(b))
}

// ReadYAML reads a YAML string or file and converts it to a DataFrame.
//
//export ReadYAML
func ReadYAML(yamlStr *C.char) *C.char {
	if yamlStr == nil {
		errStr := "ReadYAML: input is nil"
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	df := g.ReadYAML(C.GoString(yamlStr))
	jsonBytes, err := json.Marshal(df)
	if err != nil {
		errStr := fmt.Sprintf("ReadYAML: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(jsonBytes))
}

// ReadSqlite is a helper that returns the DataFrame JSON string.
//
//export ReadSqlite
func ReadSqlite(path, table, query string) (string, error) {
	df, err := g.ReadSqlite(path, table, query)
	if err != nil {
		return "", err
	}
	jsonBytes, err := json.Marshal(df)
	if err != nil {
		return "", fmt.Errorf("ReadSqliteJSON: marshal error: %w", err)
	}
	return string(jsonBytes), nil
}

//export GetSqliteTables
func GetSqliteTables(dbPath *C.char) *C.char {
	path := C.GoString(dbPath)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("open error: %v", err)))
	}
	defer db.Close()

	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("query error: %v", err)))
	}
	defer rows.Close()

	names := make([]string, 0, 16)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("scan error: %v", err)))
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("rows error: %v", err)))
	}

	payload, _ := json.Marshal(map[string]interface{}{"tables": names})
	return C.CString(string(payload))
}

//export GetSqliteSchema
func GetSqliteSchema(dbPath *C.char, table *C.char) *C.char {
	js := g.GetSqliteSchemaJSON(C.GoString(dbPath), C.GoString(table))
	return C.CString(js)
}

//export ReadHTML
func ReadHTML(htmlInput *C.char) *C.char {
	df := g.ReadHTML(C.GoString(htmlInput))
	js, err := json.Marshal(df)
	if err != nil {
		errStr := fmt.Sprintf("ReadHTML: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(js))
}

//export Clone
func Clone(dfJson *C.char) *C.char {
	js := g.CloneJSON(C.GoString(dfJson))
	return C.CString(js)
}

// Flatten accepts a JSON string for the DataFrame and a JSON array of column names to flatten.
//
//export Flatten
func Flatten(dfJson *C.char, flattenColsJson *C.char) *C.char {
	// Unmarshal the DataFrame.
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("Flatten: DataFrame unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	// Unmarshal the flatten columns (JSON array of strings).
	var flattenCols []string
	if err := json.Unmarshal([]byte(C.GoString(flattenColsJson)), &flattenCols); err != nil {
		errStr := fmt.Sprintf("Flatten: flattenCols unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	// Call the Flatten method.
	newDF := df.Flatten(flattenCols)

	// Marshal the new DataFrame to JSON.
	jsonBytes, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("Flatten: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(jsonBytes))
}

// KeysToCols accepts a JSON string for the DataFrame and a column name (as a plain C string).
// It converts any nested map in that column into separate columns and returns the updated DataFrame as JSON.
//
//export KeysToCols
func KeysToCols(dfJson *C.char, nestedCol *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("KeysToCols: DataFrame unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	newDF := df.KeysToCols(C.GoString(nestedCol))
	jsonBytes, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("KeysToCols: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(jsonBytes))
}

// flattenOnce flattens only one level of the nested map,
// prefixing each key with the given prefix and a dot.
func flattenOnce(m map[string]interface{}, prefix string) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		result[prefix+"."+k] = v
	}
	return result
}

// StringArrayConvert accepts a JSON string for the DataFrame and a column name to convert.
//
//export StringArrayConvert
func StringArrayConvert(dfJson *C.char, column *C.char) *C.char {
	// Unmarshal the DataFrame.
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("StringArrayConvert: DataFrame unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	// Call the StringArrayConvert method.
	newDF := df.StringArrayConvert(C.GoString(column))

	// Marshal the new DataFrame to JSON.
	jsonBytes, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("StringArrayConvert: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(jsonBytes))
}

// make flatten function - from pyspark methodology (for individual columns)
// func flattenWrapper(djJson *C.char, col *C.char)

// ReadParquetWrapper is a c-shared exported function that wraps ReadParquet.
// It accepts a C string representing the path (or content) of a parquet file,
// calls ReadParquet, marshals the resulting DataFrame back to JSON, and returns it as a C string.
//
//export ReadParquet
func ReadParquet(parquetPath *C.char) *C.char {
	goPath := C.GoString(parquetPath)
	df := g.ReadParquet(goPath)
	jsonBytes, err := json.Marshal(df)
	if err != nil {
		log.Fatalf("ReadParquet: error marshalling DataFrame: %v", err)
	}
	return C.CString(string(jsonBytes))
}

//export GetAPI
func GetAPI(endpoint *C.char, headers *C.char, queryParams *C.char) *C.char {
	ep := C.GoString(endpoint)
	hStr := C.GoString(headers)
	qStr := C.GoString(queryParams)

	// Parse headers from "Key: Value" lines.
	h := map[string]string{}
	if hStr != "" {
		for _, line := range strings.Split(hStr, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				k := strings.TrimSpace(parts[0])
				v := strings.TrimSpace(parts[1])
				if k != "" {
					h[k] = v
				}
			}
		}
	}

	// Parse query params from "a=b&c=d" form.
	qm := map[string]string{}
	if qStr != "" {
		values, _ := url.ParseQuery(qStr)
		for k, vs := range values {
			if len(vs) > 0 {
				qm[k] = vs[0]
			}
		}
	}

	df, err := g.GetAPI(ep, h, qm)
	if err != nil {
		errStr := fmt.Sprintf("GetAPI: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	jsonBytes, err := json.Marshal(df)
	if err != nil {
		errStr := fmt.Sprintf("GetAPI: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(jsonBytes))
}

// DISPLAYS --------------------------------------------------

//export Show
func Show(dfJson *C.char, chars C.int, record_count C.int) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("Error unmarshalling DataFrame JSON: %v", err)
	}
	text := df.Show(int(chars), int(record_count))
	return C.CString(text)
}

//export Head
func Head(dfJson *C.char, chars C.int) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("Error unmarshalling DataFrame JSON in Head: %v", err)
	}
	text := df.Head(int(chars))
	return C.CString(text)
}

//export Tail
func Tail(dfJson *C.char, chars C.int) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("Error unmarshalling DataFrame JSON in Tail: %v", err)
	}
	out := df.Tail(int(chars))
	return C.CString(out)
}

//export Vertical
func Vertical(dfJson *C.char, chars C.int, record_count C.int) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("Error unmarshalling DataFrame JSON in Vertical: %v", err)
	}
	out := df.Vertical(int(chars), int(record_count))
	return C.CString(out)
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

// DisplayHTML returns a value that gophernotes recognizes as rich HTML output.
func DisplayHTML(html string) map[string]interface{} {
	return map[string]interface{}{
		"text/html": html,
	}
}

// CHARTS --------------------------------------------------

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
			aggs = append(aggs, g.Sum(simpleAgg.ColumnName))
		case "Max":
			aggs = append(aggs, g.Max(simpleAgg.ColumnName))
		case "Min":
			aggs = append(aggs, g.Min(simpleAgg.ColumnName))
		case "Mean":
			aggs = append(aggs, g.Mean(simpleAgg.ColumnName))
		case "Median":
			aggs = append(aggs, g.Median(simpleAgg.ColumnName))
		case "Mode":
			aggs = append(aggs, g.Mode(simpleAgg.ColumnName))
		case "Unique":
			aggs = append(aggs, g.Unique(simpleAgg.ColumnName))
		case "First":
			aggs = append(aggs, g.First(simpleAgg.ColumnName))
		default:
			aggs = append(aggs, g.Sum(simpleAgg.ColumnName))
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
	// fmt.Println("printing chartJson...")
	// fmt.Println(string(chartJson))
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

	var simpleAggs []SimpleAggregation
	if err := json.Unmarshal([]byte(C.GoString(aggsJson)), &simpleAggs); err != nil {
		errStr := fmt.Sprintf("ColumnChartWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	// Reconstruct the Aggregation structs
	var aggs []Aggregation
	for _, simpleAgg := range simpleAggs {
		// Directly use the aggregation functions instead of trying to wrap them
		switch simpleAgg.ColumnName {
		case "Sum":
			aggs = append(aggs, g.Sum(simpleAgg.ColumnName))
		case "Max":
			aggs = append(aggs, g.Max(simpleAgg.ColumnName))
		case "Min":
			aggs = append(aggs, g.Min(simpleAgg.ColumnName))
		case "Mean":
			aggs = append(aggs, g.Mean(simpleAgg.ColumnName))
		case "Median":
			aggs = append(aggs, g.Median(simpleAgg.ColumnName))
		case "Mode":
			aggs = append(aggs, g.Mode(simpleAgg.ColumnName))
		case "Unique":
			aggs = append(aggs, g.Unique(simpleAgg.ColumnName))
		case "First":
			aggs = append(aggs, g.First(simpleAgg.ColumnName))
		default:
			aggs = append(aggs, g.Sum(simpleAgg.ColumnName))
		}
	}

	chart := df.ColumnChart(C.GoString(title), C.GoString(subtitle), C.GoString(groupcol), aggs)
	// displayChart := DisplayChart(chart)
	// html, ok := displayChart["text/html"].(string)
	// if !ok {
	//     errStr := "BarChartWrapper: error displaying chart"
	//     log.Fatal(errStr)
	//     return C.CString(errStr)
	// }
	chartJson, err := json.Marshal(chart)
	// fmt.Println("printing chartJson...")
	// fmt.Println(string(chartJson))
	if err != nil {
		errStr := fmt.Sprintf("ColumnChartWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(chartJson))
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

	var simpleAggs []SimpleAggregation
	if err := json.Unmarshal([]byte(C.GoString(aggsJson)), &simpleAggs); err != nil {
		errStr := fmt.Sprintf("ColumnChartWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	// Reconstruct the Aggregation structs
	var aggs []Aggregation
	for _, simpleAgg := range simpleAggs {
		// Directly use the aggregation functions instead of trying to wrap them
		switch simpleAgg.ColumnName {
		case "Sum":
			aggs = append(aggs, g.Sum(simpleAgg.ColumnName))
		case "Max":
			aggs = append(aggs, g.Max(simpleAgg.ColumnName))
		case "Min":
			aggs = append(aggs, g.Min(simpleAgg.ColumnName))
		case "Mean":
			aggs = append(aggs, g.Mean(simpleAgg.ColumnName))
		case "Median":
			aggs = append(aggs, g.Median(simpleAgg.ColumnName))
		case "Mode":
			aggs = append(aggs, g.Mode(simpleAgg.ColumnName))
		case "Unique":
			aggs = append(aggs, g.Unique(simpleAgg.ColumnName))
		case "First":
			aggs = append(aggs, g.First(simpleAgg.ColumnName))
		default:
			aggs = append(aggs, g.Sum(simpleAgg.ColumnName))
		}
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

// MixedChart (Column + Line)

// SplineChart (apexcharts...)

// REPORTS --------------------------------------------------

// report create

// CreateReportWrapper is an exported function that wraps the CreateReport method.
//
//export CreateReportWrapper
func CreateReportWrapper(title *C.char) *C.char {
	// fmt.Printf("printing dfjson:%s", []byte(C.GoString(dfJson)))
	// fmt.Println("")
	report := g.CreateReport(C.GoString(title))
	// fmt.Printf("printing report:%s", report)
	reportJson, err := json.Marshal(report)
	// fmt.Printf("printing reportJson:%s", reportJson)
	// fmt.Printf("printing stringed reportJson:%s", reportJson)
	if err != nil {
		errStr := fmt.Sprintf("CreateReportWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	reportJsonStr := string(reportJson)
	// fmt.Println("CreateReportWrapper: Created report JSON:", reportJsonStr)
	// fmt.Println("printing reportJson stringed:", reportJsonStr)
	return C.CString(reportJsonStr)
}

// OpenReportWrapper is an exported function that wraps the Open method.
//
//export OpenReportWrapper
func OpenReportWrapper(reportJson *C.char) *C.char {
	var report Report
	if err := json.Unmarshal([]byte(C.GoString(reportJson)), &report); err != nil {
		errStr := fmt.Sprintf("OpenReportWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// fmt.Println("printing report:")
	// fmt.Println(report)
	if err := report.Open(); err != nil {
		errStr := fmt.Sprintf("OpenReportWrapper: open error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString("success")
}

// SaveReportWrapper is an exported function that wraps the Save method.
//
//export SaveReportWrapper
func SaveReportWrapper(reportJson *C.char, filename *C.char) *C.char {
	var report Report
	if err := json.Unmarshal([]byte(C.GoString(reportJson)), &report); err != nil {
		errStr := fmt.Sprintf("SaveReportWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	if err := report.Save(C.GoString(filename)); err != nil {
		errStr := fmt.Sprintf("SaveReportWrapper: save error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString("success")
}

// AddPageWrapper is an exported function that wraps the AddPage method.
//
//export AddPageWrapper
func AddPageWrapper(reportJson *C.char, name *C.char) *C.char {
	var report Report
	if err := json.Unmarshal([]byte(C.GoString(reportJson)), &report); err != nil {
		errStr := fmt.Sprintf("AddPageWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// report.init() // Initialize the maps
	report.AddPage(C.GoString(name))
	// fmt.Println("AddPageWrapper: Report after adding page:", report)
	reportJsonBytes, err := json.Marshal(report)
	if err != nil {
		errStr := fmt.Sprintf("AddPageWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	// fmt.Println("AddPageWrapper: Updated report JSON:", string(reportJsonBytes))
	return C.CString(string(reportJsonBytes))
}

// AddHTMLWrapper is an exported function that wraps the AddHTML method.
//
//export AddHTMLWrapper
func AddHTMLWrapper(reportJson *C.char, page *C.char, text *C.char) *C.char {
	var report Report
	if err := json.Unmarshal([]byte(C.GoString(reportJson)), &report); err != nil {
		errStr := fmt.Sprintf("AddHTMLWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// report.init() // Initialize the maps
	report.AddHTML(C.GoString(page), C.GoString(text))
	reportJsonBytes, err := json.Marshal(report)
	if err != nil {
		errStr := fmt.Sprintf("AddHTMLWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(reportJsonBytes))
}

// AddDataframeWrapper is an exported function that wraps the AddDataframe method.
//
//export AddDataframeWrapper
func AddDataframeWrapper(reportJson *C.char, page *C.char, dfJson *C.char) *C.char {
	var report Report
	if err := json.Unmarshal([]byte(C.GoString(reportJson)), &report); err != nil {
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
	// report.init() // Initialize the maps
	report.AddDataframe(C.GoString(page), &df)
	reportJsonBytes, err := json.Marshal(report)
	if err != nil {
		errStr := fmt.Sprintf("AddDataframeWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(reportJsonBytes))
}

// AddChartWrapper is an exported function that wraps the AddChart method.
//
//export AddChartWrapper
func AddChartWrapper(reportJson *C.char, page *C.char, chartJson *C.char) *C.char {
	var report Report
	if err := json.Unmarshal([]byte(C.GoString(reportJson)), &report); err != nil {
		errStr := fmt.Sprintf("AddChartWrapper: unmarshal error: %v", err)
		return C.CString(errStr)
	}

	var chart Chart
	if err := json.Unmarshal([]byte(C.GoString(chartJson)), &chart); err != nil {
		errStr := fmt.Sprintf("AddChartWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// report.init() // Initialize the maps
	// fmt.Println("adding chart to page...")
	// fmt.Println("chart:", chart)

	report.AddChart(C.GoString(page), chart)

	reportJsonBytes, err := json.Marshal(report)
	if err != nil {
		errStr := fmt.Sprintf("AddChartWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(reportJsonBytes))
}

//export AddHeadingWrapper
func AddHeadingWrapper(reportJson *C.char, page *C.char, heading *C.char, size C.int) *C.char {
	var report Report
	if err := json.Unmarshal([]byte(C.GoString(reportJson)), &report); err != nil {
		errStr := fmt.Sprintf("AddHeadingWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	report.AddHeading(C.GoString(page), C.GoString(heading), int(size))
	reportJsonBytes, err := json.Marshal(report)
	if err != nil {
		errStr := fmt.Sprintf("AddHeadingWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(reportJsonBytes))
}

// AddTextWrapper is an exported function that wraps the AddText method.
//
//export AddTextWrapper
func AddTextWrapper(reportJson *C.char, page *C.char, text *C.char) *C.char {
	var report Report
	if err := json.Unmarshal([]byte(C.GoString(reportJson)), &report); err != nil {
		errStr := fmt.Sprintf("AddTextWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// report.init() // Initialize the maps
	report.AddText(C.GoString(page), C.GoString(text))
	reportJsonBytes, err := json.Marshal(report)
	if err != nil {
		errStr := fmt.Sprintf("AddTextWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(reportJsonBytes))
}

// AddSubTextWrapper is an exported function that wraps the AddSubText method.
//
//export AddSubTextWrapper
func AddSubTextWrapper(reportJson *C.char, page *C.char, text *C.char) *C.char {
	var report Report
	if err := json.Unmarshal([]byte(C.GoString(reportJson)), &report); err != nil {
		errStr := fmt.Sprintf("AddSubTextWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// report.init() // Initialize the maps
	report.AddSubText(C.GoString(page), C.GoString(text))
	reportJsonBytes, err := json.Marshal(report)
	if err != nil {
		errStr := fmt.Sprintf("AddSubTextWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(reportJsonBytes))
}

// AddBulletsWrapper is an exported function that wraps the AddBullets method.
//
//export AddBulletsWrapper
func AddBulletsWrapper(reportJson *C.char, page *C.char, bulletsJson *C.char) *C.char {
	var report Report
	if err := json.Unmarshal([]byte(C.GoString(reportJson)), &report); err != nil {
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
	// report.init() // Initialize the maps
	report.AddBullets(C.GoString(page), bullets...)
	reportJsonBytes, err := json.Marshal(report)
	if err != nil {
		errStr := fmt.Sprintf("AddBulletsWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(reportJsonBytes))
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

	aggs := g.Agg(cols...)
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
	agg := g.Max(C.GoString(name))
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
	agg := g.Min(C.GoString(name))
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
	agg := g.Median(C.GoString(name))
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
	agg := g.Mean(C.GoString(name))
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
	agg := g.Mode(C.GoString(name))
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
	agg := g.Unique(C.GoString(name))
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
	agg := g.First(C.GoString(name))
	aggJson, err := json.Marshal(map[string]string{"ColumnName": agg.ColumnName, "Fn": "First"})
	if err != nil {
		errStr := fmt.Sprintf("FirstWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(aggJson))
}

// LOGIC --------------------------------------------------

// IfWrapper is an exported function that wraps the If function.
// It takes JSON strings representing the condition, fn1, and fn2 Columns, calls If, and returns the resulting Column as a JSON string.
//
//export IfWrapper
func IfWrapper(conditionJson *C.char, fn1Json *C.char, fn2Json *C.char) *C.char {
	var condition, fn1, fn2 Column
	if err := json.Unmarshal([]byte(C.GoString(conditionJson)), &condition); err != nil {
		errStr := fmt.Sprintf("IfWrapper: unmarshal error for condition: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	if err := json.Unmarshal([]byte(C.GoString(fn1Json)), &fn1); err != nil {
		errStr := fmt.Sprintf("IfWrapper: unmarshal error for fn1: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	if err := json.Unmarshal([]byte(C.GoString(fn2Json)), &fn2); err != nil {
		errStr := fmt.Sprintf("IfWrapper: unmarshal error for fn2: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	result := g.If(condition, fn1, fn2)
	resultJson, err := json.Marshal(struct {
		Name string   `json:"Name"`
		Cols []string `json:"Cols"`
	}{
		Name: result.Name,
		Cols: []string{condition.Name, fn1.Name, fn2.Name},
	})
	if err != nil {
		errStr := fmt.Sprintf("IfWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(resultJson))
}

// TRANSFORMS --------------------------------------------------

// ColumnWrapper applies an operation (identified by opName) to the columns
// specified in colsJson (a JSON array of strings) and stores the result in newCol.
// The supported opName cases here are "SHA256" and "SHA512". You can add more operations as needed.
//
//export ColumnWrapper
func ColumnWrapper(dfJson *C.char, newCol *C.char, colSpecJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("Error unmarshalling DataFrame JSON in ColumnOp: %v", err)
	}

	var colSpec ColumnExpr
	if err := json.Unmarshal([]byte(C.GoString(colSpecJson)), &colSpec); err != nil {
		log.Fatalf("Error unmarshalling ColumnExpr JSON in ColumnOp: %v", err)
	}

	newDF := df.Column(C.GoString(newCol), colSpec)
	newJSON, err := json.Marshal(newDF)
	if err != nil {
		log.Fatalf("Error marshalling new DataFrame in ColumnOp: %v", err)
	}
	return C.CString(string(newJSON))
}

// FilterWrapper is an exported function that wraps the Filter method.
// It accepts a JSON string representing the DataFrame and a JSON string representing a Column (the condition).
// It returns the filtered DataFrame as a JSON string.
//
//export FilterWrapper
func FilterWrapper(dfJson *C.char, conditionJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("FilterWrapper: unmarshal error (DataFrame): %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	var expr ColumnExpr
	if err := json.Unmarshal([]byte(C.GoString(conditionJson)), &expr); err != nil {
		errStr := fmt.Sprintf("FilterWrapper: unmarshal error (Condition ColumnExpr): %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// Create a Column with the parsed ColumnExpr.

	newDF := df.Filter(expr)
	resultJson, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("FilterWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(resultJson))
}

// Explode is an exported function that wraps the Explode method.
// It accepts a JSON string representing the DataFrame and a JSON string representing an array of column names to explode.
// It returns the resulting DataFrame as a JSON string.
//
//export Explode
func Explode(dfJson *C.char, colsJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("Explode: unmarshal error (DataFrame): %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	var cols []string
	if err := json.Unmarshal([]byte(C.GoString(colsJson)), &cols); err != nil {
		errStr := fmt.Sprintf("Explode: unmarshal error (columns): %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	newDF := df.Explode(cols...)
	resultJson, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("Explode: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(resultJson))
}

//export RenameWrapper
func RenameWrapper(dfJson *C.char, oldCol *C.char, newCol *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("RenameWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	newDF := df.Rename(C.GoString(oldCol), C.GoString(newCol))
	resultJson, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("RenameWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(resultJson))
}

//export FillNAWrapper
func FillNAWrapper(dfJson *C.char, replacement *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("FillNAWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	newDF := df.FillNA(C.GoString(replacement))
	resultJson, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("FillNAWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(resultJson))
}

//export DropNAWrapper
func DropNAWrapper(dfJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("DropNAWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	newDF := df.DropNA()
	resultJson, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("DropNAWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(resultJson))
}

// The wrapper accepts a JSON string representing an array of column names. If empty,
// then the entire row is used.
//
//export DropDuplicatesWrapper
func DropDuplicatesWrapper(dfJson *C.char, colsJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("DropDuplicatesWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	var cols []string
	if err := json.Unmarshal([]byte(C.GoString(colsJson)), &cols); err != nil {
		// If unmarshalling the columns fails, default to empty slice.
		cols = []string{}
	}
	newDF := df.DropDuplicates(cols...)
	resultJson, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("DropDuplicatesWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(resultJson))
}

// SelectWrapper is an exported function that wraps the Select method.
// It takes a JSON-string representing the DataFrame and a JSON-string representing the column names.
// It returns the resulting DataFrame as a JSON string.
//
//export SelectWrapper
func SelectWrapper(dfJson *C.char, colsJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("SelectWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	var selectedCols []string
	if err := json.Unmarshal([]byte(C.GoString(colsJson)), &selectedCols); err != nil {
		errStr := fmt.Sprintf("SelectWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	selectedDF := df.Select(selectedCols...)
	resultJson, err := json.Marshal(selectedDF)
	if err != nil {
		errStr := fmt.Sprintf("SelectWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(resultJson))
}

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
			aggregations = append(aggregations, g.Sum(colName))
		case "Max":
			aggregations = append(aggregations, g.Max(colName))
		case "Min":
			aggregations = append(aggregations, g.Min(colName))
		case "Mean":
			aggregations = append(aggregations, g.Mean(colName))
		case "Median":
			aggregations = append(aggregations, g.Median(colName))
		case "Mode":
			aggregations = append(aggregations, g.Mode(colName))
		case "Unique":
			aggregations = append(aggregations, g.Unique(colName))
		case "First":
			aggregations = append(aggregations, g.First(colName))
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

// This wrapper accepts two DataFrame JSON strings and join parameters.
//
//export JoinWrapper
func JoinWrapper(leftDfJson *C.char, rightDfJson *C.char, leftOn *C.char, rightOn *C.char, joinType *C.char) *C.char {
	var leftDf, rightDf DataFrame
	if err := json.Unmarshal([]byte(C.GoString(leftDfJson)), &leftDf); err != nil {
		errStr := fmt.Sprintf("JoinWrapper: unmarshal leftDf error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	if err := json.Unmarshal([]byte(C.GoString(rightDfJson)), &rightDf); err != nil {
		errStr := fmt.Sprintf("JoinWrapper: unmarshal rightDf error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	newDF := leftDf.Join(&rightDf, C.GoString(leftOn), C.GoString(rightOn), C.GoString(joinType))
	resultJson, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("JoinWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(resultJson))
}

//export UnionWrapper
func UnionWrapper(leftDfJson *C.char, rightDfJson *C.char) *C.char {
	var leftDf, rightDf DataFrame
	if err := json.Unmarshal([]byte(C.GoString(leftDfJson)), &leftDf); err != nil {
		errStr := fmt.Sprintf("UnionWrapper: unmarshal leftDf error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	if err := json.Unmarshal([]byte(C.GoString(rightDfJson)), &rightDf); err != nil {
		errStr := fmt.Sprintf("UnionWrapper: unmarshal rightDf error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	newDF := leftDf.Union(&rightDf)
	resultJson, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("UnionWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(resultJson))
}

//export DropWrapper
func DropWrapper(dfJson *C.char, colsJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("DropWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	var cols []string
	if err := json.Unmarshal([]byte(C.GoString(colsJson)), &cols); err != nil {
		errStr := fmt.Sprintf("DropWrapper: unmarshal columns error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	newDF := df.Drop(cols...)
	resultJson, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("DropWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(resultJson))
}

//export OrderByWrapper
func OrderByWrapper(dfJson *C.char, column *C.char, asc *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("OrderByWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	// Interpret asc as a boolean. For example, pass "true" for ascending.
	ascStr := strings.ToLower(C.GoString(asc))
	var ascending bool
	if ascStr == "true" {
		ascending = true
	} else {
		ascending = false
	}
	newDF := df.OrderBy(C.GoString(column), ascending)
	resultJson, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("OrderByWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	return C.CString(string(resultJson))
}

// SortWrapper is an exported function that wraps the SortColumns method
// so that it can be called from Python.
//
//export SortWrapper
func SortWrapper(dfJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("SortWrapper: unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	df.Sort() // sort columns alphabetically

	resultJson, err := json.Marshal(df)
	if err != nil {
		errStr := fmt.Sprintf("SortWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(resultJson))
}

// FUNCTIONS --------------------------------------------------

// RETURNS --------------------------------------------------

// ColumnsWrapper returns the DataFrame columns as a JSON array.

//export ColumnsWrapper
func ColumnsWrapper(dfJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("ColumnsWrapper: error unmarshalling DataFrame: %v", err)
	}
	cols := df.Columns()
	colsJSON, err := json.Marshal(cols)
	if err != nil {
		log.Fatalf("ColumnsWrapper: error marshalling columns: %v", err)
	}
	return C.CString(string(colsJSON))
}

// CountWrapper returns the number of rows in the DataFrame.
//
//export CountWrapper
func CountWrapper(dfJson *C.char) C.int {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("CountWrapper: error unmarshalling DataFrame: %v", err)
	}
	return C.int(df.Count())
}

// CountDuplicatesWrapper returns the count of duplicate rows.
// It accepts a JSON array of column names (or an empty array to use all columns).
//
//export CountDuplicatesWrapper
func CountDuplicatesWrapper(dfJson *C.char, colsJson *C.char) C.int {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("CountDuplicatesWrapper: error unmarshalling DataFrame: %v", err)
	}

	var cols []string
	if err := json.Unmarshal([]byte(C.GoString(colsJson)), &cols); err != nil {
		// if not provided or invalid, use all columns
		cols = df.Cols
	}
	dups := df.CountDuplicates(cols...)
	return C.int(dups)
}

// CountDistinctWrapper returns the count of unique rows (or unique values in the provided columns).
// Accepts a JSON array of column names (or an empty array to use all columns).
//
//export CountDistinctWrapper
func CountDistinctWrapper(dfJson *C.char, colsJson *C.char) C.int {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("CountDistinctWrapper: error unmarshalling DataFrame: %v", err)
	}

	var cols []string
	if err := json.Unmarshal([]byte(C.GoString(colsJson)), &cols); err != nil {
		cols = df.Cols
	}
	distinct := df.CountDistinct(cols...)
	return C.int(distinct)
}

// CollectWrapper returns the collected values from a specified column as a JSON-array.
//
//export CollectWrapper
func CollectWrapper(dfJson *C.char, colName *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("CollectWrapper: error unmarshalling DataFrame: %v", err)
	}
	col := C.GoString(colName)
	collected := df.Collect(col)
	result, err := json.Marshal(collected)
	if err != nil {
		log.Fatalf("CollectWrapper: error marshalling collected values: %v", err)
	}
	return C.CString(string(result))
}

// schema of json ?

// SINKS --------------------------------------------------

//export ToCSVFile
func ToCSVFile(dfJson *C.char, filename *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("ToCSVFile: unmarshal error: %v", err)
	}
	err := df.ToCSVFile(C.GoString(filename))
	if err != nil {
		return C.CString(err.Error())
	}
	return C.CString("success")
}

//export WriteSqlite
func WriteSqlite(dbPath *C.char, table *C.char, dfJson *C.char, mode *C.char, keyColsJson *C.char, createIdx C.int) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		return C.CString(fmt.Sprintf("WriteSqlite: dataframe unmarshal error: %v", err))
	}
	var keys []string
	if err := json.Unmarshal([]byte(C.GoString(keyColsJson)), &keys); err != nil && len(C.GoString(keyColsJson)) > 0 {
		return C.CString(fmt.Sprintf("WriteSqlite: key columns unmarshal error: %v", err))
	}
	err := df.WriteSqlite(
		C.GoString(dbPath),
		C.GoString(table),
		C.GoString(mode),
		keys,
		createIdx != 0,
	)
	if err != nil {
		return C.CString(err.Error())
	}
	return C.CString("success")
}

//export PostAPI
func PostAPI(dfJson *C.char, endpoint *C.char, headers *C.char, queryParams *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("PostAPI: dataframe unmarshal error: %v", err)
		return C.CString(errStr)
	}
	ep := C.GoString(endpoint)
	hStr := C.GoString(headers)
	qStr := C.GoString(queryParams)

	// headers: "Key: Value" per line
	h := map[string]string{}
	if hStr != "" {
		for _, line := range strings.Split(hStr, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				k := strings.TrimSpace(parts[0])
				v := strings.TrimSpace(parts[1])
				if k != "" {
					h[k] = v
				}
			}
		}
	}
	// query params: a=b&c=d
	qm := map[string]string{}
	if qStr != "" {
		values, _ := url.ParseQuery(qStr)
		for k, vs := range values {
			if len(vs) > 0 {
				qm[k] = vs[0]
			}
		}
	}

	respBody, err := df.PostAPI(ep, h, qm)
	if err != nil {
		// Return raw body (may contain server error info) plus error note.
		errWrap := fmt.Sprintf("error: %v\n%s", err, respBody)
		return C.CString(errWrap)
	}
	return C.CString(respBody)
}

// END --------------------------------------------------

func main() {}
