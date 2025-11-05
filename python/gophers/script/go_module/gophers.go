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
	"reflect"
	"runtime"
	"sort"
	"html"
	"gopkg.in/yaml.v2"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"time"
	"sync"
	"unsafe"
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

// MarshalJSON custom marshaller to exclude the function field.
func (c Column) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Name string `json:"Name"`
	}{
		Name: c.Name,
	})
}

// UnmarshalJSON custom unmarshaller to handle the function field.
func (c *Column) UnmarshalJSON(data []byte) error {
	var aux struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	c.Name = aux.Name
	// Note: The function field cannot be unmarshalled from JSON.
	return nil
}

type ColumnExpr struct {
	Type      string          `json:"type"`
	Name      string          `json:"name,omitempty"`
	Value     interface{}     `json:"value,omitempty"`
	Expr      json.RawMessage `json:"expr,omitempty"`
	Left      json.RawMessage `json:"left,omitempty"`
	Right     json.RawMessage `json:"right,omitempty"`
	Cond      json.RawMessage `json:"cond,omitempty"`
	True      json.RawMessage `json:"true,omitempty"`
	False     json.RawMessage `json:"false,omitempty"`
	Cols      json.RawMessage `json:"cols,omitempty"`
	Col       string          `json:"col,omitempty"`
	Delimiter string          `json:"delimiter,omitempty"`
	Datatype  string          `json:"datatype,omitempty"`
}

func Evaluate(expr ColumnExpr, row map[string]interface{}) interface{} {
	switch expr.Type {
	case "col":
		return row[expr.Name]
	case "lit":
		return expr.Value
	case "isnull":
		// Check if the sub-expression is provided.
		if len(expr.Expr) == 0 {
			return true // or false depending on how you want to handle it
		}
		var subExpr ColumnExpr
		json.Unmarshal(expr.Expr, &subExpr)
		val := Evaluate(subExpr, row)
		if val == nil {
			return true
		}
		switch v := val.(type) {
		case string:
			return v == "" || strings.ToLower(v) == "null"
		case *string:
			return v == nil || *v == "" || strings.ToLower(*v) == "null"
		default:
			return false
		}
	case "isnotnull":
		if len(expr.Expr) == 0 {
			return false
		}
		var subExpr ColumnExpr
		json.Unmarshal(expr.Expr, &subExpr)
		val := Evaluate(subExpr, row)
		if val == nil {
			return true
		}
		switch v := val.(type) {
		case string:
			return !(v == "" || strings.ToLower(v) == "null")
		case *string:
			return !(v == nil || *v == "" || strings.ToLower(*v) == "null")
		default:
			return true
		}
	case "gt":
		var leftExpr, rightExpr ColumnExpr
		json.Unmarshal(expr.Left, &leftExpr)
		json.Unmarshal(expr.Right, &rightExpr)
		return Evaluate(leftExpr, row).(float64) > Evaluate(rightExpr, row).(float64)
	case "lt":
		var leftExpr, rightExpr ColumnExpr
		json.Unmarshal(expr.Left, &leftExpr)
		json.Unmarshal(expr.Right, &rightExpr)
		return Evaluate(leftExpr, row).(float64) < Evaluate(rightExpr, row).(float64)
	case "le":
		var leftExpr, rightExpr ColumnExpr
		json.Unmarshal(expr.Left, &leftExpr)
		json.Unmarshal(expr.Right, &rightExpr)
		return Evaluate(leftExpr, row).(float64) <= Evaluate(rightExpr, row).(float64)
	case "ge":
		var leftExpr, rightExpr ColumnExpr
		json.Unmarshal(expr.Left, &leftExpr)
		json.Unmarshal(expr.Right, &rightExpr)
		return Evaluate(leftExpr, row).(float64) >= Evaluate(rightExpr, row).(float64)
	case "eq":
		var leftExpr, rightExpr ColumnExpr
		json.Unmarshal(expr.Left, &leftExpr)
		json.Unmarshal(expr.Right, &rightExpr)
		return Evaluate(leftExpr, row).(float64) == Evaluate(rightExpr, row).(float64)
	case "ne":
		var leftExpr, rightExpr ColumnExpr
		json.Unmarshal(expr.Left, &leftExpr)
		json.Unmarshal(expr.Right, &rightExpr)
		return Evaluate(leftExpr, row).(float64) != Evaluate(rightExpr, row).(float64)
	case "or":
		var leftExpr, rightExpr ColumnExpr
		json.Unmarshal(expr.Left, &leftExpr)
		json.Unmarshal(expr.Right, &rightExpr)
		return Evaluate(leftExpr, row).(bool) || Evaluate(rightExpr, row).(bool)
	case "and":
		var leftExpr, rightExpr ColumnExpr
		json.Unmarshal(expr.Left, &leftExpr)
		json.Unmarshal(expr.Right, &rightExpr)
		return Evaluate(leftExpr, row).(bool) && Evaluate(rightExpr, row).(bool)
	case "if":
		var condExpr, trueExpr, falseExpr ColumnExpr
		json.Unmarshal(expr.Cond, &condExpr)
		json.Unmarshal(expr.True, &trueExpr)
		json.Unmarshal(expr.False, &falseExpr)
		if Evaluate(condExpr, row).(bool) {
			return Evaluate(trueExpr, row)
		} else {
			return Evaluate(falseExpr, row)
		}
	case "sha256":
		var cols []ColumnExpr
		json.Unmarshal(expr.Cols, &cols)
		var values []string
		for _, col := range cols {
			values = append(values, fmt.Sprintf("%v", Evaluate(col, row)))
		}
		return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(values, ""))))
	case "sha512":
		var cols []ColumnExpr
		json.Unmarshal(expr.Cols, &cols)
		var values []string
		for _, col := range cols {
			values = append(values, fmt.Sprintf("%v", Evaluate(col, row)))
		}
		return fmt.Sprintf("%x", sha512.Sum512([]byte(strings.Join(values, ""))))
	case "collectlist":
		colName := expr.Col
		return row[colName]
	case "collectset":
		colName := expr.Col
		return row[colName]
	case "split":
		colName := expr.Col
		delimiter := expr.Delimiter
		val := row[colName].(string)
		return strings.Split(val, delimiter)
	case "concat":
		// "concat_ws" expects a "Delimiter" field (string) and a "Cols" JSON array.
		delim := expr.Delimiter
		var cols []ColumnExpr
		if err := json.Unmarshal(expr.Cols, &cols); err != nil {
			fmt.Printf("concat_ws unmarshal error: %v\n", err)
			return ""
		}
		var parts []string
		for _, col := range cols {
			parts = append(parts, fmt.Sprintf("%v", Evaluate(col, row)))
		}
		return strings.Join(parts, delim)
	case "cast":
		// "cast" expects a "Col" field with a JSON object and a "Datatype" field.
		var subExpr ColumnExpr
		if err := json.Unmarshal([]byte(expr.Col), &subExpr); err != nil {
			fmt.Printf("cast unmarshal error (sub expression): %v\n", err)
			return nil
		}
		datatype := expr.Datatype
		val := Evaluate(subExpr, row)
		switch datatype {
		case "int":
			casted, err := toInt(val)
			if err != nil {
				fmt.Printf("cast to int error: %v\n", err)
				return nil
			}
			return casted
		case "float":
			casted, err := toFloat64(val)
			if err != nil {
				fmt.Printf("cast to float error: %v\n", err)
				return nil
			}
			return casted
		case "string":
			casted, err := toString(val)
			if err != nil {
				fmt.Printf("cast to string error: %v\n", err)
				return nil
			}
			return casted
		default:
			fmt.Printf("unsupported cast type: %s\n", datatype)
			return nil
		}
	case "arrays_zip":
		// "arrays_zip" expects a "Cols" field with a JSON array of column names.
		var cols []ColumnExpr
		if err := json.Unmarshal(expr.Cols, &cols); err != nil {
			fmt.Printf("arrays_zip unmarshal error: %v\n", err)
			return nil
		}
		var zipped []interface{}
		for _, col := range cols {
			zipped = append(zipped, Evaluate(col, row))
		}
		return zipped
	case "keys":
		colName := expr.Col
		var keys []string
		val := row[colName]
		if val == nil {
			return keys
		}
		switch t := val.(type) {
		case map[string]interface{}:
			for k := range t {
				keys = append(keys, k)
			}
		case map[interface{}]interface{}:
			nested := convertMapKeysToString(t)
			for k := range nested {
				keys = append(keys, k)
			}
		default:
			return keys
		}
		return keys
	case "lookup":
		// Evaluate the key expression from the Left field.
		var keyExpr ColumnExpr
		if err := json.Unmarshal(expr.Left, &keyExpr); err != nil {
			return nil
		}
		keyInterf := Evaluate(keyExpr, row)
		keyStr, err := toString(keyInterf)
		if err != nil {
			return nil
		}
		// fmt.Printf("Lookup key: %s\n", keyStr)

		// Evaluate the nested map expression from the Right field.
		var nestedExpr ColumnExpr
		if err := json.Unmarshal(expr.Right, &nestedExpr); err != nil {
			return nil
		}
		nestedInterf := Evaluate(nestedExpr, row)
		// fmt.Printf("Nested value: %#v\n", nestedInterf)
		if nestedInterf == nil {
			return nil
		}

		switch t := nestedInterf.(type) {
		case map[string]interface{}:
			return t[keyStr]
		case map[interface{}]interface{}:
			m := convertMapKeysToString(t)
			return m[keyStr]
		default:
			return nil
		}

	default:
		return nil
	}
}

// add other methods that modify the chart (no menu icon, no horizontal lines, highcharts vs apexcharts, colors, etc)?
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

// Report object for adding html pages, charts, and inputs for a single html output
type Report struct {
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
	Pageshtml     map[string]map[string]string
	Pagesjs       map[string]map[string]string
}

func (report *Report) init() {
	if report.Pageshtml == nil {
		report.Pageshtml = make(map[string]map[string]string)
	}
	if report.Pagesjs == nil {
		report.Pagesjs = make(map[string]map[string]string)
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
	// log.Printf("ReadJSON: Input string: %s", goJsonStr) // Log the input string

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

	// Trim whitespace and check if jsonContent starts with "{".
	trimmed := strings.TrimSpace(jsonContent)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		// Wrap single JSON object into an array.
		jsonContent = "[" + jsonContent + "]"
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

// ReadYAML reads a YAML string or file and converts it to a DataFrame.
//
//export ReadYAML
func ReadYAML(yamlStr *C.char) *C.char {
	if yamlStr == nil {
		log.Fatalf("Error: yamlStr is nil")
		return C.CString("")
	}

	goYamlStr := C.GoString(yamlStr)
	// log.Printf("ReadYAML: Input string: %s", goYamlStr) // Log the input string

	var yamlContent string

	// Check if the input is a file path.
	if fileExists(goYamlStr) {
		bytes, err := os.ReadFile(goYamlStr)
		if err != nil {
			log.Fatalf("Error reading file: %v", err)
		}
		yamlContent = string(bytes)
	} else {
		yamlContent = goYamlStr
	}

	// Unmarshal the YAML string into a generic map
	var data map[interface{}]interface{}
	if err := yaml.Unmarshal([]byte(yamlContent), &data); err != nil {
		log.Fatalf("Error unmarshalling YAML: %v", err)
	}

	// fmt.Println("printing yaml unmarshalled data...")
	// fmt.Println(data)

	// Convert the map to a slice of maps with string keys
	rows := mapToRows(convertMapKeysToString(data))
	df := Dataframe(rows)
	jsonBytes, err := json.Marshal(df)
	if err != nil {
		log.Fatalf("Error marshalling DataFrame to JSON: %v", err)
	}

	return C.CString(string(jsonBytes))
}

func fetchRows(db *sql.DB, query string, tableLabel string) ([]map[string]interface{}, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	out := make([]map[string]interface{}, 0, 128)
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(cols)+1)
		for i, c := range cols {
			v := vals[i]
			switch t := v.(type) {
			case []byte:
				row[c] = string(t)
			case time.Time:
				row[c] = t.Format(time.RFC3339Nano)
			default:
				row[c] = v
			}
		}
		if tableLabel != "" {
			row["_table"] = tableLabel
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
	
//export ReadSqlite
func ReadSqlite(dbPath *C.char, table *C.char, query *C.char) *C.char {
	path := C.GoString(dbPath)
	tbl := C.GoString(table)
	q := C.GoString(query)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatalf("ReadSqlite: open error: %v", err)
	}
	defer db.Close()

	rows := []map[string]interface{}{}
	switch {
	case q != "":
		rs, err := fetchRows(db, q, "")
		if err != nil {
			log.Fatalf("ReadSqlite: query error: %v", err)
		}
		rows = append(rows, rs...)
	case tbl != "":
		q = fmt.Sprintf(`SELECT * FROM %q`, tbl)
		rs, err := fetchRows(db, q, "")
		if err != nil {
			log.Fatalf("ReadSqlite: table read error: %v", err)
		}
		rows = append(rows, rs...)
	default:
		// read all user tables
		names := []string{}
		r, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
		if err != nil {
			log.Fatalf("ReadSqlite: list tables error: %v", err)
		}
		for r.Next() {
			var name string
			if err := r.Scan(&name); err == nil {
				names = append(names, name)
			}
		}
		_ = r.Close()
		for _, name := range names {
			rs, err := fetchRows(db, fmt.Sprintf(`SELECT * FROM %q`, name), name)
			if err != nil {
				log.Fatalf("ReadSqlite: read table %s error: %v", name, err)
			}
			rows = append(rows, rs...)
		}
	}

	df := Dataframe(rows)
	jsonBytes, err := json.Marshal(df)
	if err != nil {
		log.Fatalf("ReadSqlite: marshal error: %v", err)
	}
	return C.CString(string(jsonBytes))
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
    path := C.GoString(dbPath)
    tbl := C.GoString(table)
    if tbl == "" {
        return C.CString(`{"error":"table is required"}`)
    }

    db, err := sql.Open("sqlite3", path)
    if err != nil {
        return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("open error: %v", err)))
    }
    defer db.Close()

    // ensure table exists
    var cnt int
    if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, tbl).Scan(&cnt); err != nil {
        return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("exists check error: %v", err)))
    }
    if cnt == 0 {
        return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("table %s not found", tbl)))
    }

    // columns
    cols := []map[string]interface{}{}
    qCols := `PRAGMA table_info(` + quoteIdent(tbl) + `)`
    if rows, err := db.Query(qCols); err == nil {
        defer rows.Close()
        for rows.Next() {
            var cid int
            var name, ctype string
            var notnull, pk int
            var dflt sql.NullString
            if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
                return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("table_info scan error: %v", err)))
            }
            var dfltVal interface{}
            if dflt.Valid {
                dfltVal = dflt.String
            } else {
                dfltVal = nil
            }
            cols = append(cols, map[string]interface{}{
                "cid":        cid,
                "name":       name,
                "type":       ctype,
                "notnull":    notnull,
                "default":    dfltVal,
                "primaryKey": pk,
            })
        }
        if err := rows.Err(); err != nil {
            return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("table_info rows error: %v", err)))
        }
    } else {
        return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("table_info error: %v", err)))
    }

    // foreign keys
    fks := []map[string]interface{}{}
    qFK := `PRAGMA foreign_key_list(` + quoteIdent(tbl) + `)`
    if rows, err := db.Query(qFK); err == nil {
        defer rows.Close()
        for rows.Next() {
            var id, seq int
            var refTable, from, to, onUpdate, onDelete, match string
            if err := rows.Scan(&id, &seq, &refTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
                return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("fk scan error: %v", err)))
            }
            fks = append(fks, map[string]interface{}{
                "id":        id,
                "seq":       seq,
                "table":     refTable,
                "from":      from,
                "to":        to,
                "on_update": onUpdate,
                "on_delete": onDelete,
                "match":     match,
            })
        }
        if err := rows.Err(); err != nil {
            return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("fk rows error: %v", err)))
        }
    } // ignore fk pragma error (older builds may differ)

    // indexes
    indexes := []map[string]interface{}{}
    qIdx := `PRAGMA index_list(` + quoteIdent(tbl) + `)`
    if rows, err := db.Query(qIdx); err == nil {
        defer rows.Close()
        for rows.Next() {
            // seq, name, unique, origin, partial (partial only on newer versions)
            var seq, unique int
            var name, origin string
            var partial sql.NullInt64
            // try scanning 5 cols; if that fails, scan 4 (older sqlite)
            errScan := rows.Scan(&seq, &name, &unique, &origin, &partial)
            if errScan != nil {
                // fallback 4 cols
                rows2, err2 := db.Query(qIdx)
                if err2 != nil {
                    return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("index_list requery error: %v", err2)))
                }
                defer rows2.Close()
                indexes = []map[string]interface{}{}
                for rows2.Next() {
                    var seq2, unique2 int
                    var name2, origin2 string
                    if err := rows2.Scan(&seq2, &name2, &unique2, &origin2); err != nil {
                        return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("index_list scan error: %v", err)))
                    }
                    colsForIdx := []string{}
                    if r2, err := db.Query(`PRAGMA index_info(` + quoteIdent(name2) + `)`); err == nil {
                        for r2.Next() {
                            var seqno, cid int
                            var colName string
                            if err := r2.Scan(&seqno, &cid, &colName); err == nil {
                                colsForIdx = append(colsForIdx, colName)
                            }
                        }
                        _ = r2.Close()
                    }
                    indexes = append(indexes, map[string]interface{}{
                        "name":    name2,
                        "unique":  unique2,
                        "origin":  origin2,
                        "partial": false,
                        "columns": colsForIdx,
                    })
                }
                if err := rows2.Err(); err != nil {
                    return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("index_list rows error: %v", err)))
                }
                // done (we rebuilt indexes list using fallback path)
                goto payload
            }
            // happy path (5 cols)
            colsForIdx := []string{}
            if r2, err := db.Query(`PRAGMA index_info(` + quoteIdent(name) + `)`); err == nil {
                for r2.Next() {
                    var seqno, cid int
                    var colName string
                    if err := r2.Scan(&seqno, &cid, &colName); err == nil {
                        colsForIdx = append(colsForIdx, colName)
                    }
                }
                _ = r2.Close()
            }
            indexes = append(indexes, map[string]interface{}{
                "name":    name,
                "unique":  unique,
                "origin":  origin,
                "partial": partial.Valid && partial.Int64 != 0,
                "columns": colsForIdx,
            })
        }
        if err := rows.Err(); err != nil {
            return C.CString(fmt.Sprintf(`{"error":%q}`, fmt.Sprintf("index_list rows error: %v", err)))
        }
    }
payload:
    out := map[string]interface{}{
        "table":        tbl,
        "columns":      cols,
        "foreign_keys": fks,
        "indexes":      indexes,
    }
    js, _ := json.Marshal(out)
    return C.CString(string(js))
}

// Clone creates a deep copy of the DataFrame (new Cols slice and new per-column []interface{}).
func (df *DataFrame) Clone() *DataFrame {
    if df == nil {
        return &DataFrame{Cols: nil, Data: make(map[string][]interface{}), Rows: 0}
    }
    newCols := make([]string, len(df.Cols))
    copy(newCols, df.Cols)

    newData := make(map[string][]interface{}, len(df.Data))
    for _, c := range df.Cols {
        src := df.Data[c]
        dst := make([]interface{}, len(src))
        copy(dst, src)
        newData[c] = dst
    }

    return &DataFrame{
        Cols: newCols,
        Data: newData,
        Rows: df.Rows,
    }
}

//export CloneWrapper
func CloneWrapper(dfJson *C.char) *C.char {
    var df DataFrame
    if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
        errStr := fmt.Sprintf("CloneWrapper: unmarshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    }
    cloned := df.Clone()
    js, err := json.Marshal(cloned)
    if err != nil {
        errStr := fmt.Sprintf("CloneWrapper: marshal error: %v", err)
        log.Fatal(errStr)
        return C.CString(errStr)
    }
    return C.CString(string(js))
}

// convertMapKeysToString converts map keys to strings recursively
func convertMapKeysToString(data map[interface{}]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range data {
		strKey := fmt.Sprintf("%v", k)
		switch v := v.(type) {
		case map[interface{}]interface{}:
			result[strKey] = convertMapKeysToString(v)
		default:
			result[strKey] = v
		}
	}
	return result
}

// mapToRows converts a nested map to a slice of maps
func mapToRows(data map[string]interface{}) []map[string]interface{} {
	rows := []map[string]interface{}{data}
	// flattenMap(data, "", &rows)
	return rows
}

// flattenNestedMap recursively flattens a nested map.
// It prefixes keys with the given prefix and a dot.
func flattenNestedMap(m map[string]interface{}, prefix string) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		flatKey := prefix + "." + k
		switch child := v.(type) {
		case map[string]interface{}:
			nested := flattenNestedMap(child, flatKey)
			for nk, nv := range nested {
				result[nk] = nv
			}
		default:
			result[flatKey] = v
		}
	}
	return result
}

// FlattenWrapper accepts a JSON string for the DataFrame and a JSON array of column names to flatten.
//
//export FlattenWrapper
func FlattenWrapper(dfJson *C.char, flattenColsJson *C.char) *C.char {
	// Unmarshal the DataFrame.
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("FlattenWrapper: DataFrame unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	// Unmarshal the flatten columns (JSON array of strings).
	var flattenCols []string
	if err := json.Unmarshal([]byte(C.GoString(flattenColsJson)), &flattenCols); err != nil {
		errStr := fmt.Sprintf("FlattenWrapper: flattenCols unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	// Call the Flatten method.
	newDF := df.Flatten(flattenCols)

	// Marshal the new DataFrame to JSON.
	jsonBytes, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("FlattenWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(jsonBytes))
}

// Flatten is a DataFrame method that takes a slice of column names.
// For each row, if any specified column contains a nested map (or map[interface{}]interface{}),
// it will flatten that nested structure using dot-notation
// and add those flattened fields as new columns while removing the original column.
// func (df *DataFrame) Flatten(flattenCols []string) *DataFrame {
// 	newRows := []map[string]interface{}{}
// 	// Iterate over each row.
// 	for i := 0; i < df.Rows; i++ {
// 		// Create a copy of the row.
// 		row := make(map[string]interface{})
// 		for _, col := range df.Cols {
// 			row[col] = df.Data[col][i]
// 		}
// 		// Process each column that should be flattened.
// 		for _, fcol := range flattenCols {
// 			val, exists := row[fcol]
// 			if !exists || val == nil {
// 				continue
// 			}
// 			var nested map[string]interface{}
// 			switch t := val.(type) {
// 			case map[string]interface{}:
// 				nested = t
// 			case map[interface{}]interface{}:
// 				nested = convertMapKeysToString(t)
// 			default:
// 				// Not a map; skip.
// 				continue
// 			}
// 			// Flatten the map; use the column name as prefix.
// 			flatMap := flattenNestedMap(nested, fcol)
// 			// Remove the original nested column.
// 			delete(row, fcol)
// 			// Merge flattened key/value pairs into the row.
// 			for k, v := range flatMap {
// 				row[k] = v
// 			}
// 		}
// 		newRows = append(newRows, row)
// 	}
// 	// Return a new DataFrame built from the new rows.
// 	return Dataframe(newRows)
// }

// ---- Memory guard helpers ----
var memBudgetBytes int64 = func() int64 {
    // Set GOPHERS_MEM_BUDGET_BYTES to a number of bytes to enforce a soft cap.
    // 0 disables the guard.
    if v := os.Getenv("GOPHERS_MEM_BUDGET_BYTES"); v != "" {
        if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
            return n
        }
    }
    return 0 // disabled by default
}()

func ifaceSize() int64 {
    var x interface{}
    return int64(unsafe.Sizeof(x)) // typically 16 bytes on 64-bit
}

func humanBytes(n int64) string {
    units := []string{"B","KiB","MiB","GiB","TiB"}
    f := float64(n)
    i := 0
    for f >= 1024 && i < len(units)-1 {
        f /= 1024
        i++
    }
    return fmt.Sprintf("%.1f %s", f, units[i])
}

func estimateInterfacesExplode(df *DataFrame, totalRows int) int64 {
    // New arrays of []interface{} for each column with len=totalRows.
    return int64(totalRows) * int64(len(df.Cols))
}

func estimateInterfacesFlattenAdd(rows, addedCols int) int64 {
    // New arrays for the added columns only.
    return int64(rows) * int64(addedCols)
}

func estimatedBytesForInterfaces(nInterfaces int64) int64 {
    // Factor in slice headers and small temps (~20% headroom).
    return int64(float64(nInterfaces*ifaceSize()) * 1.2)
}

func checkMemBudget(needBytes int64, context string) error {
    if memBudgetBytes <= 0 {
        return nil // guard disabled
    }
    var ms runtime.MemStats
    runtime.ReadMemStats(&ms)
    used := int64(ms.Alloc)
    // Simple check: current heap + estimated allocation vs budget
    if used+needBytes > memBudgetBytes {
        return fmt.Errorf("%s estimated to allocate ~%s; current heap ~%s; exceeds budget %s (set GOPHERS_MEM_BUDGET_BYTES to raise)",
            context, humanBytes(needBytes), humanBytes(used), humanBytes(memBudgetBytes))
    }
    return nil
}

// Optional: public preflight helpers you can call from Python to warn users early.
func (df *DataFrame) ExplodePreflight(column string) (totalRows int, estBytes int64) {
    total := 0
    for i := 0; i < df.Rows; i++ {
        v := df.Data[column][i]
        if arr, ok := v.([]interface{}); ok && len(arr) > 0 {
            total += len(arr)
        } else {
            total++
        }
    }
    if total == 0 {
        total = df.Rows
    }
    needIface := estimateInterfacesExplode(df, total)
    return total, estimatedBytesForInterfaces(needIface)
}

func (df *DataFrame) FlattenPreflight(nestedCol string) (addedCols int, estBytes int64) {
    src, ok := df.Data[nestedCol]
    if !ok {
        return 0, 0
    }
    keySet := map[string]struct{}{}
    for i := 0; i < df.Rows; i++ {
        v := src[i]
        switch t := v.(type) {
        case map[string]interface{}:
            for k := range t { keySet[k] = struct{}{} }
        case map[interface{}]interface{}:
            m := convertMapKeysToString(t)
            for k := range m { keySet[k] = struct{}{} }
        }
    }
    add := len(keySet)
    needIface := estimateInterfacesFlattenAdd(df.Rows, add)
    return add, estimatedBytesForInterfaces(needIface)
}

// Flatten (in-place, no []map intermediate)
func (df *DataFrame) Flatten(flattenCols []string) *DataFrame {
    if df == nil || df.Rows == 0 || len(flattenCols) == 0 {
        return df
    }
    // Build a set for quick column lookup
    colSet := make(map[string]bool, len(df.Cols))
    for _, c := range df.Cols { colSet[c] = true }

    for _, fcol := range flattenCols {
        // Collect all nested keys once
        keySet := map[string]struct{}{}
        srcCol, ok := df.Data[fcol]
        if !ok { continue }
        for i := 0; i < df.Rows; i++ {
            val := srcCol[i]
            if val == nil { continue }
            switch t := val.(type) {
            case map[string]interface{}:
                for k := range t { keySet[k] = struct{}{} }
            case map[interface{}]interface{}:
                m := convertMapKeysToString(t)
                for k := range m { keySet[k] = struct{}{} }
            }
        }
        // Preflight memory guard for added columns.
        if add := len(keySet); add > 0 {
            need := estimatedBytesForInterfaces(estimateInterfacesFlattenAdd(df.Rows, add))
            if err := checkMemBudget(need, fmt.Sprintf("Flatten(%s)", fcol)); err != nil {
                log.Printf("WARN: %v. Aborting Flatten for column %q.", err, fcol)
                return df
            }
        }		
        // Create new columns with full length
        newCols := make([]string, 0, len(keySet))
        for k := range keySet {
            name := fcol + "." + k
            if !colSet[name] {
                df.Cols = append(df.Cols, name)
                colSet[name] = true
            }
            newCols = append(newCols, name)
            if _, ok := df.Data[name]; !ok {
                df.Data[name] = make([]interface{}, df.Rows)
            }
        }
        // Parallel fill
        w := runtime.GOMAXPROCS(0)
        var wg sync.WaitGroup
        chunk := (df.Rows + w - 1) / w
        for g := 0; g < w; g++ {
            start := g * chunk
            end := start + chunk
            if start >= df.Rows { break }
            if end > df.Rows { end = df.Rows }
            wg.Add(1)
            go func(s, e int) {
                defer wg.Done()
                for i := s; i < e; i++ {
                    val := srcCol[i]
                    var nested map[string]interface{}
                    switch t := val.(type) {
                    case map[string]interface{}:
                        nested = t
                    case map[interface{}]interface{}:
                        nested = convertMapKeysToString(t)
                    default:
                        nested = nil
                    }
                    for _, name := range newCols {
                        key := name[len(fcol)+1:]
                        if nested != nil {
                            df.Data[name][i] = nested[key]
                        } else {
                            df.Data[name][i] = nil
                        }
                    }
                }
            }(start, end)
        }
        wg.Wait()
        // Remove original nested column (single-threaded)
        delete(df.Data, fcol)
        kept := df.Cols[:0]
        for _, c := range df.Cols {
            if c != fcol { kept = append(kept, c) }
        }
        df.Cols = kept
    }
    return df
}

// explodePrealloc explodes a single column using preallocation
func (df *DataFrame) explodePrealloc(column string) *DataFrame {
    if df == nil || df.Rows == 0 { return df }

    // Per-row output lengths
    perLen := make([]int, df.Rows)
    total := 0
    for i := 0; i < df.Rows; i++ {
        v := df.Data[column][i]
        if arr, ok := v.([]interface{}); ok && len(arr) > 0 {
            perLen[i] = len(arr)
        } else {
            perLen[i] = 1 // copy row as-is if not array or empty
        }
        total += perLen[i]
    }
    if total == 0 { total = df.Rows }

    // Preflight memory guard for the expanded arrays.
    need := estimatedBytesForInterfaces(estimateInterfacesExplode(df, total))
    if err := checkMemBudget(need, fmt.Sprintf("Explode(%s)", column)); err != nil {
        log.Printf("WARN: %v. Aborting Explode for column %q.", err, column)
        return df
    }

    // Prefix sums -> starting offsets
    offsets := make([]int, df.Rows+1)
    for i := 0; i < df.Rows; i++ {
        offsets[i+1] = offsets[i] + perLen[i]
    }

    // Allocate output
    newCols := make([]string, len(df.Cols))
    copy(newCols, df.Cols)
    newData := make(map[string][]interface{}, len(newCols))
    for _, c := range newCols {
        newData[c] = make([]interface{}, total)
    }

    // Parallel fill by disjoint index ranges per row
    w := runtime.GOMAXPROCS(0)
    var wg sync.WaitGroup
    chunk := (df.Rows + w - 1) / w
    for g := 0; g < w; g++ {
        start := g * chunk
        end := start + chunk
        if start >= df.Rows { break }
        if end > df.Rows { end = df.Rows }
        wg.Add(1)
        go func(s, e int) {
            defer wg.Done()
            for i := s; i < e; i++ {
                base := offsets[i]
                v := df.Data[column][i]
                if arr, ok := v.([]interface{}); ok && len(arr) > 0 {
                    for j, item := range arr {
                        outIdx := base + j
                        for _, c := range newCols {
                            if c == column {
                                newData[c][outIdx] = item
                            } else {
                                newData[c][outIdx] = df.Data[c][i]
                            }
                        }
                    }
                } else {
                    outIdx := base
                    for _, c := range newCols {
                        newData[c][outIdx] = df.Data[c][i]
                    }
                }
            }
        }(start, end)
    }
    wg.Wait()

    df.Cols = newCols
    df.Data = newData
    df.Rows = total
    return df 
}

// Explode (variadic): explode multiple columns sequentially using preallocation
func (df *DataFrame) Explode(columns ...string) *DataFrame {
    for _, col := range columns {
        df = df.explodePrealloc(col)
    }
    return df
}

// KeysToColsWrapper accepts a JSON string for the DataFrame and a column name (as a plain C string).
// It converts any nested map in that column into separate columns and returns the updated DataFrame as JSON.
//
//export KeysToColsWrapper
func KeysToColsWrapper(dfJson *C.char, nestedCol *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("KeysToColsWrapper: DataFrame unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}
	newDF := df.KeysToCols(C.GoString(nestedCol))
	jsonBytes, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("KeysToColsWrapper: marshal error: %v", err)
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

// // KeysToCols turns the keys of a nested map in column nestedCol into separate columns.
// // It flattens only one level by using flattenOnce.
// func (df *DataFrame) KeysToCols(nestedCol string) *DataFrame {
// 	newRows := []map[string]interface{}{}
// 	// Iterate over each row.
// 	for i := 0; i < df.Rows; i++ {
// 		row := make(map[string]interface{})
// 		for _, col := range df.Cols {
// 			row[col] = df.Data[col][i]
// 		}
// 		// Process the specified nested column.
// 		val, exists := row[nestedCol]
// 		if !exists || val == nil {
// 			newRows = append(newRows, row)
// 			continue
// 		}
// 		var nested map[string]interface{}
// 		switch t := val.(type) {
// 		case map[string]interface{}:
// 			nested = t
// 		case map[interface{}]interface{}:
// 			nested = convertMapKeysToString(t)
// 		default:
// 			newRows = append(newRows, row)
// 			continue
// 		}
// 		// Flatten only one level from the nested map.
// 		flatMap := flattenOnce(nested, nestedCol)
// 		for k, v := range flatMap {
// 			row[k] = v
// 		}
// 		// Remove the original nested column.
// 		delete(row, nestedCol)
// 		newRows = append(newRows, row)
// 	}
// 	// Construct a new DataFrame from the updated rows.
// 	return Dataframe(newRows)
// }
// KeysToCols turns the keys of a nested map in column nestedCol into separate columns.
// It flattens only one level, in-place (no []map materialization).
func (df *DataFrame) KeysToCols(nestedCol string) *DataFrame {
    if df == nil || df.Rows == 0 {
        return df
    }
    // Source column must exist
    src, ok := df.Data[nestedCol]
    if !ok {
        return df
    }

    // Discover all keys we need to create (one pass).
    keySet := make(map[string]struct{})
    for i := 0; i < df.Rows; i++ {
        v := src[i]
        if v == nil {
            continue
        }
        switch t := v.(type) {
        case map[string]interface{}:
            for k := range t {
                keySet[k] = struct{}{}
            }
        case map[interface{}]interface{}:
            m := convertMapKeysToString(t)
            for k := range m {
                keySet[k] = struct{}{}
            }
        }
    }

    // No nested keys found; just remove nestedCol and return.
    if len(keySet) == 0 {
        // drop the column if present
        delete(df.Data, nestedCol)
        kept := df.Cols[:0]
        for _, c := range df.Cols {
            if c != nestedCol {
                kept = append(kept, c)
            }
        }
        df.Cols = kept
        return df
    }

    // Ensure new columns exist and are preallocated.
    colSet := make(map[string]bool, len(df.Cols))
    for _, c := range df.Cols {
        colSet[c] = true
    }
    newCols := make([]string, 0, len(keySet))
    for k := range keySet {
        name := nestedCol + "." + k
        newCols = append(newCols, name)
        if !colSet[name] {
            df.Cols = append(df.Cols, name)
            colSet[name] = true
        }
        if _, exists := df.Data[name]; !exists {
            df.Data[name] = make([]interface{}, df.Rows)
        }
    }

    // Fill new columns (second pass).
    for i := 0; i < df.Rows; i++ {
        var m map[string]interface{}
        switch t := src[i].(type) {
        case map[string]interface{}:
            m = t
        case map[interface{}]interface{}:
            m = convertMapKeysToString(t)
        default:
            m = nil
        }
        for _, name := range newCols {
            key := name[len(nestedCol)+1:]
            if m != nil {
                df.Data[name][i] = m[key]
            } else {
                df.Data[name][i] = nil
            }
        }
    }

    // Remove the original nested column.
    delete(df.Data, nestedCol)
    kept := df.Cols[:0]
    for _, c := range df.Cols {
        if c != nestedCol {
            kept = append(kept, c)
        }
    }
    df.Cols = kept
    return df
}
// StringArrayConvertWrapper accepts a JSON string for the DataFrame and a column name to convert.
//
//export StringArrayConvertWrapper
func StringArrayConvertWrapper(dfJson *C.char, column *C.char) *C.char {
	// Unmarshal the DataFrame.
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("StringArrayToArrayWrapper: DataFrame unmarshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	// Call the StringArrayConvert method.
	newDF := df.StringArrayConvert(C.GoString(column))

	// Marshal the new DataFrame to JSON.
	jsonBytes, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("StringArrayToArrayWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(jsonBytes))
}

func (df *DataFrame) StringArrayConvert(column string) *DataFrame {
	for i := 0; i < df.Rows; i++ {
		val := df.Data[column][i]
		str, ok := val.(string)
		if !ok {
			// Value is not a string; skip conversion.
			continue
		}
		str = strings.TrimSpace(str)
		if len(str) < 2 || str[0] != '[' || str[len(str)-1] != ']' {
			// Not a stringed array; skip.
			continue
		}
		var arr []interface{}
		if err := json.Unmarshal([]byte(str), &arr); err != nil {
			fmt.Printf("ConvertStringArrayToSlice: error unmarshalling row %d in column %s: %v\n", i, column, err)
			continue
		}
		df.Data[column][i] = arr
	}
	return df
}

// make flatten function - from pyspark methodology (for individual columns)
// func flattenWrapper(djJson *C.char, col *C.char)

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
			<div id="app" style="text-align: center;" class=" h-screen pt-12">
				<button class="btn btn-sm fixed top-2 left-2 z-50" onclick="openInNewTab()">Open in Browser</button>
				<button class="btn btn-sm fixed top-2 right-2 z-50" @click="exportCSV()">Export to CSV</button>

 <!-- center, fixed; container ignores pointer events -->
 <div class="fixed top-2 left-1/2 -translate-x-1/2 z-50 pointer-events-none">
   <!-- only this inner group is clickable -->
   <div class="join pointer-events-auto">
      <div v-if="current_page > 1">
       <button class="btn btn-ghost btn-sm join-item" @click="first_page"><span class="material-symbols-outlined">first_page</span></button>
       <button class="btn btn-ghost btn-sm join-item" @click="prev_page"><span class="material-symbols-outlined">chevron_left</span></button>
      </div>

      <span v-if="pages <= 6" v-for="page in page_list" class="join">
        <button v-if="current_page === page" class="btn btn-sm btn-active no-animation join-item"><a href="#!">[[ current_page ]]</a></button>
        <button v-else class="btn btn-sm join-item" @click="pagefunc(page)"><a href="#!">[[ page ]]</a></button>
      </span>

      <span class="join" v-else>
       <span v-if="current_page > 3" class="btn btn-ghost btn-sm join-item pointer-events-none hover:bg-transparent focus:bg-transparent active:bg-transparent no-animation cursor-default select-none">…</span>
        <span v-for="page in page_list" class="join">
          <button v-if="current_page === page" class="btn btn-sm btn-active no-animation join-item"><a href="#!">[[ current_page ]]</a></button>
          <button v-else class="btn btn-sm join-item" @click="pagefunc(page)"><a href="#!">[[ page ]]</a></button>
        </span>
       <span v-if="current_page <= pages - 3" class="btn btn-ghost btn-sm join-item pointer-events-none hover:bg-transparent focus:bg-transparent active:bg-transparent no-animation cursor-default select-none">…</span>
      </span>

      <div v-if="current_page < pages">
       <button class="btn btn-ghost btn-sm join-item" @click="next_page"><span class="material-symbols-outlined">chevron_right</span></button>
       <button class="btn btn-ghost btn-sm join-item" @click="last_page"><span class="material-symbols-outlined">last_page</span></button>
      </div>
   </div>
 </div>			<!-- spacer to account for fixed toolbar height (~3rem) -->
			<!-- <div class="h-12"></div> -->
				<table class="table table-xs table-pin-rows">
	  				<thead>
						<tr>
							<th class="sticky top-12 z-40 bg-base-100 p-2"></th>
						<th v-for="col in cols"  class="sticky top-12 z-40 bg-base-100 p-2"><div class="dropdown dropdown-hover"><div tabindex="0" role="button" class="btn btn-sm btn-ghost justify justify-start">[[ col ]]</div>
							<ul tabindex="0" class="dropdown-content menu bg-base-100 rounded-box z-[1] w-52 p-2 shadow">
								<li>
									<details closed>
									<summary class="btn-sm">Sort</summary>
									<ul>
										<li><a @click="sortColumnAsc(col)" class="flex justify-between items-center btn-sm">Ascending<span class="material-symbols-outlined">north</span></a></li>
										<li><a @click="sortColumnDesc(col)" class="flex justify-between items-center btn-sm">Descending<span class="material-symbols-outlined">south</span></a></li>
									</ul>
									</details>
								</li>
							</ul>
						</div></th>
						</tr>
					</thead>
					<tbody>
					<tr v-for="i in pageRowIndices" :key="i">
							<th class="pl-5">[[ i + 1 ]]</th>
							<td v-for="col in cols" :key="col" class="pl-5">[[ data[col]?.[i] ]]</td>
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
						pages: 0,
						page_list: [],
						current_page: 1,
						pageSize: 50
					}
				},
				methods: {
      recomputePagination() {
        this.pages = Math.max(1, Math.ceil(this.rowCount / this.pageSize));
        if (this.current_page > this.pages) this.current_page = this.pages;
        if (this.current_page < 1) this.current_page = 1;
        this.updatePageList();
      },
      updatePageList() {
        const p = this.pages;
        const cur = this.current_page;
        if (p <= 6) {
          this.page_list = Array.from({length: p}, (_, i) => i + 1);
          return;
        }
        // sliding window of up to 6 pages
        let start = Math.max(1, cur - 2);
        let end = Math.min(p, start + 5);
        start = Math.max(1, end - 5);
        this.page_list = Array.from({length: end - start + 1}, (_, i) => start + i);
      },
      first_page(){ this.current_page = 1; },
      prev_page(){ if (this.current_page > 1) this.current_page -= 1; },
      pagefunc(page){ this.current_page = page; },
      next_page(){ if (this.current_page < this.pages) this.current_page += 1; },
      last_page(){ this.current_page = this.pages; },
					
       exportCSV() {
         const cols = Array.isArray(this.cols) ? this.cols.slice() : [];
         if (!cols.length) return;
         // Determine the maximum row count across all columns
         let rowCount = 0;
         for (const c of cols) {
           const len = (this.data[c] || []).length;
           if (len > rowCount) rowCount = len;
         }
         const esc = (v) => {
           if (v === null || v === undefined) return '';
           if (typeof v === 'object') v = JSON.stringify(v);
           let s = String(v);
           s = s.replace(/"/g, '""');
           if (/[",\r\n]/.test(s)) s = '"' + s + '"';
           return s;
         };
         const lines = [];
         // Header row
         lines.push(cols.map(esc).join(','));
         // Data rows
         for (let i = 0; i < rowCount; i++) {
           const row = cols.map(c => esc((this.data[c] || [])[i]));
           lines.push(row.join(','));
         }
         const csv = lines.join('\\r\\n');
         const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
         const url = URL.createObjectURL(blob);
         const a = document.createElement('a');
         a.href = url;
         a.download = 'dataframe.csv';
         document.body.appendChild(a);
         a.click();
         document.body.removeChild(a);
         URL.revokeObjectURL(url);
       },					
	   sortColumnAsc(col) {
						// Create an array of row indices
						const rowIndices = Array.from({ length: this.data[col].length }, (_, i) => i);

						// Sort the row indices based on the values in the specified column (ascending)
						rowIndices.sort((a, b) => {
							if (this.data[col][a] < this.data[col][b]) return -1;
							if (this.data[col][a] > this.data[col][b]) return 1;
							return 0;
						});

						// Reorder all columns based on the sorted row indices
						for (const key in this.data) {
							this.data[key] = rowIndices.map(i => this.data[key][i]);
						}

						// Update the selected column
						this.selected_col = col;
					},
					sortColumnDesc(col) {
						// Create an array of row indices
						const rowIndices = Array.from({ length: this.data[col].length }, (_, i) => i);

						// Sort the row indices based on the values in the specified column (descending)
						rowIndices.sort((a, b) => {
							if (this.data[col][a] > this.data[col][b]) return -1;
							if (this.data[col][a] < this.data[col][b]) return 1;
							return 0;
						});

						// Reorder all columns based on the sorted row indices
						for (const key in this.data) {
							this.data[key] = rowIndices.map(i => this.data[key][i]);
						}

						// Update the selected column
						this.selected_col = col;
					}
				},
				watch: {
					rowCount() {
						this.recomputePagination();
					},
					pageSize() {
						this.recomputePagination();
					},
					current_page() {
						// keep page_list window centered
						this.updatePageList();
					}
				},
				created(){
					this.recomputePagination();

				},

				mounted() {

				},
				computed:{
					// Max row count across all columns
					rowCount() {
						let rc = 0;
						for (const c of this.cols || []) {
						const len = (this.data[c] || []).length;
						if (len > rc) rc = len;
						}
						return rc;
					},
					// Indices for the current page
					pageRowIndices() {
						const start = (this.current_page - 1) * this.pageSize;
						const end = Math.min(start + this.pageSize, this.rowCount);
						const n = Math.max(end - start, 0);
						return Array.from({ length: n }, (_, i) => start + i);
					}				
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
			<div id="app" style="text-align: center;" class=" h-screen pt-12">
				<button class="btn btn-sm fixed top-2 left-2 z-50" onclick="openInNewTab()">Open in Browser</button>
				<button class="btn btn-sm fixed top-2 right-2 z-50" @click="exportCSV()">Export to CSV</button>

 <!-- center, fixed; container ignores pointer events -->
 <div class="fixed top-2 left-1/2 -translate-x-1/2 z-50 pointer-events-none">
   <!-- only this inner group is clickable -->
   <div class="join pointer-events-auto">
      <div v-if="current_page > 1">
       <button class="btn btn-ghost btn-sm join-item" @click="first_page"><span class="material-symbols-outlined">first_page</span></button>
       <button class="btn btn-ghost btn-sm join-item" @click="prev_page"><span class="material-symbols-outlined">chevron_left</span></button>
      </div>

      <span v-if="pages <= 6" v-for="page in page_list" class="join">
        <button v-if="current_page === page" class="btn btn-sm btn-active no-animation join-item"><a href="#!">[[ current_page ]]</a></button>
        <button v-else class="btn btn-sm join-item" @click="pagefunc(page)"><a href="#!">[[ page ]]</a></button>
      </span>

      <span class="join" v-else>
       <span v-if="current_page > 3" class="btn btn-ghost btn-sm join-item pointer-events-none hover:bg-transparent focus:bg-transparent active:bg-transparent no-animation cursor-default select-none">…</span>
        <span v-for="page in page_list" class="join">
          <button v-if="current_page === page" class="btn btn-sm btn-active no-animation join-item"><a href="#!">[[ current_page ]]</a></button>
          <button v-else class="btn btn-sm join-item" @click="pagefunc(page)"><a href="#!">[[ page ]]</a></button>
        </span>
       <span v-if="current_page <= pages - 3" class="btn btn-ghost btn-sm join-item pointer-events-none hover:bg-transparent focus:bg-transparent active:bg-transparent no-animation cursor-default select-none">…</span>
      </span>

      <div v-if="current_page < pages">
       <button class="btn btn-ghost btn-sm join-item" @click="next_page"><span class="material-symbols-outlined">chevron_right</span></button>
       <button class="btn btn-ghost btn-sm join-item" @click="last_page"><span class="material-symbols-outlined">last_page</span></button>
      </div>
   </div>
 </div>			<!-- spacer to account for fixed toolbar height (~3rem) -->
			<!-- <div class="h-12"></div> -->
				<table class="table table-xs table-pin-rows">
	  				<thead>
						<tr>
							<th class="sticky top-12 z-40 bg-base-100 p-2"></th>
						<th v-for="col in cols"  class="sticky top-12 z-40 bg-base-100 p-2"><div class="dropdown dropdown-hover"><div tabindex="0" role="button" class="btn btn-sm btn-ghost justify justify-start">[[ col ]]</div>
							<ul tabindex="0" class="dropdown-content menu bg-base-100 rounded-box z-[1] w-52 p-2 shadow">
								<li>
									<details closed>
									<summary class="btn-sm">Sort</summary>
									<ul>
										<li><a @click="sortColumnAsc(col)" class="flex justify-between items-center btn-sm">Ascending<span class="material-symbols-outlined">north</span></a></li>
										<li><a @click="sortColumnDesc(col)" class="flex justify-between items-center btn-sm">Descending<span class="material-symbols-outlined">south</span></a></li>
									</ul>
									</details>
								</li>
							</ul>
						</div></th>
						</tr>
					</thead>
					<tbody>
					<tr v-for="i in pageRowIndices" :key="i">
							<th class="pl-5">[[ i + 1 ]]</th>
							<td v-for="col in cols" :key="col" class="pl-5">[[ data[col]?.[i] ]]</td>
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
						pages: 0,
						page_list: [],
						current_page: 1,
						pageSize: 50
					}
				},
				methods: {
      recomputePagination() {
        this.pages = Math.max(1, Math.ceil(this.rowCount / this.pageSize));
        if (this.current_page > this.pages) this.current_page = this.pages;
        if (this.current_page < 1) this.current_page = 1;
        this.updatePageList();
      },
      updatePageList() {
        const p = this.pages;
        const cur = this.current_page;
        if (p <= 6) {
          this.page_list = Array.from({length: p}, (_, i) => i + 1);
          return;
        }
        // sliding window of up to 6 pages
        let start = Math.max(1, cur - 2);
        let end = Math.min(p, start + 5);
        start = Math.max(1, end - 5);
        this.page_list = Array.from({length: end - start + 1}, (_, i) => start + i);
      },
      first_page(){ this.current_page = 1; },
      prev_page(){ if (this.current_page > 1) this.current_page -= 1; },
      pagefunc(page){ this.current_page = page; },
      next_page(){ if (this.current_page < this.pages) this.current_page += 1; },
      last_page(){ this.current_page = this.pages; },
					
       exportCSV() {
         const cols = Array.isArray(this.cols) ? this.cols.slice() : [];
         if (!cols.length) return;
         // Determine the maximum row count across all columns
         let rowCount = 0;
         for (const c of cols) {
           const len = (this.data[c] || []).length;
           if (len > rowCount) rowCount = len;
         }
         const esc = (v) => {
           if (v === null || v === undefined) return '';
           if (typeof v === 'object') v = JSON.stringify(v);
           let s = String(v);
           s = s.replace(/"/g, '""');
           if (/[",\r\n]/.test(s)) s = '"' + s + '"';
           return s;
         };
         const lines = [];
         // Header row
         lines.push(cols.map(esc).join(','));
         // Data rows
         for (let i = 0; i < rowCount; i++) {
           const row = cols.map(c => esc((this.data[c] || [])[i]));
           lines.push(row.join(','));
         }
         const csv = lines.join('\\r\\n');
         const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
         const url = URL.createObjectURL(blob);
         const a = document.createElement('a');
         a.href = url;
         a.download = 'dataframe.csv';
         document.body.appendChild(a);
         a.click();
         document.body.removeChild(a);
         URL.revokeObjectURL(url);
       },					
	   sortColumnAsc(col) {
						// Create an array of row indices
						const rowIndices = Array.from({ length: this.data[col].length }, (_, i) => i);

						// Sort the row indices based on the values in the specified column (ascending)
						rowIndices.sort((a, b) => {
							if (this.data[col][a] < this.data[col][b]) return -1;
							if (this.data[col][a] > this.data[col][b]) return 1;
							return 0;
						});

						// Reorder all columns based on the sorted row indices
						for (const key in this.data) {
							this.data[key] = rowIndices.map(i => this.data[key][i]);
						}

						// Update the selected column
						this.selected_col = col;
					},
					sortColumnDesc(col) {
						// Create an array of row indices
						const rowIndices = Array.from({ length: this.data[col].length }, (_, i) => i);

						// Sort the row indices based on the values in the specified column (descending)
						rowIndices.sort((a, b) => {
							if (this.data[col][a] > this.data[col][b]) return -1;
							if (this.data[col][a] < this.data[col][b]) return 1;
							return 0;
						});

						// Reorder all columns based on the sorted row indices
						for (const key in this.data) {
							this.data[key] = rowIndices.map(i => this.data[key][i]);
						}

						// Update the selected column
						this.selected_col = col;
					}
				},
				watch: {
					rowCount() {
						this.recomputePagination();
					},
					pageSize() {
						this.recomputePagination();
					},
					current_page() {
						// keep page_list window centered
						this.updatePageList();
					}
				},
				created(){
					this.recomputePagination();

				},

				mounted() {

				},
				computed:{
					// Max row count across all columns
					rowCount() {
						let rc = 0;
						for (const c of this.cols || []) {
						const len = (this.data[c] || []).length;
						if (len > rc) rc = len;
						}
						return rc;
					},
					// Indices for the current page
					pageRowIndices() {
						const start = (this.current_page - 1) * this.pageSize;
						const end = Math.min(start + this.pageSize, this.rowCount);
						const n = Math.max(end - start, 0);
						return Array.from({ length: n }, (_, i) => start + i);
					}				
				}

			}).mount('#app')
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

// write an html display, chart, or report to a file
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
		data = append(data, df.Data[agg.ColumnName]...)
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
	Htmlpostid := ` class="flex justify-center mx-auto p-4"></div>`
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
		data = append(data, df.Data[agg.ColumnName]...)

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
	Htmlpostid := ` class="flex justify-center mx-auto p-4"></div>`
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
		data = append(data, df.Data[agg.ColumnName]...)
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
	Htmlpostid := ` class="flex justify-center mx-auto p-4"></div>`
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
func CreateReport(title string) *Report {
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

	newReport := &Report{
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
		Pageshtml:     make(map[string]map[string]string),
		Pagesjs:       make(map[string]map[string]string),
	}
	// fmt.Println("CreateReport: Initialized report:", newReport)
	return newReport
}

// Open - open the report in browser
func (report *Report) Open() error {
	// add html element for page
	html := report.Top +
		report.Primary +
		report.Secondary +
		report.Accent +
		report.Neutral +
		report.Base100 +
		report.Info +
		report.Success +
		report.Warning +
		report.Err +
		report.Htmlheading
	if len(report.Pageshtml) > 1 {
		html += `
        <div id="app"  style="text-align: center;" class="drawer w-full lg:drawer-open">
            <input id="my-drawer-2" type="checkbox" class="drawer-toggle" />
            <div class="drawer-content flex flex-col">
                <!-- Navbar -->
                <div class="w-full navbar bg-neutral text-neutral-content shadow-lg ">
            ` +
			fmt.Sprintf(`<div class="flex-1 px-2 mx-2 btn btn-sm btn-neutral normal-case text-xl shadow-none hover:bg-neutral hover:border-neutral flex content-center"><a class="lg:ml-0 ml-14 text-4xl">%s</a></div>`, report.Title) +
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
            </div>`, report.Title) +
			`<div  class="w-full lg:w-3/4 md:w-3/4 sm:w-5/6 mx-auto flex-col justify-self-center">`

	}
	// iterate over pageshtml and add each stored HTML snippet
	for _, pageMap := range report.Pageshtml {
		// iterate in order
		// fmt.Println(pageMap)
		for i := 0; i < len(pageMap); i++ {
			html += pageMap[strconv.Itoa(i)]
		}
	}
	if len(report.Pageshtml) > 1 {
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
		for page, _ := range report.Pageshtml {
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
	html += report.Scriptheading
	pages := `pages: [`
	count := 0
	for page, _ := range report.Pageshtml {
		if count == 0 {
			html += fmt.Sprintf("%q", page) + ","
		}
		pages += fmt.Sprintf("%q", page) + ", "
		count++
	}
	pages = strings.TrimSuffix(pages, ", ") + `],`
	html += pages
	html += report.Scriptmiddle
	// iterate over pagesjs similarly
	for _, jsMap := range report.Pagesjs {
		// fmt.Println("printing jsMap")
		// fmt.Println(jsMap)
		for i := 0; i < len(jsMap); i++ {
			html += jsMap[strconv.Itoa(i)]
		}
	}

	html += report.Bottom
	// fmt.Println("printing html:")
	// fmt.Println(html)
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

// Save - save report to html file
func (report *Report) Save(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// add html element for page
	html := report.Top +
		report.Primary +
		report.Secondary +
		report.Accent +
		report.Neutral +
		report.Base100 +
		report.Info +
		report.Success +
		report.Warning +
		report.Err +
		report.Htmlheading

	if len(report.Pageshtml) > 1 {
		html += `
		<div id="app"  style="text-align: center;" class="drawer w-full lg:drawer-open">
			<input id="my-drawer-2" type="checkbox" class="drawer-toggle" />
			<div class="drawer-content flex flex-col">
				<!-- Navbar -->
				<div class="w-full navbar bg-neutral text-neutral-content shadow-lg ">
			` +
			fmt.Sprintf(`<div class="flex-1 px-2 mx-2 btn btn-sm btn-neutral normal-case text-xl shadow-none hover:bg-neutral hover:border-neutral flex content-center"><a class="lg:ml-0 ml-14 text-4xl">%s</a></div>`, report.Title) +
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
			</div>`, report.Title) +
			`<div  class="w-full lg:w-3/4 md:w-3/4 sm:w-5/6 mx-auto flex-col justify-self-center">`

	}
	// iterate over pageshtml and add each stored HTML snippet
	for _, pageMap := range report.Pageshtml {
		// iterate in order
		// fmt.Println(pageMap)
		for i := 0; i < len(pageMap); i++ {
			html += pageMap[strconv.Itoa(i)]
		}
	}
	if len(report.Pageshtml) > 1 {
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
		for page, _ := range report.Pageshtml {
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
	html += report.Scriptheading
	pages := `pages: [`
	count := 0
	for page, _ := range report.Pageshtml {
		if count == 0 {
			html += fmt.Sprintf("%q", page) + ","
		}
		pages += fmt.Sprintf("%q", page) + ", "
		count++
	}
	pages = strings.TrimSuffix(pages, ", ") + `],`
	html += pages
	html += report.Scriptmiddle
	// iterate over pagesjs similarly
	for _, jsMap := range report.Pagesjs {
		// fmt.Println("printing jsMap")
		// fmt.Println(jsMap)
		for i := 0; i < len(jsMap); i++ {
			html += jsMap[strconv.Itoa(i)]
		}
	}

	html += report.Bottom

	// Write the HTML string to the file
	if _, err := file.Write([]byte(html)); err != nil {
		return fmt.Errorf("failed to write to file: %v", err)
	}

	return nil
}

// AddPage adds a new page to the report.
func (report *Report) AddPage(name string) {
	report.init() // Ensure maps are initialized

	// Check if the page already exists.
	if _, exists := report.Pageshtml[name]; !exists {
		report.Pageshtml[name] = make(map[string]string)
	}
	if _, exists := report.Pagesjs[name]; !exists {
		report.Pagesjs[name] = make(map[string]string)
	}

	html := `<h1 v-if="page == '` + name + `' " class="text-8xl pt-24 pb-24"> ` + name + `</h1>` // Page Title at top of page
	report.Pageshtml[name][strconv.Itoa(len(report.Pageshtml[name]))] = html

	// fmt.Println("AddPage: Added page:", name)
	// fmt.Println("AddPage: Updated pageshtml:", report.Pageshtml)
}

// spacing for stuff? card or no card? background?

// add text input

// add slider

// add dropdown (array of selections) - adds variable and updates charts + uses in new charts...

// add iframe
// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (report *Report) AddHTML(page string, text string) {
	report.init() // Ensure maps are initialized

	// Check if the page exists
	if _, exists := report.Pageshtml[page]; !exists {
		report.Pageshtml[page] = make(map[string]string)
	}
	escapedtext := html.EscapeString(text)
	texthtml := `<iframe v-if="page == '` + page + `' " class="p-8 flex justify-self-center w-full h-screen" srcdoc='` + escapedtext + `'></iframe>`
	report.Pageshtml[page][strconv.Itoa(len(report.Pageshtml[page]))] = texthtml

	fmt.Println("AddHTML: Added HTML to page:", page)
	fmt.Println("AddHTML: Updated pageshtml:", report.Pageshtml)
}

// add df (paginate + filter + sort)
func (report *Report) AddDataframe(page string, df *DataFrame) {
	text := df.Display()["text/html"].(string)
	// add html to page map
	if _, exists := report.Pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPage()")
		return
	}
	report.AddHTML(page, text)
}

// AddChart adds a chart to the specified page in the report.
func (report *Report) AddChart(page string, chart Chart) {
	report.init() // Ensure maps are initialized

	// Check if the page exists
	if _, exists := report.Pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPAge().")
		return
	}
	if _, exists := report.Pagesjs[page]; !exists {
		fmt.Println("Page content does not exist.")
		return
	}

	idhtml := strconv.Itoa(len(report.Pageshtml[page]))
	chartId := chart.Htmldivid + idhtml
	idjs := strconv.Itoa(len(report.Pagesjs[page]))

	if chart.Htmlpostid == "" {
		chart.Htmlpostid = ` class="flex justify-center mx-auto p-4"></div>`
	}

	html := fmt.Sprintf(`<div v-show="page == '%s'" id="%s"%s`, page, chartId, chart.Htmlpostid)
	js := fmt.Sprintf(`%s%s%s`, chart.Jspreid, chartId, chart.Jspostid)

	report.Pageshtml[page][idhtml] = html
	report.Pagesjs[page][idjs] = js

	// fmt.Println("DASH:", report.Pageshtml)
	// fmt.Printf("AddChart: Added chart to page %s at index %s\n", page, idhtml)
	// fmt.Println("AddChart: Updated pageshtml:", report.Pageshtml)
	// fmt.Println("AddChart: Updated pagesjs:", report.Pagesjs)
}

// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
// AddHeading adds a heading to the specified page in the report.
func (report *Report) AddHeading(page string, heading string, size int) {
	report.init() // Ensure maps are initialized

	// Check if the page exists
	if _, exists := report.Pageshtml[page]; !exists {
		report.Pageshtml[page] = make(map[string]string)
	}

	var text_size string
	switch size {
	case 1:
		text_size = "text-6xl"
	case 2:
		text_size = "text-5xl"
	case 3:
		text_size = "text-4xl"
	case 4:
		text_size = "text-3xl"
	case 5:
		text_size = "text-2xl"
	case 6:
		text_size = "text-xl"
	case 7:
		text_size = "text-lg"
	case 8:
		text_size = "text-md"
	case 9:
		text_size = "text-sm"
	case 10:
		text_size = "text-xs"
	default:
		text_size = "text-md"
	}

	html := `<h1 v-if="page == '` + page + fmt.Sprintf(`' " class="%s p-8 flex justify-start"> `, text_size) + heading + `</h1>`
	report.Pageshtml[page][strconv.Itoa(len(report.Pageshtml[page]))] = html

	// fmt.Printf("AddHeading: Added heading to page %s with size %d\n", page, size)
	// fmt.Println("AddHeading: Updated pageshtml:", report.Pageshtml)
}

// AddText function fix
func (report *Report) AddText(page string, text string) {
	report.init() // Ensure maps are initialized

	// Check if the page exists
	if _, exists := report.Pageshtml[page]; !exists {
		report.Pageshtml[page] = make(map[string]string)
	}

	text_size := "text-md"
	html := `<h1 v-if="page == '` + page + fmt.Sprintf(`' " class="%s pl-12 pr-12 flex justify-start text-left"> `, text_size) + text + `</h1>`
	idx := strconv.Itoa(len(report.Pageshtml[page]))
	report.Pageshtml[page][idx] = html

	// fmt.Printf("AddText: Added text to page %s at index %s\n", page, idx)
	// fmt.Println("AddText: Updated pageshtml:", report.Pageshtml)
}

// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (report *Report) AddSubText(page string, text string) {
	report.init() // Ensure maps are initialized

	// Check if the page exists
	if _, exists := report.Pageshtml[page]; !exists {
		report.Pageshtml[page] = make(map[string]string)
	}

	text_size := "text-sm"
	html := `<h1 v-if="page == '` + page + fmt.Sprintf(`' " class="%s pl-12 pr-12 pb-8 flex justify-center"> `, text_size) + text + `</h1>`
	report.Pageshtml[page][strconv.Itoa(len(report.Pageshtml[page]))] = html

	fmt.Println("AddSubText: Added subtext to page:", page)
	fmt.Println("AddSubText: Updated pageshtml:", report.Pageshtml)
}

// add bullet list
// add html to page map
// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (report *Report) AddBullets(page string, text ...string) {

	// Check if the page exists
	if _, exists := report.Pageshtml[page]; !exists {
		report.Pageshtml[page] = make(map[string]string)
	}
	text_size := "text-md"
	html := `<ul v-if="page == '` + page + `' " class="list-disc flex-col justify-self-start pl-24 pr-12 py-2"> `
	for _, bullet := range text {
		html += fmt.Sprintf(`<li class="text-left %s">`, text_size) + bullet + `</li>`
	}
	html += `</ul>`
	report.Pageshtml[page][strconv.Itoa(len(report.Pageshtml[page]))] = html

	fmt.Println("AddBullets: Added bullets to page:", page)
	fmt.Println("AddBullets: Updated pageshtml:", report.Pageshtml)

}

// CreateReportWrapper is an exported function that wraps the CreateReport method.
//
//export CreateReportWrapper
func CreateReportWrapper(title *C.char) *C.char {
	// fmt.Printf("printing dfjson:%s", []byte(C.GoString(dfJson)))
	// fmt.Println("")
	report := CreateReport(C.GoString(title))
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
// If implements conditional logic similar to PySpark's when.
// It returns fn1 if condition returns true for a row, else fn2.
func If(condition Column, fn1 Column, fn2 Column) Column {
	return Column{
		Name: "If",
		Fn: func(row map[string]interface{}) interface{} {
			cond, ok := condition.Fn(row).(bool)
			if !ok {
				return nil
			}
			if cond {
				return fn1.Fn(row)
			}
			return fn2.Fn(row)
		},
	}
}

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

	result := If(condition, fn1, fn2)
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

// IsNull returns a new Column that, when applied to a row,
// returns true if the original column value is nil, an empty string, or "null".
func (c Column) IsNull() Column {
	return Column{
		Name: c.Name + "_isnull",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			if val == nil {
				return true
			}

			switch v := val.(type) {
			case string:
				return v == "" || strings.ToLower(v) == "null"
			case *string:
				return v == nil || *v == "" || strings.ToLower(*v) == "null"
			default:
				return false
			}
		},
	}
}

// // Updated IsNullWrapper: now accepts both a column JSON and a DataFrame JSON,
// // so that it can use real data (here the first row) rather than an empty dummy row.
// //
// //export IsNullWrapper
// func IsNullWrapper(columnJson *C.char) *C.char {
//     // Parse the incoming JSON from Python FuncColumn.
//     var colData struct {
//         Name string   `json:"func_name"`
//         Col   []string `json:"cols"`
//     }

//     if err := json.Unmarshal([]byte(C.GoString(columnJson)), &colData); err != nil {
//         errStr := fmt.Sprintf("IsNullWrapper: unmarshal error: %v", err)
//         log.Fatal(errStr)
//         return C.CString(errStr)
//     }
//     fmt.Printf("IsNullWrapper: Parsed column data: %+v\n", colData)

//     result := colData.IsNull()

//	    return C.CString(result)
//	}
//
// IsNotNull returns a new Column that, when applied to a row,
// returns true if the original column value is not nil, not an empty string, and not "null".
func (c Column) IsNotNull() Column {
	return Column{
		Name: c.Name + "_isnotnull",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			if val == nil {
				return false
			}
			switch v := val.(type) {
			case string:
				return !(v == "" || strings.ToLower(v) == "null")
			case *string:
				return !(v == nil || *v == "" || strings.ToLower(*v) == "null")
			default:
				return true
			}
		},
	}
}

// Gt returns a Column that compares the numeric value at col with the given threshold.
// The threshold can be of any numeric type (int, float32, float64, etc.).
func (c Column) Gt(threshold interface{}) Column {
	return Column{
		Name: c.Name + "_gt",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			fVal, err := toFloat64(val)
			if err != nil {
				return false
			}
			fThreshold, err := toFloat64(threshold)
			if err != nil {
				return false
			}
			return fVal > fThreshold
		},
	}
}

// Ge returns a Column that compares the numeric value at col with the given threshold.
// The threshold can be of any numeric type (int, float32, float64, etc.).
func (c Column) Ge(threshold interface{}) Column {
	return Column{
		Name: c.Name + "_ge",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			fVal, err := toFloat64(val)
			if err != nil {
				return false
			}
			fThreshold, err := toFloat64(threshold)
			if err != nil {
				return false
			}
			return fVal >= fThreshold
		},
	}
}

// Lt returns a Column that compares the numeric value at col with the given threshold.
// The threshold can be of any numeric type (int, float32, float64, etc.).
func (c Column) Lt(threshold interface{}) Column {
	return Column{
		Name: c.Name + "_lt",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			fVal, err := toFloat64(val)
			if err != nil {
				return false
			}
			fThreshold, err := toFloat64(threshold)
			if err != nil {
				return false
			}
			return fVal < fThreshold
		},
	}
}

// Le returns a Column that compares the numeric value at col with the given threshold.
// The threshold can be of any numeric type (int, float32, float64, etc.).
func (c Column) Le(threshold interface{}) Column {
	return Column{
		Name: c.Name + "_le",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			fVal, err := toFloat64(val)
			if err != nil {
				return false
			}
			fThreshold, err := toFloat64(threshold)
			if err != nil {
				return false
			}
			return fVal <= fThreshold
		},
	}
}

// Eq returns a Column that, when evaluated on a row,
// checks if the value from col is equal (same type and value) to threshold.
func (c Column) Eq(threshold interface{}) Column {
	return Column{
		Name: c.Name + "_eq",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			// If either is nil, return equality directly.
			if val == nil || threshold == nil {
				return val == threshold
			}
			// Check that both values are of the same type.
			if reflect.TypeOf(val) != reflect.TypeOf(threshold) {
				return false
			}
			// Use Go's native equality.
			return val == threshold
		},
	}
}

// Ne returns a Column that, when evaluated on a row,
// checks if the value from col is NOT equal (diff type or value) to threshold.
func (c Column) Ne(threshold interface{}) Column {
	return Column{
		Name: c.Name + "_ne",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			// If either is nil, return equality directly.
			if val == nil || threshold == nil {
				return val != threshold
			}
			// Check that both values are of the same type.
			if reflect.TypeOf(val) != reflect.TypeOf(threshold) {
				return true
			}
			// Use Go's native equality.
			return val != threshold
		},
	}
}

// Or returns a Column that evaluates to true if either of the two provided Conditions is true.
func Or(c1, c2 Column) Column {
	return Column{
		Name: "or",
		Fn: func(row map[string]interface{}) interface{} {
			cond1, ok1 := c1.Fn(row).(bool)
			cond2, ok2 := c2.Fn(row).(bool)
			if !ok1 || !ok2 {
				return false
			}
			return cond1 || cond2
		},
	}
}

// And returns a Column that evaluates to true if both of the two provided Conditions is true.
func And(c1, c2 Column) Column {
	return Column{
		Name: "and",
		Fn: func(row map[string]interface{}) interface{} {
			cond1, ok1 := c1.Fn(row).(bool)
			cond2, ok2 := c2.Fn(row).(bool)
			if !ok1 || !ok2 {
				return false
			}
			return cond1 && cond2
		},
	}
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

// Column adds or modifies a column in the DataFrame using a Column.
// This version accepts a Column (whose underlying function is applied to each row).
//
//	(moddified for c-shared library)
func (df *DataFrame) Column(column string, colSpec ColumnExpr) *DataFrame {
	values := make([]interface{}, df.Rows)
	for i := 0; i < df.Rows; i++ {
		row := make(map[string]interface{})
		for _, c := range df.Cols {
			row[c] = df.Data[c][i]
		}
		// // Use the underlying Column function.
		// values[i] = col.Fn(row)

		// Use the Evaluate function to evaluate the expression.
		values[i] = Evaluate(colSpec, row)
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

// Concat returns a Column that, when applied to a row,
// concatenates the string representations of the provided Columns using the specified delimiter.
// It converts each value to a string using toString. If conversion fails for a value, it uses an empty string.
func Concat(delim string, cols ...Column) Column {
	return Column{
		Name: "concat_ws",
		Fn: func(row map[string]interface{}) interface{} {
			var parts []string
			for _, col := range cols {
				val := col.Fn(row)
				str, err := toString(val)
				if err != nil {
					str = ""
				}
				parts = append(parts, str)
			}
			return strings.Join(parts, delim)
		},
	}
}

// Cast takes in an existing Column and a desired datatype ("int", "float", "string"),
// and returns a new Column that casts the value returned by the original Column to that datatype.
func Cast(col Column, datatype string) Column {
	return Column{
		Name: col.Name + "_cast",
		Fn: func(row map[string]interface{}) interface{} {
			val := col.Fn(row)
			switch datatype {
			case "int":
				casted, err := toInt(val)
				if err != nil {
					fmt.Printf("cast to int error: %v\n", err)
					return nil
				}
				return casted
			case "float":
				casted, err := toFloat64(val)
				if err != nil {
					fmt.Printf("cast to float error: %v\n", err)
					return nil
				}
				return casted
			case "string":
				casted, err := toString(val)
				if err != nil {
					fmt.Printf("cast to string error: %v\n", err)
					return nil
				}
				return casted
			default:
				fmt.Printf("unsupported cast type: %s\n", datatype)
				return nil
			}
		},
	}
}

// Filter returns a new DataFrame containing only the rows for which
// the condition (a Column that evaluates to a bool) is true.
func (df *DataFrame) Filter(condition ColumnExpr) *DataFrame {
	// Ensure df.Data is non-nil.
	if df.Data == nil {
		df.Data = make(map[string][]interface{})
	}

	// Create new DataFrame with the same columns.
	newDF := &DataFrame{
		Cols: df.Cols,
		Data: make(map[string][]interface{}),
	}
	// Compute the minimum row count across all columns.
	minRows := df.Rows
	for _, col := range df.Cols {
		if data, ok := df.Data[col]; ok && data != nil {
			if len(data) < minRows {
				minRows = len(data)
			}
		} else {
			minRows = 0
		}
	}

	for i := 0; i < minRows; i++ {
		// Build a row for evaluation.
		row := make(map[string]interface{})
		for _, col := range df.Cols {
			if data, ok := df.Data[col]; ok && data != nil && i < len(data) {
				row[col] = data[i]
			} else {
				row[col] = nil
			}
		}
		res := Evaluate(condition, row)
		include, ok := res.(bool)
		if !ok {
			include = false
		}
		if include {
			for _, col := range df.Cols {
				newDF.Data[col] = append(newDF.Data[col], row[col])
			}
		}
	}
	// Set new row count.
	if len(df.Cols) > 0 {
		newDF.Rows = len(newDF.Data[df.Cols[0]])
	}
	return newDF
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

// ExplodeWrapper is an exported function that wraps the Explode method.
// It accepts a JSON string representing the DataFrame and a JSON string representing an array of column names to explode.
// It returns the resulting DataFrame as a JSON string.
//
//export ExplodeWrapper
func ExplodeWrapper(dfJson *C.char, colsJson *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		errStr := fmt.Sprintf("ExplodeWrapper: unmarshal error (DataFrame): %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	var cols []string
	if err := json.Unmarshal([]byte(C.GoString(colsJson)), &cols); err != nil {
		errStr := fmt.Sprintf("ExplodeWrapper: unmarshal error (columns): %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	newDF := df.Explode(cols...)
	resultJson, err := json.Marshal(newDF)
	if err != nil {
		errStr := fmt.Sprintf("ExplodeWrapper: marshal error: %v", err)
		log.Fatal(errStr)
		return C.CString(errStr)
	}

	return C.CString(string(resultJson))
}

// Explode creates a new DataFrame where each value in the specified columns' slices becomes a separate row.
// func (df *DataFrame) Explode(columns ...string) *DataFrame {
// 	for _, column := range columns {
// 		df = df.explodeSingleColumn(column)
// 	}
// 	return df
// }

// explodeSingleColumn creates a new DataFrame where each value in the specified column's slice becomes a separate row.
func (df *DataFrame) explodeSingleColumn(column string) *DataFrame {
	newCols := df.Cols
	newData := make(map[string][]interface{})

	// Initialize newData with empty slices for each column.
	for _, col := range newCols {
		newData[col] = []interface{}{}
	}

	// Iterate over each row in the DataFrame.
	for i := 0; i < df.Rows; i++ {
		// Get the value of the specified column.
		val := df.Data[column][i]

		// Check if the value is a slice.
		if slice, ok := val.([]interface{}); ok {
			// Create a new row for each value in the slice.
			for _, item := range slice {
				for _, col := range newCols {
					if col == column {
						newData[col] = append(newData[col], item)
					} else {
						newData[col] = append(newData[col], df.Data[col][i])
					}
				}
			}
		} else {
			// If the value is not a slice, just copy the row as is.
			for _, col := range newCols {
				newData[col] = append(newData[col], df.Data[col][i])
			}
		}
	}

	return &DataFrame{
		Cols: newCols,
		Data: newData,
		Rows: len(newData[newCols[0]]),
	}
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

// alias
// func (c Column) Alias(newname string) Column{
// 	return
// }

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

// fillna
func (df *DataFrame) FillNA(replacement string) *DataFrame {
	// quotedReplacement := fmt.Sprintf("%q", replacement)
	for col, values := range df.Data {
		for i, value := range values {
			if value == nil {
				df.Data[col][i] = replacement
			} else {
				switch v := value.(type) {
				case string:
					if v == "" || strings.ToLower(v) == "null" {
						df.Data[col][i] = replacement
					}
				}
			}
		}
	}
	return df
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

// DropNA
func (df *DataFrame) DropNA() *DataFrame {
	// Create a new DataFrame with the same columns.
	newDF := &DataFrame{
		Cols: df.Cols,
		Data: make(map[string][]interface{}),
	}
	for _, col := range df.Cols {
		newDF.Data[col] = []interface{}{}
	}

	// Iterate over each row.
	for i := 0; i < df.Rows; i++ {
		// Build a row (as a map) for evaluation.
		row := make(map[string]interface{})
		for _, col := range df.Cols {
			row[col] = df.Data[col][i]
		}
		// Evaluate the condition.
		keep := true
		for _, val := range row {
			if val == nil {
				keep = false
				break
			}

			switch v := val.(type) {
			case string:
				if v == "" || strings.ToLower(v) == "null" {
					keep = false
					break
				}
			}
		}
		if keep {
			// If true, append data from this row to newDF.
			for _, col := range df.Cols {
				newDF.Data[col] = append(newDF.Data[col], row[col])
			}
		}
	}

	// Set new row count.
	if len(df.Cols) > 0 {
		newDF.Rows = len(newDF.Data[df.Cols[0]])
	}

	return newDF
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

// DropDuplicates removes duplicate rows from the DataFrame.
// If one or more columns are provided, only those columns are used to determine uniqueness.
// If no columns are provided, the entire row (all columns) is used.
func (df *DataFrame) DropDuplicates(columns ...string) *DataFrame {
	// If no columns are specified, use all columns.
	uniqueCols := columns
	if len(uniqueCols) == 0 {
		uniqueCols = df.Cols
	}

	seen := make(map[string]bool)
	newData := make(map[string][]interface{})
	for _, col := range df.Cols {
		newData[col] = []interface{}{}
	}

	for i := 0; i < df.Rows; i++ {
		// Build a subset row only with the uniqueCols.
		rowSubset := make(map[string]interface{})
		for _, col := range uniqueCols {
			rowSubset[col] = df.Data[col][i]
		}

		// Convert the subset row to a JSON string to use as a key.
		rowBytes, err := json.Marshal(rowSubset)
		if err != nil {
			// If marshalling fails, skip this row.
			continue
		}
		rowStr := string(rowBytes)

		if !seen[rowStr] {
			seen[rowStr] = true
			// Append the full row (all columns) to the new data.
			for _, col := range df.Cols {
				newData[col] = append(newData[col], df.Data[col][i])
			}
		}
	}

	// Update the DataFrame with the new data.
	df.Data = newData
	if len(df.Cols) > 0 {
		df.Rows = len(newData[df.Cols[0]])
	} else {
		df.Rows = 0
	}

	return df
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

// Select returns a new DataFrame containing only the specified columns.
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

	nRows := 0
	if len(df.Cols) > 0 {
		nRows = len(df.Data[df.Cols[0]])
	} else {
		nRows = 0
	}

	return &DataFrame{
		Cols: newCols,
		Data: newData,
		Rows: nRows,
	}
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

// Sort sorts the DataFrame's columns in alphabetical order.
func (df *DataFrame) Sort() *DataFrame {
	// Make a copy of the columns and sort it.
	sortedCols := make([]string, len(df.Cols))
	copy(sortedCols, df.Cols)
	sort.Strings(sortedCols)

	// Build a new data map using the sorted column order.
	newData := make(map[string][]interface{})
	for _, col := range sortedCols {
		if data, exists := df.Data[col]; exists {
			newData[col] = data
		} else {
			newData[col] = make([]interface{}, df.Rows)
		}
	}

	df.Cols = sortedCols
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
		Name: "Lit",
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

// // ColumnCollectList applies CollectList on the specified source column
// // and creates a new column.
// //
// //export ColumnCollectList
// func ColumnCollectList(dfJson *C.char, newCol *C.char, source *C.char) *C.char {
// 	var df DataFrame
// 	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
// 		log.Fatalf("ColumnCollectList: unmarshal error: %v", err)
// 	}
// 	newName := C.GoString(newCol)
// 	src := C.GoString(source)
// 	newDF := df.Column(newName, CollectList(src))
// 	newJSON, err := json.Marshal(newDF)
// 	if err != nil {
// 		log.Fatalf("ColumnCollectList: marshal error: %v", err)
// 	}
// 	return C.CString(string(newJSON))
// }

// // ColumnCollectSet applies CollectSet on the specified source column
// // and creates a new column.
// //
// //export ColumnCollectSet
// func ColumnCollectSet(dfJson *C.char, newCol *C.char, source *C.char) *C.char {
// 	var df DataFrame
// 	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
// 		log.Fatalf("ColumnCollectSet: unmarshal error: %v", err)
// 	}
// 	newName := C.GoString(newCol)
// 	src := C.GoString(source)
// 	newDF := df.Column(newName, CollectSet(src))
// 	newJSON, err := json.Marshal(newDF)
// 	if err != nil {
// 		log.Fatalf("ColumnCollectSet: marshal error: %v", err)
// 	}
// 	return C.CString(string(newJSON))
// }

// // ColumnSplit applies Split on the specified source column with the given delimiter
// // and creates a new column.
// //
// //export ColumnSplit
// func ColumnSplit(dfJson *C.char, newCol *C.char, source *C.char, delim *C.char) *C.char {
// 	var df DataFrame
// 	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
// 		log.Fatalf("ColumnSplit: unmarshal error: %v", err)
// 	}
// 	newName := C.GoString(newCol)
// 	src := C.GoString(source)
// 	delimiter := C.GoString(delim)
// 	newDF := df.Column(newName, Split(src, delimiter))
// 	newJSON, err := json.Marshal(newDF)
// 	if err != nil {
// 		log.Fatalf("ColumnSplit: marshal error: %v", err)
// 	}
// 	return C.CString(string(newJSON))
// }

// // CollectList returns a Column that is an array of the given column's values.
// func CollectList(name string) Column {
// 	return Column{
// 		Name: name,
// 		Fn: func(row map[string]interface{}) interface{} {
// 			values := []interface{}{}
// 			values = append(values, row[name])

// 			return values
// 		},
// 	}
// }

// // CollectSet returns a Column that is a set of unique values from the given column.
// func CollectSet(name string) Column {
// 	return Column{
// 		Name: fmt.Sprintf("CollectSet(%s)", name),
// 		Fn: func(row map[string]interface{}) interface{} {
// 			valueSet := make(map[interface{}]bool)
// 			for _, val := range row[name].([]interface{}) {
// 				valueSet[val] = true
// 			}
// 			values := []interface{}{}
// 			for val := range valueSet {
// 				values = append(values, val)
// 			}
// 			return values
// 		},
// 	}
// }

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
func (df *DataFrame) Columns() []string {
	return df.Cols
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

// count
func (df *DataFrame) Count() int {
	return df.Rows
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

// collect
func (df *DataFrame) Collect(c string) []interface{} {
	if values, exists := df.Data[c]; exists {
		return values
	}
	return []interface{}{}
}

// schema of json ?

// SINKS --------------------------------------------------

// dataframe to csv file
func (df *DataFrame) ToCSVFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the column headers directly.
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

//export ToCSVFileWrapper
func ToCSVFileWrapper(dfJson *C.char, filename *C.char) *C.char {
	var df DataFrame
	if err := json.Unmarshal([]byte(C.GoString(dfJson)), &df); err != nil {
		log.Fatalf("ToCSVFileWrapper: unmarshal error: %v", err)
	}
	err := df.ToCSVFile(C.GoString(filename))
	if err != nil {
		return C.CString(err.Error())
	}
	return C.CString("success")
}

// quote identifiers for SQLite
func quoteIdent(s string) string {
    return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func inferSQLiteTypes(df *DataFrame) map[string]string {
    types := make(map[string]string, len(df.Cols))
    for _, col := range df.Cols {
        sqlType := "TEXT"
        if values, ok := df.Data[col]; ok {
            for _, v := range values {
                if v == nil {
                    continue
                }
                switch v.(type) {
                case int, int32, int64, bool:
                    sqlType = "INTEGER"
                case float32, float64:
                    sqlType = "REAL"
                case []byte:
                    sqlType = "BLOB"
                default:
                    sqlType = "TEXT"
                }
                break
            }
        }
        types[col] = sqlType
    }
    return types
}

func tableExists(tx *sql.Tx, table string) (bool, error) {
    var cnt int
    row := tx.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table)
    if err := row.Scan(&cnt); err != nil {
        return false, err
    }
    return cnt > 0, nil
}

func getExistingColumns(tx *sql.Tx, table string) (map[string]bool, error) {
    rows, err := tx.Query(`PRAGMA table_info(` + quoteIdent(table) + `)`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    out := map[string]bool{}
    for rows.Next() {
        var cid int
        var name, ctype string
        var notnull, pk int
        var dflt interface{}
        _ = dflt
        if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
            return nil, err
        }
        out[name] = true
    }
    return out, rows.Err()
}

func ensureTableAndColumns(tx *sql.Tx, table string, df *DataFrame) error {
    colTypes := inferSQLiteTypes(df)
    exists, err := tableExists(tx, table)
    if err != nil {
        return err
    }
    if !exists {
        defs := make([]string, 0, len(df.Cols))
        for _, c := range df.Cols {
            defs = append(defs, fmt.Sprintf("%s %s", quoteIdent(c), colTypes[c]))
        }
        createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s)`, quoteIdent(table), strings.Join(defs, ","))
        if _, err := tx.Exec(createSQL); err != nil {
            return err
        }
        return nil
    }
    // add any missing columns
    current, err := getExistingColumns(tx, table)
    if err != nil {
        return err
    }
    for _, c := range df.Cols {
        if !current[c] {
            if _, err := tx.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, quoteIdent(table), quoteIdent(c), colTypes[c])); err != nil {
                return err
            }
        }
    }
    return nil
}

// Upsert helper (will be used by WriteSqlite for mode=upsert)
func upsertSqliteTx(tx *sql.Tx, table string, df *DataFrame, keys []string, createIndex bool) error {
    if len(keys) == 0 {
        return fmt.Errorf("UpsertSqlite: at least one key column is required")
    }
    if err := ensureTableAndColumns(tx, table, df); err != nil {
        return err
    }
    if createIndex {
        ixName := "ux_" + strings.ReplaceAll(strings.ReplaceAll(table, `"`, "_"), " ", "_") + "_" + strings.ReplaceAll(strings.Join(keys, "_"), `"`, "_")
        qKeys := make([]string, 0, len(keys))
        for _, k := range keys {
            qKeys = append(qKeys, quoteIdent(k))
        }
        _, _ = tx.Exec(fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (%s)`, quoteIdent(ixName), quoteIdent(table), strings.Join(qKeys, ",")))
    }

    // Try modern ON CONFLICT DO UPDATE (SQLite >= 3.24.0)
    colsQuoted := make([]string, 0, len(df.Cols))
    valHolders := make([]string, 0, len(df.Cols))
    setClauses := []string{}
    keySet := map[string]struct{}{}
    for _, k := range keys {
        keySet[k] = struct{}{}
    }
    for _, c := range df.Cols {
        colsQuoted = append(colsQuoted, quoteIdent(c))
        valHolders = append(valHolders, ":"+c)
        if _, isKey := keySet[c]; !isKey {
            setClauses = append(setClauses, fmt.Sprintf("%s=excluded.%s", quoteIdent(c), quoteIdent(c)))
        }
    }
    conflictCols := make([]string, 0, len(keys))
    for _, k := range keys {
        conflictCols = append(conflictCols, quoteIdent(k))
    }
    upsertSQL := fmt.Sprintf(
        `INSERT INTO %s (%s) VALUES (%s) ON CONFLICT(%s) DO UPDATE SET %s`,
        quoteIdent(table),
        strings.Join(colsQuoted, ","),
        strings.Join(valHolders, ","),
        strings.Join(conflictCols, ","),
        strings.Join(setClauses, ","),
    )

    if stmt, err := tx.Prepare(upsertSQL); err == nil {
        defer stmt.Close()
        for i := 0; i < df.Rows; i++ {
            args := make([]interface{}, 0, len(df.Cols))
            for _, c := range df.Cols {
                args = append(args, sql.Named(c, df.Data[c][i]))
            }
            if _, err := stmt.Exec(args...); err != nil {
                return fmt.Errorf("UpsertSqlite: exec upsert error at row %d: %w", i, err)
            }
        }
        return nil
    }

    // Fallback: UPDATE then INSERT per row (older SQLite)
    setQ := []string{}
    for _, c := range df.Cols {
        if _, isKey := keySet[c]; !isKey {
            setQ = append(setQ, fmt.Sprintf("%s=?", quoteIdent(c)))
        }
    }
    whereQ := []string{}
    for _, k := range keys {
        whereQ = append(whereQ, fmt.Sprintf("%s=?", quoteIdent(k)))
    }
    updateSQL := fmt.Sprintf(`UPDATE %s SET %s WHERE %s`, quoteIdent(table), strings.Join(setQ, ","), strings.Join(whereQ, " AND "))
    upStmt, err := tx.Prepare(updateSQL)
    if err != nil {
        return fmt.Errorf("UpsertSqlite: prepare update error: %w", err)
    }
    defer upStmt.Close()

    insCols := make([]string, 0, len(df.Cols))
    insQ := make([]string, 0, len(df.Cols))
    for _, c := range df.Cols {
        insCols = append(insCols, quoteIdent(c))
        insQ = append(insQ, "?")
    }
    insertSQL := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, quoteIdent(table), strings.Join(insCols, ","), strings.Join(insQ, ","))
    inStmt, err := tx.Prepare(insertSQL)
    if err != nil {
        return fmt.Errorf("UpsertSqlite: prepare insert error: %w", err)
    }
    defer inStmt.Close()

    for i := 0; i < df.Rows; i++ {
        upArgs := make([]interface{}, 0, len(setQ)+len(keys))
        for _, c := range df.Cols {
            if _, isKey := keySet[c]; !isKey {
                upArgs = append(upArgs, df.Data[c][i])
            }
        }
        for _, k := range keys {
            upArgs = append(upArgs, df.Data[k][i])
        }
        res, err := upStmt.Exec(upArgs...)
        if err != nil {
            return fmt.Errorf("UpsertSqlite: update error at row %d: %w", i, err)
        }
        if aff, _ := res.RowsAffected(); aff == 0 {
            insArgs := make([]interface{}, 0, len(df.Cols))
            for _, c := range df.Cols {
                insArgs = append(insArgs, df.Data[c][i])
            }
            if _, err := inStmt.Exec(insArgs...); err != nil {
                return fmt.Errorf("UpsertSqlite: insert error at row %d: %w", i, err)
            }
        }
    }
    return nil
}

// WriteSqlite performs overwrite or upsert based on mode.
func (df *DataFrame) WriteSqlite(dbPath string, table string, mode string, keys []string, createIndex bool) error {
    if df == nil {
        return fmt.Errorf("WriteSqlite: nil dataframe")
    }
    if table == "" {
        return fmt.Errorf("WriteSqlite: table is required")
    }
    if df.Rows == 0 && strings.ToLower(mode) == "overwrite" {
        // Still ensure table exists so the call is idempotent.
        db, err := sql.Open("sqlite3", dbPath)
        if err != nil { return err }
        defer db.Close()
        tx, err := db.Begin()
        if err != nil { return err }
        defer func(){ _ = tx.Rollback() }()
        if err := ensureTableAndColumns(tx, table, df); err != nil { return err }
        return tx.Commit()
    }

    db, err := sql.Open("sqlite3", dbPath)
    if err != nil {
        return fmt.Errorf("WriteSqlite: open error: %w", err)
    }
    defer db.Close()

    tx, err := db.Begin()
    if err != nil {
        return fmt.Errorf("WriteSqlite: begin tx error: %w", err)
    }
    defer func() { _ = tx.Rollback() }()

    switch strings.ToLower(mode) {
    case "overwrite":
        // Ensure table and columns exist (create or add columns)
        if err := ensureTableAndColumns(tx, table, df); err != nil {
            return err
        }
        // Clear table
        if _, err := tx.Exec(`DELETE FROM ` + quoteIdent(table)); err != nil {
            return fmt.Errorf("WriteSqlite: delete error: %w", err)
        }
        // Insert all rows
        colsQuoted := make([]string, 0, len(df.Cols))
        valQ := make([]string, 0, len(df.Cols))
        for _, c := range df.Cols {
            colsQuoted = append(colsQuoted, quoteIdent(c))
            valQ = append(valQ, ":"+c)
        }
        insertSQL := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, quoteIdent(table), strings.Join(colsQuoted, ","), strings.Join(valQ, ","))
        stmt, err := tx.Prepare(insertSQL)
        if err != nil {
            return fmt.Errorf("WriteSqlite: prepare insert error: %w", err)
        }
        defer stmt.Close()
        for i := 0; i < df.Rows; i++ {
            args := make([]interface{}, 0, len(df.Cols))
            for _, c := range df.Cols {
                args = append(args, sql.Named(c, df.Data[c][i]))
            }
            if _, err := stmt.Exec(args...); err != nil {
                return fmt.Errorf("WriteSqlite: insert error at row %d: %w", i, err)
            }
        }
    case "upsert":
        if len(keys) == 0 {
            return fmt.Errorf("WriteSqlite: upsert mode requires keys")
        }
        if err := upsertSqliteTx(tx, table, df, keys, createIndex); err != nil {
            return err
        }
    default:
        return fmt.Errorf("WriteSqlite: unsupported mode %q (use 'overwrite' or 'upsert')", mode)
    }

    if err := tx.Commit(); err != nil {
        return fmt.Errorf("WriteSqlite: commit error: %w", err)
    }
    return nil
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
// END --------------------------------------------------

func main() {}
