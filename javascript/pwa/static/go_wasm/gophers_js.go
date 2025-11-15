//go:build js && wasm

package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sync"
	"syscall/js"
	"strings"
	"strconv"

	g "github.com/SPEARTECH/gophers/go/gophers"
)

var finalReg js.Value
var finalCb js.Func

var (
	storeMu sync.Mutex
	nextID  = 1
	store   = map[int]*g.DataFrame{}
)

// Report and Chart stores
var (
	reportStoreMu sync.Mutex
	nextReportID  = 1
	reportStore   = map[int]*g.Report{}
	chartStoreMu  sync.Mutex
	nextChartID   = 1
	chartStore    = map[int]g.Chart{}
)

var chartSeq int

func putReport(r *g.Report) int {
	reportStoreMu.Lock()
	defer reportStoreMu.Unlock()
	id := nextReportID
	nextReportID++
	reportStore[id] = r
	return id
}
func getReport(id int) *g.Report {
	reportStoreMu.Lock()
	defer reportStoreMu.Unlock()
	return reportStore[id]
}
func putChart(ch g.Chart) int {
	chartStoreMu.Lock()
	defer chartStoreMu.Unlock()
	id := nextChartID
	nextChartID++
	chartStore[id] = ch
	return id
}
func getChart(id int) (g.Chart, bool) {
	chartStoreMu.Lock()
	defer chartStoreMu.Unlock()
	ch, ok := chartStore[id]
	return ch, ok
}

func put(df *g.DataFrame) int {
	storeMu.Lock()
	defer storeMu.Unlock()
	id := nextID
	nextID++
	store[id] = df
	return id
}

func get(id int) *g.DataFrame {
	storeMu.Lock()
	defer storeMu.Unlock()
	return store[id]
}

func free(this js.Value, args []js.Value) any {
	if len(args) < 1 {
		return "error: usage Free(handle)"
	}
	id := args[0].Int()
	storeMu.Lock()
	delete(store, id)
	storeMu.Unlock()
	return "ok"
}

// Parse a JS value into a g.ColumnExpr (used by Filter)
func exprFromJS(v js.Value) (g.ColumnExpr, error) {
    switch v.Type() {
    case js.TypeString:
        // shorthand: "colName" -> {"Type":"col","Name":"colName"}
        return g.ColumnExpr{Type: "col", Name: v.String()}, nil
    case js.TypeObject:
        j := js.Global().Get("JSON").Call("stringify", v).String()
        var e g.ColumnExpr
        if err := json.Unmarshal([]byte(j), &e); err != nil {
            return g.ColumnExpr{}, fmt.Errorf("unmarshal expr: %w", err)
        }
        return e, nil
    default:
        // literal
        return g.ColumnExpr{Type: "lit", Value: jsValToAny(v)}, nil
    }
}

// Helpers to convert JS -> Go values
func jsValToAny(v js.Value) interface{} {
    switch v.Type() {
    case js.TypeUndefined, js.TypeNull:
        return nil
    case js.TypeBoolean:
        return v.Bool()
    case js.TypeNumber:
        return v.Float() // Go json uses float64 for numbers
    case js.TypeString:
        return v.String()
    case js.TypeObject:
        // Array
        if arr := js.Global().Get("Array"); arr.Truthy() && v.InstanceOf(arr) {
            n := v.Length()
            out := make([]interface{}, n)
            for i := 0; i < n; i++ {
                out[i] = jsValToAny(v.Index(i))
            }
            return out
        }
        // Plain object -> map[string]interface{}
        keys := js.Global().Get("Object").Call("keys", v)
        n := keys.Length()
        m := make(map[string]interface{}, n)
        for i := 0; i < n; i++ {
            k := keys.Index(i).String()
            m[k] = jsValToAny(v.Get(k))
        }
        return m
    default:
        return fmt.Sprintf("%v", v) // fallback
    }
}

// buildColumnFromJS turns a JS value into g.Column via ColumnExpr -> Compile.
func buildColumnFromJS(v js.Value) (g.Column, error) {
    // strings are shorthand for column refs
    if v.Type() == js.TypeString {
        return g.Col(v.String()), nil
    }
    // allow passing already-stringified JSON
    var jsonText string
    switch v.Type() {
    case js.TypeObject:
        // JSON.stringify(v)
        jsonText = js.Global().Get("JSON").Call("stringify", v).String()
    case js.TypeString:
        jsonText = v.String()
    default:
        // treat as literal
        return g.Lit(jsValToAny(v)), nil
    }
    var expr g.ColumnExpr
    if err := json.Unmarshal([]byte(jsonText), &expr); err != nil {
        return g.Column{}, fmt.Errorf("unmarshal expr: %w", err)
    }
    col := g.Compile(expr)
    return col, nil
}

// ---- Aggregation helpers (JS -> Go) ----
func aggFromJS(v js.Value) (g.Aggregation, error) {
    // Accept:
    //  - { op: "sum"|"max"|"min"|"median"|"mean"|"mode"|"unique"|"first", col: "price" }
    //  - "sum:price" (string shorthand)
    if !v.Truthy() {
        return g.Aggregation{}, fmt.Errorf("invalid aggregation")
    }
    if v.Type() == js.TypeObject {
        op := strings.ToLower(v.Get("op").String())
        col := v.Get("col").String()
        switch op {
        case "sum":
            return g.Sum(col), nil
        case "max":
            return g.Max(col), nil
        case "min":
            return g.Min(col), nil
        case "median":
            return g.Median(col), nil
        case "mean", "avg", "average":
            return g.Mean(col), nil
        case "mode":
            return g.Mode(col), nil
        case "unique", "count_distinct":
            return g.Unique(col), nil
        case "first":
            return g.First(col), nil
        default:
            return g.Aggregation{}, fmt.Errorf("unknown op %q", op)
        }
    }
    if v.Type() == js.TypeString {
        // "op:col" or "op(col)"
        s := strings.TrimSpace(v.String())
        if i := strings.IndexByte(s, ':'); i > 0 {
            op := strings.ToLower(strings.TrimSpace(s[:i]))
            col := strings.TrimSpace(s[i+1:])
            return aggFromJS(js.ValueOf(map[string]any{"op": op, "col": col}))
        }
        if strings.HasSuffix(s, ")") && strings.Contains(s, "(") {
            i := strings.IndexByte(s, '(')
            op := strings.ToLower(strings.TrimSpace(s[:i]))
            col := strings.TrimSuffix(strings.TrimSpace(s[i+1:]), ")")
            return aggFromJS(js.ValueOf(map[string]any{"op": op, "col": col}))
        }
    }
    return g.Aggregation{}, fmt.Errorf("unsupported aggregation spec")
}

func aggsFromJS(args []js.Value) ([]g.Aggregation, error) {
    // Accept varargs aggs or a single array of aggs
    out := []g.Aggregation{}
    if len(args) == 1 {
        if arr := js.Global().Get("Array"); arr.Truthy() && args[0].InstanceOf(arr) {
            a := args[0]
            for i := 0; i < a.Length(); i++ {
                agg, err := aggFromJS(a.Index(i))
                if err != nil { return nil, err }
                out = append(out, agg)
            }
            return out, nil
        }
    }
    for _, v := range args {
        agg, err := aggFromJS(v)
        if err != nil { return nil, err }
        out = append(out, agg)
    }
    return out, nil
}

// Helper: build a JS DataFrame object with methods bound to an internal handle.
func dfObject(id int) js.Value {
	obj := js.Global().Get("Object").New()
	obj.Set("handle", id)
	// ------ SOURCES -------

    // df.Clone() -> deep copy; returns a new DataFrame
    obj.Set("Clone", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil {
            return "error: invalid handle"
        }
        out := df.Clone()
        newID := put(out)
        return dfObject(newID)
    }))


	// ------ SINKS -------
	// df.ToJSON(format?) -> rows array (default) or columnar object
	obj.Set("ToJSON", js.FuncOf(func(this js.Value, args []js.Value) any {
		df := get(id)
		if df == nil {
			return "error: invalid handle"
		}
		format := "rows"
		if len(args) >= 1 && args[0].Type() == js.TypeString {
			format = args[0].String()
		}
		if format == "columnar" {
			payload := map[string]any{
				"cols": df.Cols,
				"data": df.Data,
				"rows": df.Rows,
			}
			b, _ := json.Marshal(payload)
			return js.Global().Get("JSON").Call("parse", string(b))
		}
		// rows format
		rows := df.ToRows()
		b, _ := json.Marshal(rows)
		return js.Global().Get("JSON").Call("parse", string(b))
	}))
	// df.ToJSONFile(filename?, format?, pretty?)
	// - filename: string (default "dataframe.json")
	// - format: "rows" | "columnar" (default "rows")
	// - pretty: boolean (default false)
	obj.Set("ToJSONFile", js.FuncOf(func(this js.Value, args []js.Value) any {
		df := get(id)
		if df == nil {
			return "error: invalid handle"
		}
		filename := "dataframe.json"
		if len(args) >= 1 && args[0].Type() == js.TypeString && args[0].String() != "" {
			filename = args[0].String()
		}
		format := "rows"
		if len(args) >= 2 && args[1].Type() == js.TypeString {
			format = args[1].String()
		}
		pretty := false
		if len(args) >= 3 && args[2].Type() == js.TypeBoolean {
			pretty = args[2].Bool()
		}

		var b []byte
		if format == "columnar" {
			payload := map[string]any{
				"cols": df.Cols,
				"data": df.Data,
				"rows": df.Rows,
			}
			if pretty {
				b, _ = json.MarshalIndent(payload, "", "  ")
			} else {
				b, _ = json.Marshal(payload)
			}
		} else {
			rows := df.ToRows()
			if pretty {
				b, _ = json.MarshalIndent(rows, "", "  ")
			} else {
				b, _ = json.Marshal(rows)
			}
		}
		text := string(b)

		// Blob + anchor download
		array := js.Global().Get("Array").New()
		array.Call("push", js.ValueOf(text))
		opts := js.Global().Get("Object").New()
		opts.Set("type", "application/json;charset=utf-8")
		blob := js.Global().Get("Blob").New(array, opts)
		url := js.Global().Get("URL").Call("createObjectURL", blob)

		doc := js.Global().Get("document")
		a := doc.Call("createElement", "a")
		a.Set("href", url)
		a.Set("download", filename)
		a.Set("rel", "noopener")
		// append, click, remove
		doc.Get("body").Call("appendChild", a)
		a.Call("click")
		a.Get("parentNode").Call("removeChild", a)
		// cleanup
		js.Global().Get("setTimeout").Invoke(js.FuncOf(func(this js.Value, args []js.Value) any {
			js.Global().Get("URL").Call("revokeObjectURL", url)
			return nil
		}), 1000)

		return "ok"
	}))
	obj.Set("ToCSVFile", js.FuncOf(func(this js.Value, args []js.Value) any {
		df := get(id)
		if df == nil {
			return "error: invalid handle"
		}
		filename := "dataframe.csv"
		if len(args) >= 1 && args[0].Type() == js.TypeString && args[0].String() != "" {
			filename = args[0].String()
		}
		delimiter := ','
		if len(args) >= 2 && args[1].Type() == js.TypeString && args[1].String() != "" {
			runes := []rune(args[1].String())
			if len(runes) > 0 {
				delimiter = runes[0]
			}
		}
		includeHeader := true
		if len(args) >= 3 && args[2].Type() == js.TypeBoolean {
			includeHeader = args[2].Bool()
		}

		// Build CSV text
		var buf bytes.Buffer
		cw := csv.NewWriter(&buf)
		cw.Comma = delimiter

		if includeHeader {
			if err := cw.Write(df.Cols); err != nil {
				return fmt.Sprintf("error: write header: %v", err)
			}
		}
		for i := 0; i < df.Rows; i++ {
			rec := make([]string, len(df.Cols))
			for ci, col := range df.Cols {
				colData := df.Data[col]
				var v interface{}
				if i < len(colData) {
					v = colData[i]
				}
				// stringify: strings as-is; objects/slices as JSON; others via fmt.Sprint
				switch t := v.(type) {
				case nil:
					rec[ci] = ""
				case string:
					rec[ci] = t
				default:
					// Try JSON for complex types, else fallback to fmt.Sprint
					if b, err := json.Marshal(v); err == nil && (t == nil || (fmt.Sprintf("%T", v)[0] == '[' || fmt.Sprintf("%T", v)[0] == 'm')) {
						rec[ci] = string(b)
					} else {
						rec[ci] = fmt.Sprint(v)
					}
				}
			}
			if err := cw.Write(rec); err != nil {
				return fmt.Sprintf("error: write row %d: %v", i, err)
			}
		}
		cw.Flush()
		if err := cw.Error(); err != nil {
			return fmt.Sprintf("error: csv flush: %v", err)
		}
		csvText := buf.String()

		// Blob + anchor download
		array := js.Global().Get("Array").New()
		array.Call("push", js.ValueOf(csvText))
		opts := js.Global().Get("Object").New()
		opts.Set("type", "text/csv;charset=utf-8")
		blob := js.Global().Get("Blob").New(array, opts)
		url := js.Global().Get("URL").Call("createObjectURL", blob)

		doc := js.Global().Get("document")
		a := doc.Call("createElement", "a")
		a.Set("href", url)
		a.Set("download", filename)
		a.Set("rel", "noopener")
		doc.Get("body").Call("appendChild", a)
		a.Call("click")
		a.Get("parentNode").Call("removeChild", a)
		js.Global().Get("setTimeout").Invoke(js.FuncOf(func(this js.Value, args []js.Value) any {
			js.Global().Get("URL").Call("revokeObjectURL", url)
			return nil
		}), 1000)

		return "ok"
	}))
    // df.ToNDJSONFile(filename) -> triggers browser download; returns "ok"
    obj.Set("ToNDJSONFile", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil {
            return "error: invalid handle"
        }
        if len(args) < 1 || args[0].Type() != js.TypeString {
            return "error: usage ToNDJSONFile(filename)"
        }
        filename := args[0].String()

        // Build NDJSON string
        var b strings.Builder
        for i := 0; i < df.Rows; i++ {
            row := make(map[string]interface{}, len(df.Cols))
            for _, c := range df.Cols {
                row[c] = df.Data[c][i]
            }
            j, _ := json.Marshal(row)
            b.Write(j)
            b.WriteByte('\n')
        }
        nd := []byte(b.String())

        // Create Blob and download
        u8 := js.Global().Get("Uint8Array").New(len(nd))
        _ = js.CopyBytesToJS(u8, nd)
        parts := js.Global().Get("Array").New()
        parts.Call("push", u8)
        blob := js.Global().Get("Blob").New(parts, map[string]any{"type": "application/x-ndjson"})
        url := js.Global().Get("URL").Call("createObjectURL", blob)
        doc := js.Global().Get("document")
        a := doc.Call("createElement", "a")
        a.Set("href", url)
        a.Set("download", filename)
        doc.Get("body").Call("appendChild", a)
        a.Call("click")
        a.Get("parentNode").Call("removeChild", a)
        js.Global().Get("URL").Call("revokeObjectURL", url)
        return "ok"
    }))
    // df.PostAPI(url[, headersObj, queryObj]) -> string (raw response body)
    obj.Set("PostAPI", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil {
            return "error: invalid handle"
        }
        if len(args) < 1 || args[0].Type() != js.TypeString {
            return "error: usage PostAPI(url[, headersObj, queryObj])"
        }
        url := args[0].String()
        var headers, query map[string]string
        if len(args) >= 2 && args[1].Type() == js.TypeObject {
            headers = jsObjToStringMap(args[1])
        }
        if len(args) >= 3 && args[2].Type() == js.TypeObject {
            query = jsObjToStringMap(args[2])
        }

        body, err := df.PostAPI(url, headers, query)
        if err != nil {
            return "error: " + err.Error()
        }
        return body
    }))
	// --------- Transforms ---------
    // df.Column(newName, exprSpec) -> in-place; returns same df
    obj.Set("Column", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil {
            return "error: invalid handle"
        }
        if len(args) < 2 || args[0].Type() != js.TypeString {
            return "error: usage Column(newName, exprSpec)"
        }
        colName := args[0].String()

        // Build ColumnExpr from JS spec
        colSpec, err := exprFromJS(args[1])
        if err != nil {
            return "error: " + err.Error()
        }

        df.Column(colName, colSpec)
        return dfObject(id)
    }))    
	// df.Flatten(...cols) or df.Flatten(['nested1','nested2']) -> in-place; returns same df
    obj.Set("Flatten", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil {
            return "error: invalid handle"
        }
        if len(args) == 0 {
            return "error: usage Flatten(col1, col2, ...) or Flatten(['col1','col2'])"
        }
        cols := []string{}
        // Array form
        arrCtor := js.Global().Get("Array")
        if args[0].Type() == js.TypeObject && arrCtor.Truthy() && args[0].InstanceOf(arrCtor) {
            a := args[0]
            for i := 0; i < a.Length(); i++ {
                v := a.Index(i)
                if v.Type() == js.TypeString {
                    cols = append(cols, v.String())
                }
            }
        } else {
            // Varargs strings
            for _, v := range args {
                if v.Type() == js.TypeString {
                    cols = append(cols, v.String())
                }
            }
        }
        if len(cols) == 0 {
            return "error: no columns supplied"
        }
        df.Flatten(cols)
        return dfObject(id)
    }))
    // df.StringArrayConvert(col) or df.StringArrayConvert('c1','c2') or df.StringArrayConvert(['c1','c2'])
    obj.Set("StringArrayConvert", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil {
            return "error: invalid handle"
        }
        if len(args) == 0 {
            return "error: usage StringArrayConvert(col) | StringArrayConvert('c1','c2') | StringArrayConvert(['c1','c2'])"
        }

        cols := []string{}
        arrCtor := js.Global().Get("Array")
        if args[0].Type() == js.TypeObject && arrCtor.Truthy() && args[0].InstanceOf(arrCtor) {
            a := args[0]
            for i := 0; i < a.Length(); i++ {
                if v := a.Index(i); v.Type() == js.TypeString {
                    cols = append(cols, v.String())
                }
            }
        } else {
            for _, v := range args {
                if v.Type() == js.TypeString {
                    cols = append(cols, v.String())
                }
            }
        }
        if len(cols) == 0 {
            return "error: no columns supplied"
        }

        for _, c := range cols {
            df.StringArrayConvert(c)
        }
        return dfObject(id)
    }))
    // df.Filter(expr) -> returns a new DataFrame
    obj.Set("Filter", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil {
            return "error: invalid handle"
        }
        if len(args) < 1 {
            return "error: usage Filter(expr)"
        }
        expr, err := exprFromJS(args[0])
        if err != nil {
            return "error: " + err.Error()
        }
        out := df.Filter(expr)
        newID := put(out)
        return dfObject(newID)
    }))
	// df.Sort() -> in-place; returns same df
	obj.Set("Sort", js.FuncOf(func(this js.Value, args []js.Value) any {
		df := get(id)
		if df == nil {
			return "error: invalid handle"
		}
		df.Sort()
		return dfObject(id)
	}))
    // df.Explode(...cols) or df.Explode(['c1','c2']) -> in-place; returns same df
    obj.Set("Explode", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil {
            return "error: invalid handle"
        }
        if len(args) == 0 {
            return "error: usage Explode(col1, col2, ...) or Explode(['col1','col2'])"
        }
        cols := []string{}
        arrCtor := js.Global().Get("Array")
        if args[0].Type() == js.TypeObject && arrCtor.Truthy() && args[0].InstanceOf(arrCtor) {
            a := args[0]
            for i := 0; i < a.Length(); i++ {
                v := a.Index(i)
                if v.Type() == js.TypeString {
                    cols = append(cols, v.String())
                }
            }
        } else {
            for _, v := range args {
                if v.Type() == js.TypeString {
                    cols = append(cols, v.String())
                }
            }
        }
        if len(cols) == 0 {
            return "error: no columns supplied"
        }
        df.Explode(cols...)
        return dfObject(id)
    }))
    // df.KeysToCols(col) or df.KeysToCols('c1','c2') or df.KeysToCols(['c1','c2']) -> in-place; returns same df
    obj.Set("KeysToCols", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil {
            return "error: invalid handle"
        }
        if len(args) == 0 {
            return "error: usage KeysToCols(col) | KeysToCols('c1','c2') | KeysToCols(['c1','c2'])"
        }

        cols := []string{}
        arrCtor := js.Global().Get("Array")
        if args[0].Type() == js.TypeObject && arrCtor.Truthy() && args[0].InstanceOf(arrCtor) {
            a := args[0]
            for i := 0; i < a.Length(); i++ {
                if v := a.Index(i); v.Type() == js.TypeString {
                    cols = append(cols, v.String())
                }
            }
        } else {
            for _, v := range args {
                if v.Type() == js.TypeString {
                    cols = append(cols, v.String())
                }
            }
        }
        if len(cols) == 0 {
            return "error: no columns supplied"
        }

        for _, c := range cols {
            df.KeysToCols(c)
        }
        return dfObject(id)
    }))
	// df.Rename(oldName, newName) -> returns a new DataFrame
	obj.Set("Rename", js.FuncOf(func(this js.Value, args []js.Value) any {
		df := get(id)
		if df == nil {
			return "error: invalid handle"
		}
		if len(args) < 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
			return "error: usage Rename(oldName, newName)"
		}
		out := df.Rename(args[0].String(), args[1].String())
		newID := put(out)
		return dfObject(newID)
	}))
    // df.FillNA(replacementString) -> in-place; returns same df
    obj.Set("FillNA", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil {
            return "error: invalid handle"
        }
        if len(args) < 1 || args[0].Type() != js.TypeString {
            return "error: usage FillNA(replacementString)"
        }
        repl := args[0].String()
        df.FillNA(repl)
        return dfObject(id)
    }))
	// df.DropNA() -> in-place; returns same df
	obj.Set("DropNA", js.FuncOf(func(this js.Value, args []js.Value) any {
		df := get(id)
		if df == nil {
			return "error: invalid handle"
		}
		df.DropNA()
		return dfObject(id)
	}))
	// df.DropDuplicates() | df.DropDuplicates('c1','c2') | df.DropDuplicates(['c1','c2'])
	obj.Set("DropDuplicates", js.FuncOf(func(this js.Value, args []js.Value) any {
		df := get(id)
		if df == nil {
			return "error: invalid handle"
		}
		// No args -> use all columns
		if len(args) == 0 {
			df.DropDuplicates()
			return dfObject(id)
		}

		cols := []string{}
		arrCtor := js.Global().Get("Array")
		if len(args) == 1 && args[0].Type() == js.TypeObject && arrCtor.Truthy() && args[0].InstanceOf(arrCtor) {
			a := args[0]
			for i := 0; i < a.Length(); i++ {
				if v := a.Index(i); v.Type() == js.TypeString {
					cols = append(cols, v.String())
				}
			}
		} else {
			for _, v := range args {
				if v.Type() == js.TypeString {
					cols = append(cols, v.String())
				}
			}
		}

		if len(cols) == 0 {
			df.DropDuplicates()
		} else {
			df.DropDuplicates(cols...)
		}
		return dfObject(id)
	}))
	// df.Select(...cols) or df.select(['col1','col2', ...]) -> new DataFrame object
    obj.Set("Select", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil {
            return "error: invalid handle"
        }
        cols := make([]string, 0, len(args))
        if len(args) == 0 {
            return "error: usage select(...cols) or select(['a','b'])"
        }
        // If first arg is an Array, treat it as the list of column names
        if js.Global().Get("Array").Truthy() && args[0].InstanceOf(js.Global().Get("Array")) {
            arr := args[0]
            l := arr.Length()
            for i := 0; i < l; i++ {
                v := arr.Index(i)
                if v.Type() == js.TypeString {
                    cols = append(cols, v.String())
                }
            }
        } else {
            // Varargs strings
            for _, a := range args {
                if a.Type() == js.TypeString {
                    cols = append(cols, a.String())
                }
            }
        }
        if len(cols) == 0 {
            return "error: select requires at least one column name"
        }
        out := df.Select(cols...)
        newID := put(out)
        return dfObject(newID)
    }))
	// df.GroupBy(key, ...aggs) or df.GroupBy(key, [aggSpec...]) -> returns new DataFrame
	obj.Set("GroupBy", js.FuncOf(func(this js.Value, args []js.Value) any {
		df := get(id)
		if df == nil {
			return "error: invalid handle"
		}
		if len(args) < 2 || args[0].Type() != js.TypeString {
			return "error: usage GroupBy(key, aggs...)"
		}
		key := args[0].String()
		aggs, err := aggsFromJS(args[1:])
		if err != nil {
			return "error: " + err.Error()
		}
		out := df.GroupBy(key, aggs...)
		newID := put(out)
		return dfObject(newID)
	}))
    // df.Join(otherDf, leftOn, rightOn, joinType?) -> new DataFrame
    // joinType: "inner" | "left" | "right" | "outer" (default "inner")
    obj.Set("Join", js.FuncOf(func(this js.Value, args []js.Value) any {
        left := get(id)
        if left == nil {
            return "error: invalid left dataframe handle"
        }
        if len(args) < 3 {
            return "error: usage Join(otherDf, leftOn, rightOn, joinType?)"
        }
        otherObj := args[0]
        if otherObj.Type() != js.TypeObject {
            return "error: first arg must be a DataFrame object"
        }
        otherHandle := otherObj.Get("handle")
        if !otherHandle.Truthy() {
            return "error: otherDf missing handle"
        }
        right := get(otherHandle.Int())
        if right == nil {
            return "error: invalid right dataframe handle"
        }
        if args[1].Type() != js.TypeString || args[2].Type() != js.TypeString {
            return "error: leftOn and rightOn must be strings"
        }
        leftOn := args[1].String()
        rightOn := args[2].String()
        joinType := "inner"
        if len(args) >= 4 && args[3].Type() == js.TypeString {
            joinType = strings.ToLower(args[3].String())
        }
        out := left.Join(right, leftOn, rightOn, joinType)
        if out == nil {
            return "error: join failed"
        }
        newID := put(out)
        return dfObject(newID)
    }))
    // df.Union(otherDf[, moreDf...]) -> new DataFrame (vertical append)
    obj.Set("Union", js.FuncOf(func(this js.Value, args []js.Value) any {
        base := get(id)
        if base == nil {
            return "error: invalid base dataframe handle"
        }
        if len(args) < 1 {
            return "error: usage Union(otherDf[, moreDf...])"
        }
        current := base
        for _, a := range args {
            if a.Type() != js.TypeObject {
                return "error: each argument must be a DataFrame object"
            }
            h := a.Get("handle")
            if !h.Truthy() {
                return "error: missing handle on argument"
            }
            other := get(h.Int())
            if other == nil {
                return "error: invalid union target handle"
            }
            current = current.Union(other)
        }
        newID := put(current)
        return dfObject(newID)
    }))
    // df.Drop(...cols) or df.Drop(['c1','c2']) -> in-place; returns same df
    obj.Set("Drop", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil {
            return "error: invalid handle"
        }
        if len(args) == 0 {
            return "error: usage Drop(col1, col2, ...) or Drop(['col1','col2'])"
        }
        cols := []string{}
        arr := js.Global().Get("Array")
        if args[0].Type() == js.TypeObject && arr.Truthy() && args[0].InstanceOf(arr) {
            a := args[0]
            for i := 0; i < a.Length(); i++ {
                v := a.Index(i)
                if v.Type() == js.TypeString {
                    cols = append(cols, v.String())
                }
            }
        } else {
            for _, v := range args {
                if v.Type() == js.TypeString {
                    cols = append(cols, v.String())
                }
            }
        }
        if len(cols) == 0 {
            return "error: no columns supplied"
        }
        df.Drop(cols...)
        return dfObject(id)
    }))
    // df.OrderBy(column, asc?) -> in-place; returns same df
    obj.Set("OrderBy", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil {
            return "error: invalid handle"
        }
        if len(args) < 1 || args[0].Type() != js.TypeString {
            return "error: usage OrderBy(columnName, ascBool?)"
        }
        col := args[0].String()
        asc := true
        if len(args) >= 2 && args[1].Type() == js.TypeBoolean {
            asc = args[1].Bool()
        }
        df.OrderBy(col, asc)
        return dfObject(id)
    }))
	// ---------- Returns ----------

	// df.Show(chars?, recordCount?) -> logs to console; returns string
	obj.Set("Show", js.FuncOf(func(this js.Value, args []js.Value) any {
		df := get(id)
		if df == nil {
			return "error: invalid handle"
		}
		chars := 25
		if len(args) >= 1 && args[0].Type() == js.TypeNumber {
			chars = args[0].Int()
		}
		recordCount := 0 // 0 -> all rows (clamped by Show)
		if len(args) >= 2 && args[1].Type() == js.TypeNumber {
			recordCount = args[1].Int()
		}
		out := df.Show(chars, recordCount)
		// js.Global().Get("console").Call("log", out)
		return out
	}))
	// df.Head(chars?) -> logs first 5 rows to console; returns string
	obj.Set("Head", js.FuncOf(func(this js.Value, args []js.Value) any {
		df := get(id)
		if df == nil {
			return "error: invalid handle"
		}
		chars := 25
		if len(args) >= 1 && args[0].Type() == js.TypeNumber {
			chars = args[0].Int()
		}
		out := df.Head(chars)
		// js.Global().Get("console").Call("log", out)
		return out
	}))

	// df.Tail(chars?) -> logs last 5 rows to console; returns string
	obj.Set("Tail", js.FuncOf(func(this js.Value, args []js.Value) any {
		df := get(id)
		if df == nil {
			return "error: invalid handle"
		}
		chars := 25
		if len(args) >= 1 && args[0].Type() == js.TypeNumber {
			chars = args[0].Int()
		}
		out := df.Tail(chars)
		// js.Global().Get("console").Call("log", out)
		return out
	}))
    // df.Vertical(chars?, recordCount?) -> logs to console; returns string
    obj.Set("Vertical", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil {
            return "error: invalid handle"
        }
        chars := 25
        if len(args) >= 1 && args[0].Type() == js.TypeNumber {
            chars = args[0].Int()
        }
        recordCount := 0 // 0 => all rows (Vertical will clamp)
        if len(args) >= 2 && args[1].Type() == js.TypeNumber {
            recordCount = args[1].Int()
        }
        out := df.Vertical(chars, recordCount)
        // js.Global().Get("console").Call("log", out)
        return out
    }))

	// ---------- Displays ----------

	// df.Display() -> returns helper with .ElementID(id) to mount the generated HTML
	obj.Set("Display", js.FuncOf(func(this js.Value, args []js.Value) any {
		df := get(id)
		if df == nil {
			return "error: invalid handle"
		}
		res := df.Display()
		html, ok := res["text/html"].(string)
		if !ok {
			return "error: display did not return HTML"
		}

		o := js.Global().Get("Object").New()
		o.Set("html", html)

		// chainable: .ElementID("container-id")
		o.Set("ElementID", js.FuncOf(func(this js.Value, a []js.Value) any {
			if len(a) < 1 || a[0].Type() != js.TypeString {
				return "error: ElementID(id)"
			}
			doc := js.Global().Get("document")
			if !doc.Truthy() {
				return "error: document is not available"
			}
			el := doc.Call("getElementById", a[0].String())
			if !el.Truthy() {
				return "error: element not found"
			}

			// If it's a full document, mount inside an iframe
			if strings.Contains(html, "<!DOCTYPE") || strings.Contains(html, "<html") {
				el.Set("innerHTML", "")
				iframe := doc.Call("createElement", "iframe")
				// style
				iframe.Get("style").Set("width", "100%")
				iframe.Get("style").Set("height", "100%")
				iframe.Set("frameBorder", "0")
				// Add sandbox attributes
				iframe.Call("setAttribute", "sandbox", "allow-scripts allow-popups allow-downloads allow-top-navigation-by-user-activation")

				// Prefer srcdoc (supported by modern browsers)
				if !iframe.Get("srcdoc").IsUndefined() {
					iframe.Set("srcdoc", html)
					el.Call("appendChild", iframe)
					return o
				}

				// Fallback: Blob URL
				parts := js.Global().Get("Array").New()
				parts.Call("push", js.ValueOf(html))
				opts := js.Global().Get("Object").New()
				opts.Set("type", "text/html;charset=utf-8")
				blob := js.Global().Get("Blob").New(parts, opts)
				url := js.Global().Get("URL").Call("createObjectURL", blob)

				// Revoke URL after iframe loads
				var onload js.Func
				onload = js.FuncOf(func(this js.Value, _ []js.Value) any {
					js.Global().Get("URL").Call("revokeObjectURL", url)
					onload.Release()
					return nil
				})
				iframe.Set("onload", onload)
				iframe.Set("src", url)

				el.Call("appendChild", iframe)
				return o
			}

			// Otherwise, treat as a fragment
			el.Set("innerHTML", html)
			return o
		}))

		return o
	}))
	// df.DisplayToFile(filename?) -> downloads an HTML file of the DataFrame
	obj.Set("DisplayToFile", js.FuncOf(func(this js.Value, args []js.Value) any {
		df := get(id)
		if df == nil {
			return "error: invalid handle"
		}
		filename := "dataframe.html"
		if len(args) >= 1 && args[0].Type() == js.TypeString && args[0].String() != "" {
			filename = args[0].String()
		}

		// Get HTML from Display()
		res := df.Display()
		html, ok := res["text/html"].(string)
		if !ok {
			return "error: display did not return HTML"
		}

		// Blob + anchor download
		array := js.Global().Get("Array").New()
		array.Call("push", js.ValueOf(html))
		opts := js.Global().Get("Object").New()
		opts.Set("type", "text/html;charset=utf-8")
		blob := js.Global().Get("Blob").New(array, opts)
		url := js.Global().Get("URL").Call("createObjectURL", blob)

		doc := js.Global().Get("document")
		a := doc.Call("createElement", "a")
		a.Set("href", url)
		a.Set("download", filename)
		a.Set("rel", "noopener")
		doc.Get("body").Call("appendChild", a)
		a.Call("click")
		a.Get("parentNode").Call("removeChild", a)

		// cleanup
		js.Global().Get("setTimeout").Invoke(js.FuncOf(func(this js.Value, args []js.Value) any {
			js.Global().Get("URL").Call("revokeObjectURL", url)
			return nil
		}), 1000)

		return "ok"
	}))

	// df.DisplayBrowser(filename?) -> opens HTML in a new tab; returns "ok"
	obj.Set("DisplayBrowser", js.FuncOf(func(this js.Value, args []js.Value) any {
		df := get(id)
		if df == nil {
			return "error: invalid handle"
		}
		// Get HTML from Display()
		res := df.Display()
		html, ok := res["text/html"].(string)
		if !ok {
			return "error: display did not return HTML"
		}

		// Create Blob URL
		array := js.Global().Get("Array").New()
		array.Call("push", js.ValueOf(html))
		opts := js.Global().Get("Object").New()
		opts.Set("type", "text/html;charset=utf-8")
		blob := js.Global().Get("Blob").New(array, opts)
		url := js.Global().Get("URL").Call("createObjectURL", blob)

		// Try window.open; fallback to anchor click
		win := js.Global().Get("window").Call("open", url, "_blank")
		if !win.Truthy() {
			doc := js.Global().Get("document")
			a := doc.Call("createElement", "a")
			a.Set("href", url)
			a.Set("target", "_blank")
			a.Set("rel", "noopener")
			doc.Get("body").Call("appendChild", a)
			a.Call("click")
			a.Get("parentNode").Call("removeChild", a)
		}

		// Cleanup the URL after a short delay
		js.Global().Get("setTimeout").Invoke(js.FuncOf(func(this js.Value, args []js.Value) any {
			js.Global().Get("URL").Call("revokeObjectURL", url)
			return nil
		}), 1000)

		return "ok"
	}))
	// ---------- Charts ----------

    // df.BarChart(title, subtitle, groupcol, aggs...) or df.BarChart(title, subtitle, groupcol, [aggs...])
    obj.Set("BarChart", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil { return "error: invalid handle" }
        if len(args) < 4 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString || args[2].Type() != js.TypeString {
            return "error: usage BarChart(title, subtitle, groupcol, aggs...)"
        }
        title, subtitle, groupcol := args[0].String(), args[1].String(), args[2].String()
        aggs, err := aggsFromJS(args[3:])
        if err != nil { return "error: " + err.Error() }

        ch := df.BarChart(title, subtitle, groupcol, aggs)
		chID := putChart(ch)
        chartSeq++
        divID := fmt.Sprintf("%s_%d", ch.Htmldivid, chartSeq)
        html := ch.Htmlpreid + divID + ch.Htmlpostid
        jsText := ch.Jspreid + divID + ch.Jspostid

        helper := js.Global().Get("Object").New()
		helper.Set("chartHandle", chID)
        helper.Set("HTML", js.FuncOf(func(this js.Value, a []js.Value) any { return html }))
        helper.Set("JS", js.FuncOf(func(this js.Value, a []js.Value) any { return jsText }))
        helper.Set("ElementID", js.FuncOf(func(this js.Value, a []js.Value) any {
            if len(a) < 1 || a[0].Type() != js.TypeString { return "error: ElementID(id)" }
            doc := js.Global().Get("document")
            el := doc.Call("getElementById", a[0].String())
            if !el.Truthy() { return "error: element not found" }
            el.Set("innerHTML", html)
            s := doc.Call("createElement", "script")
            s.Set("textContent", jsText)
            el.Call("appendChild", s)
            return "ok"
        }))
        return helper
    }))

    // df.ColumnChart(...)
    obj.Set("ColumnChart", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil { return "error: invalid handle" }
        if len(args) < 4 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString || args[2].Type() != js.TypeString {
            return "error: usage ColumnChart(title, subtitle, groupcol, aggs...)"
        }
        title, subtitle, groupcol := args[0].String(), args[1].String(), args[2].String()
        aggs, err := aggsFromJS(args[3:])
        if err != nil { return "error: " + err.Error() }

        ch := df.ColumnChart(title, subtitle, groupcol, aggs)
		chID := putChart(ch)
        chartSeq++
        divID := fmt.Sprintf("%s_%d", ch.Htmldivid, chartSeq)
        html := ch.Htmlpreid + divID + ch.Htmlpostid
        jsText := ch.Jspreid + divID + ch.Jspostid

        helper := js.Global().Get("Object").New()
		helper.Set("chartHandle", chID)
        helper.Set("HTML", js.FuncOf(func(this js.Value, a []js.Value) any { return html }))
        helper.Set("JS", js.FuncOf(func(this js.Value, a []js.Value) any { return jsText }))
        helper.Set("ElementID", js.FuncOf(func(this js.Value, a []js.Value) any {
            if len(a) < 1 || a[0].Type() != js.TypeString { return "error: ElementID(id)" }
            doc := js.Global().Get("document")
            el := doc.Call("getElementById", a[0].String())
            if !el.Truthy() { return "error: element not found" }
            el.Set("innerHTML", html)
            s := doc.Call("createElement", "script")
            s.Set("textContent", jsText)
            el.Call("appendChild", s)
            return "ok"
        }))
        return helper
    }))

    // df.StackedBarChart(...)
    obj.Set("StackedBarChart", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil { return "error: invalid handle" }
        if len(args) < 4 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString || args[2].Type() != js.TypeString {
            return "error: usage StackedBarChart(title, subtitle, groupcol, aggs...)"
        }
        title, subtitle, groupcol := args[0].String(), args[1].String(), args[2].String()
        aggs, err := aggsFromJS(args[3:])
        if err != nil { return "error: " + err.Error() }

        ch := df.StackedBarChart(title, subtitle, groupcol, aggs)
		chID := putChart(ch)
        chartSeq++
        divID := fmt.Sprintf("%s_%d", ch.Htmldivid, chartSeq)
        html := ch.Htmlpreid + divID + ch.Htmlpostid
        jsText := ch.Jspreid + divID + ch.Jspostid

        helper := js.Global().Get("Object").New()
		helper.Set("chartHandle", chID)
        helper.Set("HTML", js.FuncOf(func(this js.Value, a []js.Value) any { return html }))
        helper.Set("JS", js.FuncOf(func(this js.Value, a []js.Value) any { return jsText }))
        helper.Set("ElementID", js.FuncOf(func(this js.Value, a []js.Value) any {
            if len(a) < 1 || a[0].Type() != js.TypeString { return "error: ElementID(id)" }
            doc := js.Global().Get("document")
            el := doc.Call("getElementById", a[0].String())
            if !el.Truthy() { return "error: element not found" }
            el.Set("innerHTML", html)
            s := doc.Call("createElement", "script")
            s.Set("textContent", jsText)
            el.Call("appendChild", s)
            return "ok"
        }))
        return helper
    }))

    // df.StackedPercentChart(...)
    obj.Set("StackedPercentChart", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil { return "error: invalid handle" }
        if len(args) < 4 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString || args[2].Type() != js.TypeString {
            return "error: usage StackedPercentChart(title, subtitle, groupcol, aggs...)"
        }
        title, subtitle, groupcol := args[0].String(), args[1].String(), args[2].String()
        aggs, err := aggsFromJS(args[3:])
        if err != nil { return "error: " + err.Error() }

        ch := df.StackedPercentChart(title, subtitle, groupcol, aggs)
		chID := putChart(ch)
        chartSeq++
        divID := fmt.Sprintf("%s_%d", ch.Htmldivid, chartSeq)
        html := ch.Htmlpreid + divID + ch.Htmlpostid
        jsText := ch.Jspreid + divID + ch.Jspostid

        helper := js.Global().Get("Object").New()
		helper.Set("chartHandle", chID)
        helper.Set("HTML", js.FuncOf(func(this js.Value, a []js.Value) any { return html }))
        helper.Set("JS", js.FuncOf(func(this js.Value, a []js.Value) any { return jsText }))
        helper.Set("ElementID", js.FuncOf(func(this js.Value, a []js.Value) any {
            if len(a) < 1 || a[0].Type() != js.TypeString { return "error: ElementID(id)" }
            doc := js.Global().Get("document")
            el := doc.Call("getElementById", a[0].String())
            if !el.Truthy() { return "error: element not found" }
            el.Set("innerHTML", html)
            s := doc.Call("createElement", "script")
            s.Set("textContent", jsText)
            el.Call("appendChild", s)
            return "ok"
        }))
        return helper
    }))

    // df.GroupedSeriesJSON(x, y) -> string
    obj.Set("GroupedSeriesJSON", js.FuncOf(func(this js.Value, args []js.Value) any {
        df := get(id)
        if df == nil { return "error: invalid handle" }
        if len(args) < 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
            return "error: usage GroupedSeriesJSON(xColumn, yColumn)"
        }
        return df.GroupedSeriesJSON(args[0].String(), args[1].String())
    }))

    // Helpers for string-based charts (PieChart/LineChart/ScatterPlot/BubbleChart/AreaChart)
    makeSimpleChart := func(gen func(*g.DataFrame, string, string) string) js.Func {
        return js.FuncOf(func(this js.Value, args []js.Value) any {
            df := get(id)
            if df == nil { return "error: invalid handle" }
            if len(args) < 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
                return "error: usage ChartFunc(x, y) or appropriate args"
            }
            // Generate JS snippet (expects Highcharts.chart('container', ...))
            code := gen(df, args[0].String(), args[1].String())

            // Unique chart div id and patched code
            chartSeq++
            innerID := fmt.Sprintf("chart_%d", chartSeq)
            replaced := strings.ReplaceAll(code, "Highcharts.chart('container'", "Highcharts.chart('"+innerID+"'")

            helper := js.Global().Get("Object").New()
            helper.Set("ChartID", js.FuncOf(func(this js.Value, a []js.Value) any {
                if len(a) < 1 || a[0].Type() != js.TypeString { return "error: ChartID(id)" }
                custom := a[0].String()
                replacedCustom := strings.ReplaceAll(code, "Highcharts.chart('container'", "Highcharts.chart('"+custom+"'")
                obj := js.Global().Get("Object").New()
                obj.Set("ElementID", js.FuncOf(func(this js.Value, b []js.Value) any {
                    if len(b) < 1 || b[0].Type() != js.TypeString { return "error: ElementID(containerId)" }
                    doc := js.Global().Get("document")
                    el := doc.Call("getElementById", b[0].String())
                    if !el.Truthy() { return "error: element not found" }
                    // create target div
                    div := doc.Call("createElement", "div")
                    div.Call("setAttribute", "id", custom)
                    el.Call("appendChild", div)
                    // append script
                    s := doc.Call("createElement", "script")
                    s.Set("textContent", replacedCustom)
                    el.Call("appendChild", s)
                    return "ok"
                }))
                return obj
            }))
            helper.Set("ElementID", js.FuncOf(func(this js.Value, a []js.Value) any {
                if len(a) < 1 || a[0].Type() != js.TypeString { return "error: ElementID(containerId)" }
                doc := js.Global().Get("document")
                el := doc.Call("getElementById", a[0].String())
                if !el.Truthy() { return "error: element not found" }
                // create target div
                div := doc.Call("createElement", "div")
                div.Call("setAttribute", "id", innerID)
                el.Call("appendChild", div)
                // append script
                s := doc.Call("createElement", "script")
                s.Set("textContent", replaced)
                el.Call("appendChild", s)
                return "ok"
            }))
            helper.Set("JS", js.FuncOf(func(this js.Value, a []js.Value) any { return replaced }))
            helper.Set("ChartDivID", innerID)
            return helper
        })
    }

    // df.PieChart(name, value) -> helper (ElementID)
    obj.Set("PieChart", makeSimpleChart(func(df *g.DataFrame, x, y string) string { return df.PieChart(x, y) }))
    // df.LineChart(x, y) -> helper (ElementID)
    obj.Set("LineChart", makeSimpleChart(func(df *g.DataFrame, x, y string) string { return df.LineChart(x, y) }))
    // df.ScatterPlot(x, y) -> helper (ElementID)
    obj.Set("ScatterPlot", makeSimpleChart(func(df *g.DataFrame, x, y string) string { return df.ScatterPlot(x, y) }))
    // df.BubbleChart(x, y) -> helper (ElementID)
    obj.Set("BubbleChart", makeSimpleChart(func(df *g.DataFrame, x, y string) string { return df.BubbleChart(x, y) }))
    // df.AreaChart(x, y) -> helper (ElementID)
    obj.Set("AreaChart", makeSimpleChart(func(df *g.DataFrame, x, y string) string { return df.AreaChart(x, y) }))
	// ---------- Other ----------
	// df.free()
	obj.Set("free", js.FuncOf(func(this js.Value, args []js.Value) any {
		storeMu.Lock()
		delete(store, id)
		storeMu.Unlock()
		return "ok"
	}))

	// inside dfObject(id) before return:
	if finalReg.Truthy() {
		finalReg.Call("register", obj, js.ValueOf(id))
	}
	return obj
}

// ------------- SINKS ---------------
// toJSONString converts a JS value to JSON text.
// - string: returns as-is
// - object/array: JSON.stringify
// - Uint8Array/ArrayBuffer: read bytes and decode as UTF-8
func toJSONString(v js.Value) (string, string) {
	t := v.Type()
	if t == js.TypeString {
		return v.String(), ""
	}
	// Uint8Array
	if ua := js.Global().Get("Uint8Array"); ua.Truthy() && v.InstanceOf(ua) {
		b := make([]byte, v.Get("length").Int())
		js.CopyBytesToGo(b, v)
		return string(b), ""
	}
	// ArrayBuffer
	if ab := js.Global().Get("ArrayBuffer"); ab.Truthy() && v.InstanceOf(ab) {
		u8 := js.Global().Get("Uint8Array").New(v)
		b := make([]byte, u8.Get("length").Int())
		js.CopyBytesToGo(b, u8)
		return string(b), ""
	}
	// Object/Array -> stringify
	j := js.Global().Get("JSON")
	if j.Truthy() {
		return j.Call("stringify", v).String(), ""
	}
	return "", "error: JSON is not available"
}

// ------------- SOURCES ---------------



// toText converts a JS value to plain text.
// Accepts: string, Uint8Array, ArrayBuffer. Returns error for others.
func toText(v js.Value) (string, string) {
	t := v.Type()
	if t == js.TypeString {
		return v.String(), ""
	}
	// Uint8Array
	if ua := js.Global().Get("Uint8Array"); ua.Truthy() && v.InstanceOf(ua) {
		b := make([]byte, v.Get("length").Int())
		js.CopyBytesToGo(b, v)
		return string(b), ""
	}
	// ArrayBuffer
	if ab := js.Global().Get("ArrayBuffer"); ab.Truthy() && v.InstanceOf(ab) {
		u8 := js.Global().Get("Uint8Array").New(v)
		b := make([]byte, u8.Get("length").Int())
		js.CopyBytesToGo(b, u8)
		return string(b), ""
	}
	return "", "error: expected string, Uint8Array, or ArrayBuffer"
}

// ReadJSON takes a JS value (object/array/string/Uint8Array/ArrayBuffer|File|Blob),
// builds a Go DataFrame, and returns a JS DataFrame object with methods.
func readJSON(this js.Value, args []js.Value) any {
    if len(args) < 1 {
        return "error: usage ReadJSON(value|File|Blob)"
    }
    v := args[0]

    // New: support File/Blob (async)
    if isBlobOrFile(v) {
        return js.Global().Get("Promise").New(js.FuncOf(func(this js.Value, prArgs []js.Value) any {
            resolve, reject := prArgs[0], prArgs[1]
            promiseReadBlobText(v).Call("then",
                js.FuncOf(func(this js.Value, a []js.Value) any {
                    text := a[0].String()
                    df := g.ReadJSON(text)
                    id := put(df)
                    resolve.Invoke(dfObject(id))
                    return nil
                }),
                js.FuncOf(func(this js.Value, a []js.Value) any {
                    reject.Invoke(a[0])
                    return nil
                }),
            )
            return nil
        }))
    }

    // Existing sync paths
    jsonText, err := toJSONString(v)
    if err != "" {
        return err
    }
    df := g.ReadJSON(jsonText)
    id := put(df)
    return dfObject(id)
}

// ...existing code...

// ReadCSV takes CSV (string/Uint8Array/ArrayBuffer|File|Blob) and returns a DataFrame object.
func readCSV(this js.Value, args []js.Value) any {
    if len(args) < 1 {
        return "error: usage ReadCSV(text|Uint8Array|ArrayBuffer|File|Blob)"
    }
    v := args[0]

    // New: support File/Blob (async)
    if isBlobOrFile(v) {
        return js.Global().Get("Promise").New(js.FuncOf(func(this js.Value, prArgs []js.Value) any {
            resolve, reject := prArgs[0], prArgs[1]
            promiseReadBlobText(v).Call("then",
                js.FuncOf(func(this js.Value, a []js.Value) any {
                    text := a[0].String()
                    df := g.ReadCSV(text)
                    id := put(df)
                    resolve.Invoke(dfObject(id))
                    return nil
                }),
                js.FuncOf(func(this js.Value, a []js.Value) any {
                    reject.Invoke(a[0])
                    return nil
                }),
            )
            return nil
        }))
    }

    // Existing sync paths
    csvText, err := toText(v)
    if err != "" {
        return err
    }
    df := g.ReadCSV(csvText)
    id := put(df)
    return dfObject(id)
}

// jsObjToStringMap converts a plain JS object to map[string]string (values are coerced to String()).
func jsObjToStringMap(v js.Value) map[string]string {
	out := map[string]string{}
	if !v.Truthy() || v.Type() != js.TypeObject {
		return out
	}
	keys := js.Global().Get("Object").Call("keys", v)
	l := keys.Length()
	for i := 0; i < l; i++ {
		k := keys.Index(i).String()
		val := v.Get(k)
		if val.Truthy() && val.Type() != js.TypeUndefined && val.Type() != js.TypeNull {
			out[k] = val.String()
		}
	}
	return out
}

// GetAPI(url[, headersObj, queryObj]) -> DataFrame object
func getAPI(this js.Value, args []js.Value) any {
	if len(args) < 1 || args[0].Type() != js.TypeString {
		return "error: usage GetAPI(url[, headersObj, queryObj])"
	}
	url := args[0].String()
	var headers, query map[string]string
	if len(args) >= 2 && args[1].Type() == js.TypeObject {
		headers = jsObjToStringMap(args[1])
	}
	if len(args) >= 3 && args[2].Type() == js.TypeObject {
		query = jsObjToStringMap(args[2])
	}

	df, err := g.GetAPI(url, headers, query)
	if err != nil {
		return "error: " + err.Error()
	}
	id := put(df)
	return dfObject(id)
}

// ...in main(), expose JS constructors for convenience:
func aggCtor(op string) js.Func {
    return js.FuncOf(func(this js.Value, args []js.Value) any {
        if len(args) < 1 || args[0].Type() != js.TypeString {
            return "error: usage " + op + "(columnName)"
        }
        // Return a plain JS object descriptor; GroupBy will convert it.
        o := js.Global().Get("Object").New()
        o.Set("op", op)
        o.Set("col", args[0].String())
        return o
    })
}

// Helpers: detect Blob/File and read text via Promise
func isBlobOrFile(v js.Value) bool {
    if v.Type() != js.TypeObject || !v.Truthy() {
        return false
    }
    blob := js.Global().Get("Blob")
    if blob.Truthy() && v.InstanceOf(blob) { return true }
    file := js.Global().Get("File")
    if file.Truthy() && v.InstanceOf(file) { return true }
    return false
}

func promiseReadBlobText(v js.Value) js.Value {
    return js.Global().Get("Promise").New(js.FuncOf(func(this js.Value, args []js.Value) any {
        resolve, reject := args[0], args[1]
        // Modern: File/Blob.text() -> Promise<string>
        if m := v.Get("text"); m.Truthy() {
            p := v.Call("text")
            onOK := js.FuncOf(func(this js.Value, a []js.Value) any { resolve.Invoke(a[0]); return nil })
            onErr := js.FuncOf(func(this js.Value, a []js.Value) any { reject.Invoke(a[0]); return nil })
            p.Call("then", onOK, onErr)
            return nil
        }
		// Fallback: FileReader
		fr := js.Global().Get("FileReader").New()
		var onload js.Func
		var onerr js.Func
		onload = js.FuncOf(func(this js.Value, a []js.Value) any {
			resolve.Invoke(fr.Get("result"))
			onload.Release()
			onerr.Release()
			return nil
		})
		onerr = js.FuncOf(func(this js.Value, a []js.Value) any {
			reject.Invoke(fr.Get("error"))
			onload.Release()
			onerr.Release()
			return nil
		})
		fr.Set("onload", onload)
		fr.Set("onerror", onerr)
		fr.Call("readAsText", v)
		return nil
    }))
}

// ...existing code...

// ReadNDJSON(text|Uint8Array|ArrayBuffer|File|Blob) -> DataFrame object or Promise<DataFrame>
func readNDJSON(this js.Value, args []js.Value) any {
    if len(args) < 1 {
        return "error: usage ReadNDJSON(value|File|Blob)"
    }
    v := args[0]
    // If File/Blob: return a Promise that resolves with a DataFrame object
    if isBlobOrFile(v) {
        return js.Global().Get("Promise").New(js.FuncOf(func(this js.Value, prArgs []js.Value) any {
            resolve, reject := prArgs[0], prArgs[1]
            promiseReadBlobText(v).Call("then",
                js.FuncOf(func(this js.Value, a []js.Value) any {
                    text := a[0].String()
                    df := g.ReadNDJSON(text)
                    id := put(df)
                    resolve.Invoke(dfObject(id))
                    return nil
                }),
                js.FuncOf(func(this js.Value, a []js.Value) any {
                    reject.Invoke(a[0])
                    return nil
                }),
            )
            return nil
        }))
    }
    // Synchronous inputs (string/Uint8Array/ArrayBuffer)
    text, err := toText(v)
    if err != "" {
        return err
    }
    df := g.ReadNDJSON(text)
    id := put(df)
    return dfObject(id)
}

// ReadYAML(text|Uint8Array|ArrayBuffer|File|Blob) -> DataFrame object or Promise<DataFrame>
func readYAML(this js.Value, args []js.Value) any {
    if len(args) < 1 {
        return "error: usage ReadYAML(value|File|Blob)"
    }
    v := args[0]
    if isBlobOrFile(v) {
        return js.Global().Get("Promise").New(js.FuncOf(func(this js.Value, prArgs []js.Value) any {
            resolve, reject := prArgs[0], prArgs[1]
            promiseReadBlobText(v).Call("then",
                js.FuncOf(func(this js.Value, a []js.Value) any {
                    text := a[0].String()
                    df := g.ReadYAML(text)
                    id := put(df)
                    resolve.Invoke(dfObject(id))
                    return nil
                }),
                js.FuncOf(func(this js.Value, a []js.Value) any {
                    reject.Invoke(a[0])
                    return nil
                }),
            )
            return nil
        }))
    }
    // Synchronous path
    text, err := toText(v)
    if err != "" {
        return err
    }
    df := g.ReadYAML(text)
    id := put(df)
    return dfObject(id)
}

// reportObject builds the JS wrapper for a Report handle.
func reportObject(id int) js.Value {
    r := js.Global().Get("Object").New()
    r.Set("handle", id)

    // r.AddPage(name)
    r.Set("AddPage", js.FuncOf(func(this js.Value, args []js.Value) any {
        rep := getReport(id)
        if rep == nil { return "error: invalid handle" }
        if len(args) < 1 || args[0].Type() != js.TypeString {
            return "error: usage AddPage(name)"
        }
        rep.AddPage(args[0].String())
        return reportObject(id)
    }))

    // r.AddHTML(page, html)
    r.Set("AddHTML", js.FuncOf(func(this js.Value, args []js.Value) any {
        rep := getReport(id)
        if rep == nil { return "error: invalid handle" }
        if len(args) < 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
            return "error: usage AddHTML(page, html)"
        }
        rep.AddHTML(args[0].String(), args[1].String())
        return reportObject(id)
    }))

    // r.AddDataframe(page, df)
    r.Set("AddDataframe", js.FuncOf(func(this js.Value, args []js.Value) any {
        rep := getReport(id)
        if rep == nil { return "error: invalid handle" }
        if len(args) < 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeObject {
            return "error: usage AddDataframe(page, df)"
        }
        dfh := args[1].Get("handle")
        if !dfh.Truthy() { return "error: df missing handle" }
        df := get(dfh.Int())
        if df == nil { return "error: invalid df handle" }
        rep.AddDataframe(args[0].String(), df)
        return reportObject(id)
    }))

    // r.AddChart(page, chartHelperOrObj) – accepts a helper returned by df.BarChart/... (has chartHandle)
    r.Set("AddChart", js.FuncOf(func(this js.Value, args []js.Value) any {
        rep := getReport(id)
        if rep == nil { return "error: invalid handle" }
        if len(args) < 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeObject {
            return "error: usage AddChart(page, chart)"
        }
        chVal := args[1]
        // Prefer handle
        if chVal.Get("chartHandle").Truthy() {
            chID := chVal.Get("chartHandle").Int()
            if ch, ok := getChart(chID); ok {
                rep.AddChart(args[0].String(), ch)
                return reportObject(id)
            }
            return "error: invalid chart handle"
        }
        // Or accept plain object with fields matching Chart
        ch := g.Chart{
            Htmlpreid:  chVal.Get("Htmlpreid").String(),
            Htmldivid:  chVal.Get("Htmldivid").String(),
            Htmlpostid: chVal.Get("Htmlpostid").String(),
            Jspreid:    chVal.Get("Jspreid").String(),
            Jspostid:   chVal.Get("Jspostid").String(),
        }
        rep.AddChart(args[0].String(), ch)
        return reportObject(id)
    }))

    // r.AddHeading(page, text, size)
    r.Set("AddHeading", js.FuncOf(func(this js.Value, args []js.Value) any {
        rep := getReport(id)
        if rep == nil { return "error: invalid handle" }
        if len(args) < 3 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString || args[2].Type() != js.TypeNumber {
            return "error: usage AddHeading(page, text, size)"
        }
        rep.AddHeading(args[0].String(), args[1].String(), args[2].Int())
        return reportObject(id)
    }))

    // r.AddText(page, text)
    r.Set("AddText", js.FuncOf(func(this js.Value, args []js.Value) any {
        rep := getReport(id)
        if rep == nil { return "error: invalid handle" }
        if len(args) < 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
            return "error: usage AddText(page, text)"
        }
        rep.AddText(args[0].String(), args[1].String())
        return reportObject(id)
    }))

    // r.AddSubText(page, text)
    r.Set("AddSubText", js.FuncOf(func(this js.Value, args []js.Value) any {
        rep := getReport(id)
        if rep == nil { return "error: invalid handle" }
        if len(args) < 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
            return "error: usage AddSubText(page, text)"
        }
        rep.AddSubText(args[0].String(), args[1].String())
        return reportObject(id)
    }))

    // r.AddBullets(page, ...items) or r.AddBullets(page, ['a','b'])
    r.Set("AddBullets", js.FuncOf(func(this js.Value, args []js.Value) any {
        rep := getReport(id)
        if rep == nil { return "error: invalid handle" }
        if len(args) < 2 || args[0].Type() != js.TypeString {
            return "error: usage AddBullets(page, ...items)"
        }
        page := args[0].String()
        items := []string{}
        if arr := js.Global().Get("Array"); arr.Truthy() && args[1].InstanceOf(arr) {
            a := args[1]
            for i := 0; i < a.Length(); i++ {
                if v := a.Index(i); v.Type() == js.TypeString {
                    items = append(items, v.String())
                }
            }
        } else {
            for _, v := range args[1:] {
                if v.Type() == js.TypeString {
                    items = append(items, v.String())
                }
            }
        }
        rep.AddBullets(page, items...)
        return reportObject(id)
    }))

    // r.Save(filename) -> downloads file
    r.Set("Save", js.FuncOf(func(this js.Value, args []js.Value) any {
        rep := getReport(id)
        if rep == nil { return "error: invalid handle" }
        filename := "report.html"
        if len(args) >= 1 && args[0].Type() == js.TypeString && args[0].String() != "" {
            filename = args[0].String()
        }
        html := buildReportHTML(rep)
        array := js.Global().Get("Array").New()
        array.Call("push", js.ValueOf(html))
        opts := js.Global().Get("Object").New()
        opts.Set("type", "text/html;charset=utf-8")
        blob := js.Global().Get("Blob").New(array, opts)
        url := js.Global().Get("URL").Call("createObjectURL", blob)
        doc := js.Global().Get("document")
        a := doc.Call("createElement", "a")
        a.Set("href", url)
        a.Set("download", filename)
        a.Set("rel", "noopener")
        doc.Get("body").Call("appendChild", a)
        a.Call("click")
        a.Get("parentNode").Call("removeChild", a)
        js.Global().Get("setTimeout").Invoke(js.FuncOf(func(this js.Value, _ []js.Value) any {
            js.Global().Get("URL").Call("revokeObjectURL", url)
            return nil
        }), 1000)
        return "ok"
    }))

    // r.Open() -> opens in a new tab
    r.Set("Open", js.FuncOf(func(this js.Value, args []js.Value) any {
        rep := getReport(id)
        if rep == nil { return "error: invalid handle" }
        html := buildReportHTML(rep)
        array := js.Global().Get("Array").New()
        array.Call("push", js.ValueOf(html))
        opts := js.Global().Get("Object").New()
        opts.Set("type", "text/html;charset=utf-8")
        blob := js.Global().Get("Blob").New(array, opts)
        url := js.Global().Get("URL").Call("createObjectURL", blob)
        win := js.Global().Get("window").Call("open", url, "_blank")
        if !win.Truthy() {
            // fallback
            doc := js.Global().Get("document")
            a := doc.Call("createElement", "a")
            a.Set("href", url)
            a.Set("target", "_blank")
            a.Set("rel", "noopener")
            doc.Get("body").Call("appendChild", a)
            a.Call("click")
            a.Get("parentNode").Call("removeChild", a)
        }
        js.Global().Get("setTimeout").Invoke(js.FuncOf(func(this js.Value, _ []js.Value) any {
            js.Global().Get("URL").Call("revokeObjectURL", url)
            return nil
        }), 1500)
        return "ok"
    }))

    return r
}

// buildReportHTML reproduces reports.go Open/Save assembly without touching the filesystem.
func buildReportHTML(report *g.Report) string {
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
            ` + fmt.Sprintf(`<div class="flex-1 px-2 mx-2 btn btn-sm btn-neutral normal-case text-xl shadow-none hover:bg-neutral hover:border-neutral flex content-center"><a class="lg:ml-0 ml-14 text-4xl">%s</a></div>`, report.Title) + `
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
        ` + fmt.Sprintf(`<div class="flex-1 px-2 mx-2 btn btn-sm btn-neutral normal-case text-xl shadow-none hover:bg-neutral hover:border-neutral flex content-center"><a class=" text-4xl">%s</a></div>
            </div>`, report.Title) + `<div  class="w-full lg:w-3/4 md:w-3/4 sm:w-5/6 mx-auto flex-col justify-self-center">`
    }
    // pages html
    for _, pageMap := range report.Pageshtml {
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
        for page := range report.Pageshtml {
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
    for page := range report.Pageshtml {
        if count == 0 {
            html += fmt.Sprintf("%q", page) + ","
        }
        pages += fmt.Sprintf("%q", page) + ", "
        count++
    }
    pages = strings.TrimSuffix(pages, ", ") + `],`
    html += pages
    html += report.Scriptmiddle
    for _, jsMap := range report.Pagesjs {
        for i := 0; i < len(jsMap); i++ {
            html += jsMap[strconv.Itoa(i)]
        }
    }
    html += report.Bottom
    return html
}

func main() {
	root := js.Global()
	if root.Get("gophers").IsUndefined() {
		root.Set("gophers", js.ValueOf(map[string]any{}))
	}
	api := root.Get("gophers")
	// ---------- Sources ----------
	api.Set("ReadJSON", js.FuncOf(readJSON)) // now returns a DataFrame object with methods
	api.Set("ReadCSV", js.FuncOf(readCSV))
    api.Set("ReadNDJSON", js.FuncOf(readNDJSON))
    api.Set("ReadYAML", js.FuncOf(readYAML))
	api.Set("GetAPI", js.FuncOf(getAPI))
    // CloneJSON(dfJsonLike) -> string (JSON of cloned DataFrame)
    // Accepts a string, object, array, Uint8Array, or ArrayBuffer.
    api.Set("CloneJSON", js.FuncOf(func(this js.Value, args []js.Value) any {
        if len(args) < 1 {
            return "error: usage CloneJSON(dataframeJSON)"
        }
        text, err := toJSONString(args[0])
        if err != "" {
            return err
        }
        return g.CloneJSON(text)
    }))

	// ---------- Aggregations ----------
    api.Set("Sum", aggCtor("sum"))
    api.Set("Max", aggCtor("max"))
    api.Set("Min", aggCtor("min"))
    api.Set("Median", aggCtor("median"))
    api.Set("Mean", aggCtor("mean"))
    api.Set("Mode", aggCtor("mode"))
    api.Set("Unique", aggCtor("unique"))
    api.Set("First", aggCtor("first"))
	api.Set("Agg", js.FuncOf(func(this js.Value, args []js.Value) any {
		// Usage: gophers.Agg(gophers.Sum("col2"), gophers.First("col3"))
		// Each arg should be an object produced by Sum/Max/... or a string shorthand "sum:col"
		arrCtor := js.Global().Get("Array")
		out := arrCtor.New()
		for _, a := range args {
			switch a.Type() {
			case js.TypeObject:
				out.Call("push", a)
			case js.TypeString:
				// Accept "op:col" or "op(col)" shorthand
				s := strings.TrimSpace(a.String())
				spec := js.Global().Get("Object").New()
				if i := strings.IndexByte(s, ':'); i > 0 {
					spec.Set("op", strings.ToLower(strings.TrimSpace(s[:i])))
					spec.Set("col", strings.TrimSpace(s[i+1:]))
					out.Call("push", spec)
					continue
				}
				if strings.HasSuffix(s, ")") && strings.Contains(s, "(") {
					i := strings.IndexByte(s, '(')
					spec.Set("op", strings.ToLower(strings.TrimSpace(s[:i])))
					spec.Set("col", strings.TrimSuffix(strings.TrimSpace(s[i+1:]), ")"))
					out.Call("push", spec)
					continue
				}
				// Fallback: ignore malformed string
			default:
				// Ignore unsupported types
			}
		}
		return out
	}))
	// ---------- Logic ----------

    // ---- Expression builder helpers (mirror pure Go syntax) ----
    // internal helper: wrap JS value into a ColumnExpr object
    toExpr := js.FuncOf(func(this js.Value, args []js.Value) any {
        if len(args) == 0 {
            return js.Global().Get("Object").New()
        }
        v := args[0]
        // If already an expr object (has Type), return as-is
        if v.Type() == js.TypeObject && v.Get("Type").Type() == js.TypeString {
            return v
        }
        switch v.Type() {
        case js.TypeString:
            o := js.Global().Get("Object").New()
            o.Set("Type", "col")
            o.Set("Name", v.String())
            return o
        case js.TypeNumber, js.TypeBoolean:
            o := js.Global().Get("Object").New()
            o.Set("Type", "lit")
            o.Set("Value", jsValToAny(v))
            return o
        default:
            if v.Type() == js.TypeObject {
                // treat plain object as literal value
                o := js.Global().Get("Object").New()
                o.Set("Type", "lit")
                o.Set("Value", v)
                return o
            }
            o := js.Global().Get("Object").New()
            o.Set("Type", "lit")
            o.Set("Value", jsValToAny(v))
            return o
        }
    })

    api.Set("Col", js.FuncOf(func(this js.Value, args []js.Value) any {
        if len(args) < 1 || args[0].Type() != js.TypeString {
            return "error: Col(name)"
        }
        return toExpr.Invoke(args[0])
    }))

    api.Set("Lit", js.FuncOf(func(this js.Value, args []js.Value) any {
        if len(args) < 1 {
            return "error: Lit(value)"
        }
        o := js.Global().Get("Object").New()
        o.Set("Type", "lit")
        o.Set("Value", jsValToAny(args[0]))
        return o
    }))

    makeBinary := func(op string) js.Func {
        return js.FuncOf(func(this js.Value, args []js.Value) any {
            if len(args) < 2 {
                return "error: " + op + "(left, right)"
            }
            left := toExpr.Invoke(args[0])
            right := toExpr.Invoke(args[1])
            o := js.Global().Get("Object").New()
            o.Set("Type", op)
            o.Set("Left", left)
            o.Set("Right", right)
            return o
        })
    }

    api.Set("Gt", makeBinary("gt"))
    api.Set("Ge", makeBinary("ge"))
    api.Set("Lt", makeBinary("lt"))
    api.Set("Le", makeBinary("le"))
    api.Set("Eq", makeBinary("eq"))
    api.Set("Ne", makeBinary("ne"))
    api.Set("And", makeBinary("and"))
    api.Set("Or", makeBinary("or"))

    api.Set("IsNull", js.FuncOf(func(this js.Value, args []js.Value) any {
        if len(args) < 1 {
            return "error: IsNull(expr)"
        }
        ex := toExpr.Invoke(args[0])
        o := js.Global().Get("Object").New()
        o.Set("Type", "isnull")
        o.Set("Expr", ex)
        return o
    }))

    api.Set("IsNotNull", js.FuncOf(func(this js.Value, args []js.Value) any {
        if len(args) < 1 {
            return "error: IsNotNull(expr)"
        }
        ex := toExpr.Invoke(args[0])
        o := js.Global().Get("Object").New()
        o.Set("Type", "isnotnull")
        o.Set("Expr", ex)
        return o
    }))

    api.Set("If", js.FuncOf(func(this js.Value, args []js.Value) any {
        if len(args) < 3 {
            return "error: If(cond, thenExpr, elseExpr)"
        }
        cond := toExpr.Invoke(args[0])
        thenE := toExpr.Invoke(args[1])
        elseE := toExpr.Invoke(args[2])
        o := js.Global().Get("Object").New()
        o.Set("Type", "if")
        o.Set("Cond", cond)
        o.Set("True", thenE)
        o.Set("False", elseE)
        return o
    }))

	// --------- Functions ---------

    // ---------- Functions (builders that mirror functions.go) ----------
    // Reuse toExpr helper defined above to wrap strings/numbers/objects into an expr.

    // SHA256(expr1, expr2, ...)
    api.Set("SHA256", js.FuncOf(func(this js.Value, args []js.Value) any {
        arr := js.Global().Get("Array").New()
        for _, a := range args {
            arr.Call("push", toExpr.Invoke(a))
        }
        o := js.Global().Get("Object").New()
        o.Set("Type", "sha256")
        o.Set("Cols", arr)
        return o
    }))

    // SHA512(expr1, expr2, ...)
    api.Set("SHA512", js.FuncOf(func(this js.Value, args []js.Value) any {
        arr := js.Global().Get("Array").New()
        for _, a := range args {
            arr.Call("push", toExpr.Invoke(a))
        }
        o := js.Global().Get("Object").New()
        o.Set("Type", "sha512")
        o.Set("Cols", arr)
        return o
    }))

    // Split(name, delimiter) – name should be a column string (matches pure Go Split)
    api.Set("Split", js.FuncOf(func(this js.Value, args []js.Value) any {
        if len(args) < 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
            return "error: Split(columnName, delimiter)"
        }
        o := js.Global().Get("Object").New()
        o.Set("Type", "split")
        o.Set("Col", args[0].String())
        o.Set("Delimiter", args[1].String())
        return o
    }))

    // Keys(name) – name should be a column string
    api.Set("Keys", js.FuncOf(func(this js.Value, args []js.Value) any {
        if len(args) < 1 || args[0].Type() != js.TypeString {
            return "error: Keys(columnName)"
        }
        o := js.Global().Get("Object").New()
        o.Set("Type", "keys")
        o.Set("Col", args[0].String())
        return o
    }))

    // Lookup(keyExpr, nestCol) – keyExpr can be string/expr; nestCol is a column string
    api.Set("Lookup", js.FuncOf(func(this js.Value, args []js.Value) any {
        if len(args) < 2 || args[1].Type() != js.TypeString {
            return "error: Lookup(keyExpr, nestedColumnName)"
        }
        key := toExpr.Invoke(args[0])
        nest := toExpr.Invoke(args[1]) // will become a {"Type":"col","Name": "..."}
        o := js.Global().Get("Object").New()
        o.Set("Type", "lookup")
        o.Set("Left", key)
        o.Set("Right", nest)
        return o
    }))

    // Concat(delimiter, ...exprs)
    api.Set("Concat", js.FuncOf(func(this js.Value, args []js.Value) any {
        if len(args) < 2 || args[0].Type() != js.TypeString {
            return "error: Concat(delimiter, ...exprs)"
        }
        delim := args[0].String()
        arr := js.Global().Get("Array").New()
        for _, a := range args[1:] {
            arr.Call("push", toExpr.Invoke(a))
        }
        o := js.Global().Get("Object").New()
        o.Set("Type", "concat")
        o.Set("Delimiter", delim)
        o.Set("Cols", arr)
        return o
    }))

    // Cast(expr, datatype) – datatype in {"int","float","string"}
	api.Set("Cast", js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) < 2 || args[1].Type() != js.TypeString {
			return "error: Cast(expr, datatype)"
		}
		dtype := args[1].String()
		sub := args[0]

		// Wrap sub into expr object (always) then stringify into Col
		subExpr := toExpr.Invoke(sub)
		jsonStr := js.Global().Get("JSON").Call("stringify", subExpr).String()

		o := js.Global().Get("Object").New()
		o.Set("Type", "cast")
		o.Set("Col", jsonStr)      // Evaluate expects JSON here
		o.Set("Datatype", dtype)
		return o
	}))
	// CollectList(name) – name should be a column string
    api.Set("CollectList", js.FuncOf(func(this js.Value, args []js.Value) any {
        if len(args) < 1 || args[0].Type() != js.TypeString {
            return "error: CollectList(columnName)"
        }
        o := js.Global().Get("Object").New()
        o.Set("Type", "collectlist")
        o.Set("Col", args[0].String())
        return o
    }))

    // CollectSet(name) – name should be a column string
    api.Set("CollectSet", js.FuncOf(func(this js.Value, args []js.Value) any {
        if len(args) < 1 || args[0].Type() != js.TypeString {
            return "error: CollectSet(columnName)"
        }
        o := js.Global().Get("Object").New()
        o.Set("Type", "collectset")
        o.Set("Col", args[0].String())
        return o
    }))
	// --------- Display ---------

	// DisplayHTML(html) -> returns helper with .ElementID(id) to mount HTML
	api.Set("DisplayHTML", js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) < 1 || args[0].Type() != js.TypeString {
			return "error: usage DisplayHTML(htmlString)"
		}
		html := args[0].String()

		o := js.Global().Get("Object").New()
		o.Set("html", html)

		o.Set("ElementID", js.FuncOf(func(this js.Value, a []js.Value) any {
			if len(a) < 1 || a[0].Type() != js.TypeString {
				return "error: ElementID(id)"
			}
			doc := js.Global().Get("document")
			if !doc.Truthy() {
				return "error: document is not available"
			}
			el := doc.Call("getElementById", a[0].String())
			if !el.Truthy() {
				return "error: element not found"
			}

			if strings.Contains(html, "<!DOCTYPE") || strings.Contains(html, "<html") {
				el.Set("innerHTML", "")
				iframe := doc.Call("createElement", "iframe")
				iframe.Get("style").Set("width", "100%")
				iframe.Get("style").Set("height", "100%")
				iframe.Set("frameBorder", "0")
				// Add sandbox attributes
				iframe.Call("setAttribute", "sandbox", "allow-scripts allow-popups allow-downloads allow-top-navigation-by-user-activation")

				if !iframe.Get("srcdoc").IsUndefined() {
					iframe.Set("srcdoc", html)
					el.Call("appendChild", iframe)
					return o
				}
				parts := js.Global().Get("Array").New()
				parts.Call("push", js.ValueOf(html))
				opts := js.Global().Get("Object").New()
				opts.Set("type", "text/html;charset=utf-8")
				blob := js.Global().Get("Blob").New(parts, opts)
				url := js.Global().Get("URL").Call("createObjectURL", blob)

				var onload js.Func
				onload = js.FuncOf(func(this js.Value, _ []js.Value) any {
					js.Global().Get("URL").Call("revokeObjectURL", url)
					onload.Release()
					return nil
				})
				iframe.Set("onload", onload)
				iframe.Set("src", url)

				el.Call("appendChild", iframe)
				return o
			}

			el.Set("innerHTML", html)
			return o
		}))

		return o
	}))	
	// ---------- Reports ----------

	api.Set("CreateReport", js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) < 1 || args[0].Type() != js.TypeString {
			return "error: usage CreateReport(title)"
		}
		r := g.CreateReport(args[0].String())
		id := putReport(r)
		return reportObject(id)
	}))
	// --------- Other ---------
	api.Set("Free", js.FuncOf(free)) // legacy handle-based
	if fr := js.Global().Get("FinalizationRegistry"); fr.Truthy() {
		finalCb = js.FuncOf(func(this js.Value, args []js.Value) any {
			id := args[0].Int()
			storeMu.Lock()
			delete(store, id)
			storeMu.Unlock()
			return nil
		})
		finalReg = fr.New(finalCb)
	}
	select {}
}
