//go:build js && wasm

package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sync"
	"syscall/js"

	g "github.com/SPEARTECH/gophers/go/gophers"
)

var (
	storeMu sync.Mutex
	nextID  = 1
	store   = map[int]*g.DataFrame{}
)

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

// Helper: build a JS DataFrame object with methods bound to an internal handle.
func dfObject(id int) js.Value {
	obj := js.Global().Get("Object").New()
	obj.Set("handle", id)

	// df.toJSON(format?) -> rows array (default) or columnar object
	obj.Set("toJSON", js.FuncOf(func(this js.Value, args []js.Value) any {
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
	// df.toJSONFile(filename?, format?, pretty?)
	// - filename: string (default "dataframe.json")
	// - format: "rows" | "columnar" (default "rows")
	// - pretty: boolean (default false)
	obj.Set("toJSONFile", js.FuncOf(func(this js.Value, args []js.Value) any {
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
	obj.Set("toCSVFile", js.FuncOf(func(this js.Value, args []js.Value) any {
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
	// df.free()
	obj.Set("free", js.FuncOf(func(this js.Value, args []js.Value) any {
		storeMu.Lock()
		delete(store, id)
		storeMu.Unlock()
		return "ok"
	}))

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

// ReadJSON takes a JS value (object/array/string/Uint8Array/ArrayBuffer),
// builds a Go DataFrame, and returns a JS DataFrame object with methods.
func readJSON(this js.Value, args []js.Value) any {
	if len(args) < 1 {
		return "error: usage ReadJSON(value)"
	}
	jsonText, err := toJSONString(args[0])
	if err != "" {
		return err
	}
	df := g.ReadJSON(jsonText)
	id := put(df)
	return dfObject(id)
}

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

// ReadCSV takes CSV text (string/Uint8Array/ArrayBuffer) and returns a DataFrame object.
func readCSV(this js.Value, args []js.Value) any {
	if len(args) < 1 {
		return "error: usage ReadCSV(text|Uint8Array|ArrayBuffer)"
	}
	csvText, err := toText(args[0])
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

func main() {
	root := js.Global()
	if root.Get("gophers").IsUndefined() {
		root.Set("gophers", js.ValueOf(map[string]any{}))
	}
	api := root.Get("gophers")
	api.Set("ReadJSON", js.FuncOf(readJSON)) // now returns a DataFrame object with methods
	api.Set("ReadCSV", js.FuncOf(readCSV))
	api.Set("GetAPI", js.FuncOf(getAPI))
	api.Set("Free", js.FuncOf(free)) // legacy handle-based
	select {}
}
