package gophers

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
    "bytes"
    "hash/fnv"
)

// // referencedCols returns the set of column names used by a ColumnExpr.
// func referencedCols(e ColumnExpr, acc map[string]struct{}) map[string]struct{} {
//     if acc == nil { acc = make(map[string]struct{}) }
//     switch strings.ToLower(e.Type) {
//     case "col":
//         if e.Name != "" { acc[e.Name] = struct{}{} }
//         if e.Col != "" { acc[e.Col] = struct{}{} }
//     default:
//         var x ColumnExpr
//         if len(e.Expr) > 0 { _ = json.Unmarshal(e.Expr, &x); referencedCols(x, acc) }
//         if len(e.Left) > 0 { _ = json.Unmarshal(e.Left, &x); referencedCols(x, acc) }
//         if len(e.Right) > 0 { _ = json.Unmarshal(e.Right, &x); referencedCols(x, acc) }
//         if len(e.Cond) > 0 { _ = json.Unmarshal(e.Cond, &x); referencedCols(x, acc) }
//         if len(e.True) > 0 { _ = json.Unmarshal(e.True, &x); referencedCols(x, acc) }
//         if len(e.False) > 0 { _ = json.Unmarshal(e.False, &x); referencedCols(x, acc) }
//         if len(e.Cols) > 0 {
//             var xs []ColumnExpr; _ = json.Unmarshal(e.Cols, &xs)
//             for _, y := range xs { referencedCols(y, acc) }
//         }
//         if e.Col != "" { acc[e.Col] = struct{}{} }
//     }
//     return acc
// }
func referencedCols(e ColumnExpr, acc map[string]struct{}) map[string]struct{} {
    if acc == nil { acc = make(map[string]struct{}) }
    switch strings.ToLower(e.Type) {
    case "col":
        if e.Name != "" { acc[e.Name] = struct{}{} }
        if e.Col != "" { acc[e.Col] = struct{}{} }
    case "cast":
        // e.Col should be JSON of a sub expression; if not valid JSON, treat as plain column name.
        if e.Col != "" {
            if json.Valid([]byte(e.Col)) {
                var sub ColumnExpr
                if err := json.Unmarshal([]byte(e.Col), &sub); err == nil {
                    referencedCols(sub, acc)
                }
            } else {
                acc[e.Col] = struct{}{}
            }
        }
    default:
        var x ColumnExpr
        if len(e.Expr) > 0 { _ = json.Unmarshal(e.Expr, &x); referencedCols(x, acc) }
        if len(e.Left) > 0 { _ = json.Unmarshal(e.Left, &x); referencedCols(x, acc) }
        if len(e.Right) > 0 { _ = json.Unmarshal(e.Right, &x); referencedCols(x, acc) }
        if len(e.Cond) > 0 { _ = json.Unmarshal(e.Cond, &x); referencedCols(x, acc) }
        if len(e.True) > 0 { _ = json.Unmarshal(e.True, &x); referencedCols(x, acc) }
        if len(e.False) > 0 { _ = json.Unmarshal(e.False, &x); referencedCols(x, acc) }
        if len(e.Cols) > 0 {
            var xs []ColumnExpr
            _ = json.Unmarshal(e.Cols, &xs)
            for _, y := range xs { referencedCols(y, acc) }
        }
        // Fallback plain Col (non-cast types that carry a direct column name)
        if e.Col != "" && !json.Valid([]byte(e.Col)) {
            acc[e.Col] = struct{}{}
        }
    }
    return acc
}

// add: "hash/fnv"; "bytes"
func rowKeyFast(cols []string, data map[string][]interface{}, i int) string {
    h := fnv.New64a()
    var b bytes.Buffer
    for _, c := range cols {
        b.Reset()
        b.WriteString(c); b.WriteByte('=')
        v := data[c][i]
        switch t := v.(type) {
        case nil: b.WriteString("∅")
        case string: b.WriteString(t); b.WriteByte('|')
        case int: b.WriteString(strconv.Itoa(t)); b.WriteByte('|')
        case int64: b.WriteString(strconv.FormatInt(t,10)); b.WriteByte('|')
        case float64: b.WriteString(strconv.FormatFloat(t,'g',-1,64)); b.WriteByte('|')
        case bool: if t { b.WriteByte('1') } else { b.WriteByte('0') }
        default: fmt.Fprintf(&b, "%v|", t)
        }
        h.Write(b.Bytes())
    }
    return strconv.FormatUint(h.Sum64(), 36)
}

// // Column adds or modifies a column in the DataFrame using a ColumnExpr.
// // Compiles the expr once, then evaluates its closure per row.
// func (df *DataFrame) Column(column string, colSpec ColumnExpr) *DataFrame {
//     values := make([]interface{}, df.Rows)
//     if df.Rows == 0 {
//         df.Data[column] = values
//         exists := false
//         for _, c := range df.Cols {
//             if c == column {
//                 exists = true
//                 break
//             }
//         }
//         if !exists {
//             df.Cols = append(df.Cols, column)
//         }
//         return df
//     }

//     // Compile once (fast path vs per-row Evaluate)
//     compiled := Compile(colSpec)

//     // Only materialize referenced columns per row
//     refSet := referencedCols(colSpec, nil)
//     refCols := make([]string, 0, len(refSet))
//     for c := range refSet { refCols = append(refCols, c) }
//     refSlices := make(map[string][]interface{}, len(refCols))
//     for _, c := range refCols { refSlices[c] = df.Data[c] }

//     w := runtime.GOMAXPROCS(0)
//     var wg sync.WaitGroup
//     chunk := (df.Rows + w - 1) / w
//     for g := 0; g < w; g++ {
//         // ...existing code...
//         go func(s, e int) {
//             defer wg.Done()
//             row := make(map[string]interface{}, len(refCols))
//             for i := s; i < e; i++ {
//                 for _, c := range refCols {
//                     row[c] = refSlices[c][i]
//                 }
//                 values[i] = compiled.Fn(row)
//             }
//         }(start, end)
//     }    
//     wg.Wait()

//     df.Data[column] = values
//     exists := false
//     for _, c := range df.Cols {
//         if c == column {
//             exists = true
//             break
//         }
//     }
//     if !exists {
//         df.Cols = append(df.Cols, column)
//     }
//     return df
// }

// Column adds or modifies a column. Accepts either:
//   - ColumnExpr (will Compile to Column)
//   - Column (already compiled)
// It keeps concurrency & referenced column optimization for ColumnExpr.
func (df *DataFrame) Column(column string, spec interface{}) *DataFrame {
    if df == nil {
        return df
    }
    if df.Data == nil {
        df.Data = make(map[string][]interface{})
    }
    values := make([]interface{}, df.Rows)
    if df.Rows == 0 {
        df.Data[column] = values
        found := false
        for _, c := range df.Cols {
            if c == column { found = true; break }
        }
        if !found { df.Cols = append(df.Cols, column) }
        return df
    }

    var compiled Column
    var refCols []string

    switch v := spec.(type) {
    case ColumnExpr:
        compiled = Compile(v)
        refSet := referencedCols(v, nil)
        refCols = make([]string, 0, len(refSet))
        for c := range refSet { refCols = append(refCols, c) }

        // Pad referenced slices
        for _, c := range refCols {
            s := df.Data[c]
            if s == nil {
                s = make([]interface{}, df.Rows)
                df.Data[c] = s
            } else if len(s) < df.Rows {
                p := make([]interface{}, df.Rows)
                copy(p, s)
                df.Data[c] = p
            }
        }
    case Column:
        // Already compiled closure
        compiled = v
        // Fallback: no referenced column discovery; build full row maps.
        refCols = append(refCols, df.Cols...)
    default:
        fmt.Printf("Column error: unsupported spec type %T\n", v)
        return df
    }

    w := runtime.GOMAXPROCS(0)
    if w < 1 { w = 1 }
    chunk := (df.Rows + w - 1) / w
    var wg sync.WaitGroup

    for g := 0; g < w; g++ {
        start := g * chunk
        end := start + chunk
        if start >= df.Rows { break }
        if end > df.Rows { end = df.Rows }
        wg.Add(1)
        go func(s, e int) {
            defer wg.Done()
            row := make(map[string]interface{}, len(refCols))
            for i := s; i < e; i++ {
                for _, c := range refCols {
                    row[c] = df.Data[c][i]
                }
                values[i] = compiled.Fn(row)
            }
        }(start, end)
    }
    wg.Wait()

    df.Data[column] = values
    found := false
    for _, c := range df.Cols {
        if c == column { found = true; break }
    }
    if !found { df.Cols = append(df.Cols, column) }
    return df
}

// Flatten (in-place, no []map intermediate)
func (df *DataFrame) Flatten(flattenCols []string) *DataFrame {
	if df == nil || df.Rows == 0 || len(flattenCols) == 0 {
		return df
	}
	// Build a set for quick column lookup
	colSet := make(map[string]bool, len(df.Cols))
	for _, c := range df.Cols {
		colSet[c] = true
	}

	for _, fcol := range flattenCols {
		// Collect all nested keys once
		keySet := map[string]struct{}{}
		srcCol, ok := df.Data[fcol]
		if !ok {
			continue
		}
		for i := 0; i < df.Rows; i++ {
			val := srcCol[i]
			if val == nil {
				continue
			}
			switch t := val.(type) {
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
			if start >= df.Rows {
				break
			}
			if end > df.Rows {
				end = df.Rows
			}
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
			if c != fcol {
				kept = append(kept, c)
			}
		}
		df.Cols = kept
	}
	return df
}

// Parallel StringArrayConvert (single column)
func (df *DataFrame) StringArrayConvert(column string) *DataFrame {
	if df == nil {
		return df
	}
	slice, ok := df.Data[column]
	if !ok {
		return df
	}
	w := runtime.GOMAXPROCS(0)
	chunk := (df.Rows + w - 1) / w
	var wg sync.WaitGroup
	for g := 0; g < w; g++ {
		start := g * chunk
		end := start + chunk
		if start >= df.Rows {
			break
		}
		if end > df.Rows {
			end = df.Rows
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			for i := s; i < e; i++ {
				str, ok := slice[i].(string)
				if !ok || len(str) < 2 || str[0] != '[' || str[len(str)-1] != ']' {
					continue
				}
				var arr []interface{}
				if err := json.Unmarshal([]byte(str), &arr); err == nil {
					slice[i] = arr
				}
			}
		}(start, end)
	}
	wg.Wait()
	return df
}

func (df *DataFrame) Check() string {
    for _, c := range df.Cols {
        if len(df.Data[c]) != df.Rows {
            panic("invariant violated: column length mismatch: " + c)
			return fmt.Sprintf("column %s has length %d but expected %d", c, len(df.Data[c]), df.Rows)
        }
    }
    return "ok"
}

func (df *DataFrame) Filter(cond interface{}) *DataFrame {
    if df == nil || df.Rows == 0 {
        return &DataFrame{Cols: df.Cols, Data: make(map[string][]interface{}), Rows: 0}
    }

    var pred Column
    var refCols []string

    switch v := cond.(type) {
    case ColumnExpr:
        pred = Compile(v)
        refSet := referencedCols(v, nil)
        refCols = make([]string, 0, len(refSet))
        for c := range refSet { refCols = append(refCols, c) }
        // pad missing/short referenced columns
        for _, c := range refCols {
            s := df.Data[c]
            if s == nil {
                s = make([]interface{}, df.Rows)
                df.Data[c] = s
            } else if len(s) < df.Rows {
                p := make([]interface{}, df.Rows)
                copy(p, s)
                df.Data[c] = p
            }
        }
    case Column:
        pred = v
        // build full row maps (no ref optimization)
        refCols = append(refCols, df.Cols...)
    default:
        fmt.Printf("Filter error: unsupported spec type %T\n", v)
        return df
    }

    w := runtime.GOMAXPROCS(0)
    chunk := (df.Rows + w - 1) / w
    masks := make([][]bool, w)
    counts := make([]int, w)
    var wg sync.WaitGroup

    for g := 0; g < w; g++ {
        start := g * chunk
        end := start + chunk
        if start >= df.Rows { break }
        if end > df.Rows { end = df.Rows }
        wg.Add(1)
        go func(idx, s, e int) {
            defer wg.Done()
            mask := make([]bool, e-s)
            row := make(map[string]interface{}, len(refCols))
            cnt := 0
            for i := s; i < e; i++ {
                for _, c := range refCols { row[c] = df.Data[c][i] }
                if ok, _ := pred.Fn(row).(bool); ok {
                    mask[i-s] = true
                    cnt++
                }
            }
            masks[idx] = mask
            counts[idx] = cnt
        }(g, start, end)
    }
    wg.Wait()

    total := 0
    offsets := make([]int, w)
    for i := 0; i < w; i++ {
        offsets[i] = total
        total += counts[i]
    }

    out := &DataFrame{Cols: df.Cols, Data: make(map[string][]interface{}, len(df.Cols)), Rows: total}
    for _, c := range df.Cols {
        out.Data[c] = make([]interface{}, total)
    }

    for g := 0; g < w; g++ {
        start := g * chunk
        end := start + chunk
        if start >= df.Rows { break }
        if end > df.Rows { end = df.Rows }
        base := offsets[g]
        mask := masks[g]
        wg.Add(1)
        go func(s, e, off int, mask []bool) {
            defer wg.Done()
            widx := off
            for i := s; i < e; i++ {
                if mask[i-s] {
                    for _, c := range df.Cols { out.Data[c][widx] = df.Data[c][i] }
                    widx++
                }
            }
        }(start, end, base, mask)
    }
    wg.Wait()
    return out
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

// explodePrealloc explodes a single column using preallocation
func (df *DataFrame) explodePrealloc(column string) *DataFrame {
	if df == nil || df.Rows == 0 {
		return df
	}

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
	if total == 0 {
		total = df.Rows
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
		if start >= df.Rows {
			break
		}
		if end > df.Rows {
			end = df.Rows
		}
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


// ConvertMapKeysToString converts map keys to strings recursively
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

// flattenOnce flattens only one level of the nested map,
// prefixing each key with the given prefix and a dot.
func flattenOnce(m map[string]interface{}, prefix string) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		result[prefix+"."+k] = v
	}
	return result
}

func (df *DataFrame) KeysToCols(nestedCol string) *DataFrame {
	if df == nil || df.Rows == 0 {
		return df
	}
	src, ok := df.Data[nestedCol]
	if !ok {
		return df
	}

	// discover keys
	keySet := make(map[string]struct{})
	for i := 0; i < df.Rows; i++ {
		v := src[i]
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
	if len(keySet) == 0 {
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

	// Parallel second-pass fill
	w := runtime.GOMAXPROCS(0)
	var wg sync.WaitGroup
	chunk := (df.Rows + w - 1) / w
	for g := 0; g < w; g++ {
		start := g * chunk
		end := start + chunk
		if start >= df.Rows {
			break
		}
		if end > df.Rows {
			end = df.Rows
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			for i := s; i < e; i++ {
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
		}(start, end)
	}
	wg.Wait()

	// drop original
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

// alias
// func (c Column) Alias(newname string) Column{
// 	return
// }

// Parallel FillNA
func (df *DataFrame) FillNA(repl string) *DataFrame {
	if df == nil || df.Rows == 0 {
		return df
	}
	var wg sync.WaitGroup
	for _, c := range df.Cols {
		col := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := df.Data[col]
			for i, v := range s {
				switch t := v.(type) {
				case nil:
					s[i] = repl
				case string:
					if t == "" || strings.ToLower(t) == "null" {
						s[i] = repl
					}
				}
			}
		}()
	}
	wg.Wait()
	return df
}


// Parallel DropNA (mask + prefix-sum + scatter)
func (df *DataFrame) DropNA() *DataFrame {
    if df == nil || df.Rows == 0 {
        return df
    }
    w := runtime.GOMAXPROCS(0)
    chunk := (df.Rows + w - 1) / w

    // Pass 1: build per-shard mask + counts
    masks := make([][]bool, w)
    counts := make([]int, w)
    var wg sync.WaitGroup

    for g := 0; g < w; g++ {
        start := g * chunk
        end := start + chunk
        if start >= df.Rows { break }
        if end > df.Rows { end = df.Rows }
        wg.Add(1)
        go func(idx, s, e int) {
            defer wg.Done()
            mask := make([]bool, e-s)
            cnt := 0
            for i := s; i < e; i++ {
                keep := true
                for _, c := range df.Cols {
                    v := df.Data[c][i]
                    if v == nil { keep = false; break }
                    if str, ok := v.(string); ok && (str == "" || strings.ToLower(str) == "null") {
                        keep = false; break
                    }
                }
                if keep { mask[i-s] = true; cnt++ }
            }
            masks[idx] = mask
            counts[idx] = cnt
        }(g, start, end)
    }
    wg.Wait()

    // Prefix-sum counts to offsets
    total := 0
    offsets := make([]int, w)
    for i := 0; i < w; i++ { offsets[i] = total; total += counts[i] }

    // Allocate output once
    for _, c := range df.Cols {
        df.Data[c] = make([]interface{}, total)
    }
    df.Rows = total

    // Pass 2: scatter
    for g := 0; g < w; g++ {
        start := g * chunk
        end := start + chunk
        if start >= df.Rows+counts[g] { break } // defensive
        if end > df.Rows+counts[g] { end = df.Rows+counts[g] }
        base := offsets[g]
        mask := masks[g]
        wg.Add(1)
        go func(s, e, outStart int, mask []bool) {
            defer wg.Done()
            outIdx := outStart
            for i := s; i < e; i++ {
                if mask[i-s] {
                    for _, c := range df.Cols {
                        df.Data[c][outIdx] = df.Data[c][i]
                    }
                    outIdx++
                }
            }
        }(start, end, base, mask)
    }
    wg.Wait()
    return df
}
// Parallel DropDuplicates (shard hash -> merge)
func (df *DataFrame) DropDuplicates(columns ...string) *DataFrame {
    if df == nil || df.Rows == 0 {
        return df
    }
    uniqueCols := columns
    if len(uniqueCols) == 0 { uniqueCols = df.Cols }

    // Pass 0: compute per-row hash keys in parallel
    w := runtime.GOMAXPROCS(0)
    chunk := (df.Rows + w - 1) / w
    keys := make([]string, df.Rows)
    var wg sync.WaitGroup
    for g := 0; g < w; g++ {
        start := g * chunk
        end := start + chunk
        if start >= df.Rows { break }
        if end > df.Rows { end = df.Rows }
        wg.Add(1)
        go func(s, e int) {
            defer wg.Done()
            for i := s; i < e; i++ {
                keys[i] = rowKeyFast(uniqueCols, df.Data, i)
            }
        }(start, end)
    }
    wg.Wait()

    // Pass 1: build keep mask (first occurrence wins)
    seen := make(map[string]struct{}, df.Rows)
    keep := make([]bool, df.Rows)
    kept := 0
    for i := 0; i < df.Rows; i++ {
        k := keys[i]
        if _, ok := seen[k]; ok {
            continue
        }
        seen[k] = struct{}{}
        keep[i] = true
        kept++
    }

    // Pass 2: prefix-sum positions
    pos := make([]int, df.Rows+1)
    for i := 0; i < df.Rows; i++ {
        pos[i+1] = pos[i]
        if keep[i] { pos[i+1]++ }
    }

    // Allocate and scatter
    for _, c := range df.Cols {
        src := df.Data[c]
        dst := make([]interface{}, kept)
        for i := 0; i < df.Rows; i++ {
            if keep[i] {
                dst[pos[i]] = src[i]
            }
        }
        df.Data[c] = dst
    }
    df.Rows = kept
    return df
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

// GroupBy groups the DataFrame rows by the value produced by groupcol.
// For each group, applies provided Aggregations; all other columns are
// automatically CollectList'ed to preserve data and alignment.
func (df *DataFrame) GroupBy(groupcol string, aggs ...Aggregation) *DataFrame {
    if df == nil || df.Rows == 0 {
        return &DataFrame{Cols: []string{groupcol}, Data: map[string][]interface{}{groupcol: {}}, Rows: 0}
    }

    // Build effective aggregation list: user-provided + default CollectList for the rest
    aggSet := make(map[string]struct{}, len(aggs))
    for _, a := range aggs { aggSet[a.ColumnName] = struct{}{} }

    effectiveAggs := make([]Aggregation, 0, len(aggs)+len(df.Cols))
    effectiveAggs = append(effectiveAggs, aggs...)
    for _, c := range df.Cols {
        if c == groupcol { continue }
        if _, ok := aggSet[c]; !ok {
            effectiveAggs = append(effectiveAggs, CollectList(c))
        }
    }

    // shard in parallel: key -> (col -> []interface{})
    w := runtime.GOMAXPROCS(0)
    chunk := (df.Rows + w - 1) / w
    type groupMap = map[interface{}]map[string][]interface{}
    shards := make([]groupMap, w)

    var wg sync.WaitGroup
    for g := 0; g < w; g++ {
        start := g * chunk
        end := start + chunk
        if start >= df.Rows { break }
        if end > df.Rows { end = df.Rows }
        wg.Add(1)
        go func(idx, s, e int) {
            defer wg.Done()
            local := make(groupMap)
            row := make(map[string]interface{}, len(df.Cols))
            for i := s; i < e; i++ {
                for _, c := range df.Cols {
                    row[c] = df.Data[c][i]
                }
                key := row[groupcol]
                dst, ok := local[key]
                if !ok {
                    dst = make(map[string][]interface{}, len(effectiveAggs))
                    for _, agg := range effectiveAggs {
                        dst[agg.ColumnName] = make([]interface{}, 0, 8)
                    }
                    local[key] = dst
                }
                for _, agg := range effectiveAggs {
                    if v, ok := row[agg.ColumnName]; ok {
                        dst[agg.ColumnName] = append(dst[agg.ColumnName], v)
                    }
                }
            }
            shards[idx] = local
        }(g, start, end)
    }
    wg.Wait()

    // merge shards
    master := make(groupMap)
    for _, sh := range shards {
        for key, cols := range sh {
            dst, ok := master[key]
            if !ok {
                dst = make(map[string][]interface{}, len(cols))
                for name := range cols {
                    dst[name] = make([]interface{}, 0, len(cols[name]))
                }
                master[key] = dst
            }
            for name, vals := range cols {
                master[key][name] = append(master[key][name], vals...)
            }
        }
    }

    // Output schema: group key + all aggregated columns (user aggs + defaults)
    newCols := []string{groupcol}
    for _, agg := range effectiveAggs {
        newCols = append(newCols, agg.ColumnName)
    }
    newData := make(map[string][]interface{}, len(newCols))
    for _, c := range newCols {
        newData[c] = make([]interface{}, 0, len(master))
    }

    // Extract keys (order undefined; sort if needed)
    keys := make([]interface{}, 0, len(master))
    for k := range master { keys = append(keys, k) }

    // Parallel aggregate per key
    w2 := runtime.GOMAXPROCS(0)
    chunk2 := (len(keys) + w2 - 1) / w2
    type rowAgg struct {
        key  interface{}
        vals []interface{} // len = 1 + len(effectiveAggs)
    }
    rowsAgg := make([]rowAgg, len(keys))
    var wg2 sync.WaitGroup
    for g := 0; g < w2; g++ {
        s := g * chunk2
        e := s + chunk2
        if s >= len(keys) { break }
        if e > len(keys) { e = len(keys) }
        wg2.Add(1)
        go func(s, e int) {
            defer wg2.Done()
            for i := s; i < e; i++ {
                k := keys[i]
                cols := master[k]
                ra := rowAgg{key: k, vals: make([]interface{}, 1+len(effectiveAggs))}
                ra.vals[0] = k
                for j, agg := range effectiveAggs {
                    ra.vals[1+j] = agg.Fn(cols[agg.ColumnName])
                }
                rowsAgg[i] = ra
            }
        }(s, e)
    }
    wg2.Wait()

    // Assemble
    for _, r := range rowsAgg {
        newData[groupcol] = append(newData[groupcol], r.vals[0])
        for j, agg := range effectiveAggs {
            newData[agg.ColumnName] = append(newData[agg.ColumnName], r.vals[1+j])
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
// Parallel Join (index build + row assembly)
// Join: keep both sides; collisions become name_l (left) and name_r (right)
func (left *DataFrame) Join(right *DataFrame, leftOn, rightOn, joinType string) *DataFrame {
    if left == nil || right == nil { return nil }

    // Build collision-aware mappings
    leftSet := make(map[string]struct{}, len(left.Cols))
    rightSet := make(map[string]struct{}, len(right.Cols))
    for _, c := range left.Cols { leftSet[c] = struct{}{} }
    for _, c := range right.Cols { rightSet[c] = struct{}{} }

    leftOut := make(map[string]string, len(left.Cols))
    rightOut := make(map[string]string, len(right.Cols))
    newCols := make([]string, 0, len(left.Cols)+len(right.Cols))

    for _, c := range left.Cols {
        if _, dup := rightSet[c]; dup {
            name := c + "_l"
            leftOut[c] = name
            newCols = append(newCols, name)
        } else {
            leftOut[c] = c
            newCols = append(newCols, c)
        }
    }
    for _, c := range right.Cols {
        if _, dup := leftSet[c]; dup {
            name := c + "_r"
            rightOut[c] = name
            newCols = append(newCols, name)
        } else {
            rightOut[c] = c
            newCols = append(newCols, c)
        }
    }

    // Build indices
    leftIndex := make(map[interface{}][]int, left.Rows)
    rightIndex := make(map[interface{}][]int, right.Rows)
    var wg sync.WaitGroup
    wg.Add(2)
    go func() {
        defer wg.Done()
        for i := 0; i < left.Rows; i++ {
            k := left.Data[leftOn][i]
            leftIndex[k] = append(leftIndex[k], i)
        }
    }()
    go func() {
        defer wg.Done()
        for i := 0; i < right.Rows; i++ {
            k := right.Data[rightOn][i]
            rightIndex[k] = append(rightIndex[k], i)
        }
    }()
    wg.Wait()

    type pair struct{ l, r *int }
    pairs := []pair{}
    switch joinType {
    case "inner", "left", "outer":
        for k, lRows := range leftIndex {
            if rRows, ok := rightIndex[k]; ok {
                for _, li := range lRows {
                    for _, ri := range rRows {
                        liCopy, riCopy := li, ri
                        pairs = append(pairs, pair{&liCopy, &riCopy})
                    }
                }
            } else if joinType == "left" || joinType == "outer" {
                for _, li := range lRows {
                    liCopy := li
                    pairs = append(pairs, pair{&liCopy, nil})
                }
            }
        }
        if joinType == "outer" {
            for k, rRows := range rightIndex {
                if _, ok := leftIndex[k]; !ok {
                    for _, ri := range rRows {
                        riCopy := ri
                        pairs = append(pairs, pair{nil, &riCopy})
                    }
                }
            }
        }
    case "right":
        for k, rRows := range rightIndex {
            if lRows, ok := leftIndex[k]; ok {
                for _, li := range lRows {
                    for _, ri := range rRows {
                        liCopy, riCopy := li, ri
                        pairs = append(pairs, pair{&liCopy, &riCopy})
                    }
                }
            } else {
                for _, ri := range rRows {
                    riCopy := ri
                    pairs = append(pairs, pair{nil, &riCopy})
                }
            }
        }
    default:
        fmt.Printf("Unsupported join type %s\n", joinType)
        return nil
    }

    out := make(map[string][]interface{}, len(newCols))
    for _, c := range newCols { out[c] = make([]interface{}, len(pairs)) }

    // Materialize pairs using mapped names
    w := runtime.GOMAXPROCS(0)
    chunk := (len(pairs) + w - 1) / w
    wg = sync.WaitGroup{}
    for g := 0; g < w; g++ {
        start := g * chunk
        end := start + chunk
        if start >= len(pairs) { break }
        if end > len(pairs) { end = len(pairs) }
        wg.Add(1)
        go func(s, e int) {
            defer wg.Done()
            for idx := s; idx < e; idx++ {
                p := pairs[idx]
                for _, c := range left.Cols {
                    name := leftOut[c]
                    if p.l != nil { out[name][idx] = left.Data[c][*p.l] } else { out[name][idx] = nil }
                }
                for _, c := range right.Cols {
                    name := rightOut[c]
                    if p.r != nil { out[name][idx] = right.Data[c][*p.r] } else { out[name][idx] = nil }
                }
            }
        }(start, end)
    }
    wg.Wait()
    return &DataFrame{Cols: newCols, Data: out, Rows: len(pairs)}
}
// Union appends the rows of the other DataFrame to the receiver.
// It returns a new DataFrame that contains the union (vertical concatenation)
// of rows. Columns missing in one DataFrame are filled with nil.
// Parallel Union (two-phase copy)
func (df *DataFrame) Union(other *DataFrame) *DataFrame {
	if df == nil {
		return other
	}
	if other == nil {
		return df
	}
	colSet := make(map[string]struct{})
	newCols := []string{}
	for _, c := range df.Cols {
		if _, ok := colSet[c]; !ok {
			colSet[c] = struct{}{}
			newCols = append(newCols, c)
		}
	}
	for _, c := range other.Cols {
		if _, ok := colSet[c]; !ok {
			colSet[c] = struct{}{}
			newCols = append(newCols, c)
		}
	}
	total := df.Rows + other.Rows
	newData := make(map[string][]interface{}, len(newCols))
	for _, c := range newCols {
		newData[c] = make([]interface{}, total)
	}

	// Limit parallelism to GOMAXPROCS
	workers := runtime.GOMAXPROCS(0)
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, c := range newCols {
		cLocal := c
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			dst := newData[cLocal]
			// copy first df
			if colData, ok := df.Data[cLocal]; ok {
				copy(dst[0:df.Rows], colData)
			} else {
				for i := 0; i < df.Rows; i++ {
					dst[i] = nil
				}
			}
			// copy second df
			if colData, ok := other.Data[cLocal]; ok {
				copy(dst[df.Rows:total], colData)
			} else {
				for i := df.Rows; i < total; i++ {
					dst[i] = nil
				}
			}
		}()
	}
	wg.Wait()
	return &DataFrame{Cols: newCols, Data: newData, Rows: total}
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
// OrderBy sorts the DataFrame by the specified column.
// Parallelize the column rebuild after computing sorted indices.
func (df *DataFrame) OrderBy(column string, asc bool) *DataFrame {
    colData, ok := df.Data[column]
    if !ok {
        fmt.Printf("column %q does not exist\n", column)
        return df
    }
    indices := make([]int, df.Rows)
    for i := 0; i < df.Rows; i++ { indices[i] = i }

    // Precompute keys once, then sort indices using them.
    isString := true
    for i := 0; i < df.Rows; i++ {
        if v := colData[i]; v != nil {
            if _, ok := v.(string); !ok { isString = false; break }
        }
    }

    if isString {
        keys := make([]string, df.Rows)
        for i := 0; i < df.Rows; i++ {
            if s, ok := colData[i].(string); ok { keys[i] = s }
        }
        sort.SliceStable(indices, func(i, j int) bool {
            ai, aj := keys[indices[i]], keys[indices[j]]
            if asc { return ai < aj }
            return ai > aj
        })
    } else {
        keys := make([]float64, df.Rows)
        for i := 0; i < df.Rows; i++ {
            f, _ := toFloat64(colData[i])
            keys[i] = f
        }
        sort.SliceStable(indices, func(i, j int) bool {
            ai, aj := keys[indices[i]], keys[indices[j]]
            if asc { return ai < aj }
            return ai > aj
        })
    }

    newData := make(map[string][]interface{}, len(df.Cols))
    var wg sync.WaitGroup
    var mu sync.Mutex
    w := runtime.GOMAXPROCS(0)
    sem := make(chan struct{}, w)
    for _, col := range df.Cols {
        c := col
        wg.Add(1); sem <- struct{}{}
        go func() {
            defer wg.Done(); defer func(){ <-sem }()
            orig := df.Data[c]
            sorted := make([]interface{}, df.Rows)
            for i, idx := range indices { sorted[i] = orig[idx] }
            mu.Lock()
            newData[c] = sorted
            mu.Unlock()
        }()
    }
    wg.Wait()

    df.Data = newData
    return df
}

// ExceptAll returns rows from df that are not present in other, considering duplicates.
// Comparison is done on the provided columns; if none given, uses the intersection of df and other columns.
func (df *DataFrame) ExceptAll(other *DataFrame, columns ...string) *DataFrame {
    if df == nil {
        return nil
    }
    if other == nil || other.Rows == 0 {
        return df
    }
    // Determine comparison columns
    var cols []string
    if len(columns) > 0 {
        cols = append([]string(nil), columns...)
    } else {
        // intersection of column names, in df order
        rightSet := make(map[string]struct{}, len(other.Cols))
        for _, c := range other.Cols { rightSet[c] = struct{}{} }
        for _, c := range df.Cols {
            if _, ok := rightSet[c]; ok {
                cols = append(cols, c)
            }
        }
    }
    if len(cols) == 0 {
        // Nothing to compare; return df unchanged
        return df
    }

    // Prepare data views with required columns padded to full length
    prepare := func(data map[string][]interface{}, rows int, cols []string) map[string][]interface{} {
        view := make(map[string][]interface{}, len(cols))
        for _, c := range cols {
            s := data[c]
            if s == nil || len(s) < rows {
                tmp := make([]interface{}, rows)
                if s != nil { copy(tmp, s) }
                s = tmp
            }
            view[c] = s
        }
        return view
    }
    leftView := prepare(df.Data, df.Rows, cols)
    rightView := prepare(other.Data, other.Rows, cols)

    // Compute keys in parallel
    w := runtime.GOMAXPROCS(0)
    if w < 1 { w = 1 }
    chunkL := (df.Rows + w - 1) / w
    keysLeft := make([]string, df.Rows)
    var wg sync.WaitGroup
    for g := 0; g < w; g++ {
        s := g * chunkL
        e := s + chunkL
        if s >= df.Rows { break }
        if e > df.Rows { e = df.Rows }
        wg.Add(1)
        go func(s, e int) {
            defer wg.Done()
            for i := s; i < e; i++ {
                keysLeft[i] = rowKeyFast(cols, leftView, i)
            }
        }(s, e)
    }
    wg.Wait()

    chunkR := (other.Rows + w - 1) / w
    keysRight := make([]string, other.Rows)
    for g := 0; g < w; g++ {
        s := g * chunkR
        e := s + chunkR
        if s >= other.Rows { break }
        if e > other.Rows { e = other.Rows }
        wg.Add(1)
        go func(s, e int) {
            defer wg.Done()
            for i := s; i < e; i++ {
                keysRight[i] = rowKeyFast(cols, rightView, i)
            }
        }(s, e)
    }
    wg.Wait()

    // Build multiset counts from right
    rightCount := make(map[string]int, len(keysRight))
    for _, k := range keysRight {
        rightCount[k]++
    }

    // Keep mask: skip as many left duplicates as rightCount permits
    keep := make([]bool, df.Rows)
    kept := 0
    for i := 0; i < df.Rows; i++ {
        k := keysLeft[i]
        if cnt := rightCount[k]; cnt > 0 {
            rightCount[k] = cnt - 1 // consume
            continue
        }
        keep[i] = true
        kept++
    }

    // Prefix-sum positions
    pos := make([]int, df.Rows+1)
    for i := 0; i < df.Rows; i++ {
        pos[i+1] = pos[i]
        if keep[i] { pos[i+1]++ }
    }

    // Assemble output with same schema as df
    newData := make(map[string][]interface{}, len(df.Cols))
    for _, c := range df.Cols {
        dst := make([]interface{}, kept)
        src := df.Data[c]
        for i := 0; i < df.Rows; i++ {
            if keep[i] {
                dst[pos[i]] = src[i]
            }
        }
        newData[c] = dst
    }
    return &DataFrame{Cols: append([]string(nil), df.Cols...), Data: newData, Rows: kept}
}