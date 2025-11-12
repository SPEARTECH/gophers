package gophers

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Column adds or modifies a column in the DataFrame using a Column.
// This version accepts a Column (whose underlying function is applied to each row).
func (df *DataFrame) Column(column string, colSpec ColumnExpr) *DataFrame {
	values := make([]interface{}, df.Rows)
	if df.Rows == 0 {
		df.Data[column] = values
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
	// Parallel per-row evaluation
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
			// reuse a single map per worker to reduce allocs
			row := make(map[string]interface{}, len(df.Cols))
			for i := s; i < e; i++ {
				for _, c := range df.Cols {
					row[c] = df.Data[c][i]
				}
				values[i] = Evaluate(colSpec, row)
			}
		}(start, end)
	}
	wg.Wait()

	df.Data[column] = values
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

// Parallel Filter (preserves order)
func (df *DataFrame) Filter(cond ColumnExpr) *DataFrame {
	if df == nil || df.Rows == 0 {
		return &DataFrame{Cols: df.Cols, Data: make(map[string][]interface{}), Rows: 0}
	}
	w := runtime.GOMAXPROCS(0)
	chunk := (df.Rows + w - 1) / w
	type shard struct {
		rows int
		data map[string][]interface{}
	}
	shards := make([]shard, w)
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
		go func(idx, s, e int) {
			defer wg.Done()
			loc := shard{data: make(map[string][]interface{}, len(df.Cols))}
			for _, c := range df.Cols {
				loc.data[c] = make([]interface{}, 0, e-s) // over alloc; trimmed by actual matches
			}
			row := make(map[string]interface{}, len(df.Cols))
			for i := s; i < e; i++ {
				for _, c := range df.Cols {
					row[c] = df.Data[c][i]
				}
				okVal, _ := Evaluate(cond, row).(bool)
				if okVal {
					for _, c := range df.Cols {
						loc.data[c] = append(loc.data[c], row[c])
					}
					loc.rows++
				}
			}
			shards[idx] = loc
		}(g, start, end)
	}
	wg.Wait()
	// merge
	out := &DataFrame{Cols: df.Cols, Data: make(map[string][]interface{}, len(df.Cols))}
	total := 0
	for _, sh := range shards {
		total += sh.rows
	}
	for _, c := range df.Cols {
		out.Data[c] = make([]interface{}, 0, total)
		for _, sh := range shards {
			out.Data[c] = append(out.Data[c], sh.data[c]...)
		}
	}
	out.Rows = total
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

// Parallel DropNA (all values non-nil/non-empty)
func (df *DataFrame) DropNA() *DataFrame {
	if df == nil || df.Rows == 0 {
		return df
	}
	w := runtime.GOMAXPROCS(0)
	chunk := (df.Rows + w - 1) / w
	type shard struct {
		rows int
		data map[string][]interface{}
	}
	shards := make([]shard, w)
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
		go func(idx, s, e int) {
			defer wg.Done()
			loc := shard{data: make(map[string][]interface{}, len(df.Cols))}
			for _, c := range df.Cols {
				loc.data[c] = make([]interface{}, 0, e-s)
			}
			for i := s; i < e; i++ {
				keep := true
				for _, c := range df.Cols {
					v := df.Data[c][i]
					if v == nil {
						keep = false
						break
					}
					if str, ok := v.(string); ok && (str == "" || strings.ToLower(str) == "null") {
						keep = false
						break
					}
				}
				if keep {
					for _, c := range df.Cols {
						loc.data[c] = append(loc.data[c], df.Data[c][i])
					}
					loc.rows++
				}
			}
			shards[idx] = loc
		}(g, start, end)
	}
	wg.Wait()
	total := 0
	for _, sh := range shards {
		total += sh.rows
	}
	for _, c := range df.Cols {
		merged := make([]interface{}, 0, total)
		for _, sh := range shards {
			merged = append(merged, sh.data[c]...)
		}
		df.Data[c] = merged
	}
	df.Rows = total
	return df
}

// Parallel DropDuplicates (shard hash -> merge)
func (df *DataFrame) DropDuplicates(columns ...string) *DataFrame {
	if df == nil || df.Rows == 0 {
		return df
	}
	uniqueCols := columns
	if len(uniqueCols) == 0 {
		uniqueCols = df.Cols
	}
	w := runtime.GOMAXPROCS(0)
	chunk := (df.Rows + w - 1) / w
	type shard struct {
		keys []string
		idxs []int
	}
	shards := make([]shard, w)
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
		go func(idx, s, e int) {
			defer wg.Done()
			locSeen := make(map[string]int)
			keys := []string{}
			idxs := []int{}
			row := make(map[string]interface{}, len(uniqueCols))
			for i := s; i < e; i++ {
				for _, c := range uniqueCols {
					row[c] = df.Data[c][i]
				}
				b, _ := json.Marshal(row)
				k := string(b)
				if _, ok := locSeen[k]; !ok {
					locSeen[k] = i
					keys = append(keys, k)
					idxs = append(idxs, i)
				}
			}
			shards[idx] = shard{keys, idxs}
		}(g, start, end)
	}
	wg.Wait()
	global := make(map[string]bool)
	outIdx := []int{}
	for _, sh := range shards {
		for i, k := range sh.keys {
			if !global[k] {
				global[k] = true
				outIdx = append(outIdx, sh.idxs[i])
			}
		}
	}
	for _, c := range df.Cols {
		src := df.Data[c]
		dst := make([]interface{}, len(outIdx))
		for i, idx := range outIdx {
			dst[i] = src[idx]
		}
		df.Data[c] = dst
	}
	df.Rows = len(outIdx)
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
// For each group, it applies each provided Aggregation on the values from the corresponding column.
// Parallel sharded build + merge; then aggregate.
func (df *DataFrame) GroupBy(groupcol string, aggs ...Aggregation) *DataFrame {
	if df == nil || df.Rows == 0 {
		return &DataFrame{Cols: []string{groupcol}, Data: map[string][]interface{}{groupcol: {}}, Rows: 0}
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
		if start >= df.Rows {
			break
		}
		if end > df.Rows {
			end = df.Rows
		}
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
					dst = make(map[string][]interface{}, len(aggs))
					for _, agg := range aggs {
						dst[agg.ColumnName] = make([]interface{}, 0, 8)
					}
					local[key] = dst
				}
				for _, agg := range aggs {
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

	// // Build output columns: group key + one per agg target (same names as input cols)
	// newCols := []string{groupcol}
	// for _, agg := range aggs {
	//     newCols = append(newCols, agg.ColumnName)
	// }
	// newData := make(map[string][]interface{}, len(newCols))
	// for _, c := range newCols {
	//     newData[c] = []interface{}{}
	// }

	// // Emit one row per group (order is map iteration order; sort keys if you need determinism)
	// for key, colVals := range master {
	//     newData[groupcol] = append(newData[groupcol], key)
	//     for _, agg := range aggs {
	//         newData[agg.ColumnName] = append(newData[agg.ColumnName], agg.Fn(colVals[agg.ColumnName]))
	//     }
	// }

	newCols := []string{groupcol}
	for _, agg := range aggs {
		newCols = append(newCols, agg.ColumnName)
	}
	newData := make(map[string][]interface{}, len(newCols))
	for _, c := range newCols {
		newData[c] = make([]interface{}, 0, len(master))
	}

	// Extract keys (for deterministic order you could sort later)
	keys := make([]interface{}, 0, len(master))
	for k := range master {
		keys = append(keys, k)
	}

	// Parallel aggregate per key
	w2 := runtime.GOMAXPROCS(0)
	chunk2 := (len(keys) + w2 - 1) / w2
	type rowAgg struct {
		key  interface{}
		vals []interface{} // len = 1 + len(aggs)
	}
	rowsAgg := make([]rowAgg, len(keys))
	var wg2 sync.WaitGroup
	for g := 0; g < w2; g++ {
		s := g * chunk2
		e := s + chunk2
		if s >= len(keys) {
			break
		}
		if e > len(keys) {
			e = len(keys)
		}
		wg2.Add(1)
		go func(s, e int) {
			defer wg2.Done()
			for i := s; i < e; i++ {
				k := keys[i]
				cols := master[k]
				ra := rowAgg{key: k, vals: make([]interface{}, 1+len(aggs))}
				ra.vals[0] = k
				for j, agg := range aggs {
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
		for j, agg := range aggs {
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
func (left *DataFrame) Join(right *DataFrame, leftOn, rightOn, joinType string) *DataFrame {
	if left == nil || right == nil {
		return nil
	}
	newCols := append([]string{}, left.Cols...)
	for _, c := range right.Cols {
		if c != rightOn {
			newCols = append(newCols, c)
		}
	}
	leftIndex := make(map[interface{}][]int, left.Rows)
	rightIndex := make(map[interface{}][]int, right.Rows)
	// Build indices in parallel
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
			rRows, ok := rightIndex[k]
			if ok {
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
			lRows, ok := leftIndex[k]
			if ok {
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
	for _, c := range newCols {
		out[c] = make([]interface{}, len(pairs))
	}

	// Parallel materialize pairs
	w := runtime.GOMAXPROCS(0)
	chunk := (len(pairs) + w - 1) / w
	wg = sync.WaitGroup{}
	for g := 0; g < w; g++ {
		start := g * chunk
		end := start + chunk
		if start >= len(pairs) {
			break
		}
		if end > len(pairs) {
			end = len(pairs)
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			for idx := s; idx < e; idx++ {
				p := pairs[idx]
				// left columns
				for _, c := range left.Cols {
					if p.l != nil {
						out[c][idx] = left.Data[c][*p.l]
					} else {
						out[c][idx] = nil
					}
				}
				// right columns (skip rightOn)
				for _, c := range right.Cols {
					if c == rightOn {
						continue
					}
					if p.r != nil {
						out[c][idx] = right.Data[c][*p.r]
					} else {
						out[c][idx] = nil
					}
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
	for i := 0; i < df.Rows; i++ {
		indices[i] = i
	}

	sort.Slice(indices, func(i, j int) bool {
		a := colData[indices[i]]
		b := colData[indices[j]]
		if sa, okA := a.(string); okA {
			if sb, okB := b.(string); okB {
				if asc {
					return sa < sb
				}
				return sa > sb
			}
		}
		af, ea := toFloat64(a)
		bf, eb := toFloat64(b)
		if ea != nil || eb != nil {
			as := fmt.Sprintf("%v", a)
			bs := fmt.Sprintf("%v", b)
			if asc {
				return as < bs
			}
			return as > bs
		}
		if asc {
			return af < bf
		}
		return af > bf
	})

	newData := make(map[string][]interface{}, len(df.Cols))
	// parallel rebuild per column (guard map writes)
	var wg sync.WaitGroup
	var mu sync.Mutex
	w := runtime.GOMAXPROCS(0)
	sem := make(chan struct{}, w) // bound goroutines
	for _, col := range df.Cols {
		c := col
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			orig := df.Data[c]
			sorted := make([]interface{}, df.Rows)
			for i, idx := range indices {
				sorted[i] = orig[idx]
			}
			mu.Lock()
			newData[c] = sorted
			mu.Unlock()
		}()
	}
	wg.Wait()

	df.Data = newData
	return df
}
