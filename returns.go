package gophers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"strings"
)

func (df *DataFrame) Columns() []string {
	return df.Cols
}

// schema of json ?

// count
func (df *DataFrame) Count() int {
	return df.Rows
}

// Parallel CountDuplicates (returns count)
func (df *DataFrame) CountDuplicates(columns ...string) int {
	if df == nil || df.Rows == 0 {
		return 0
	}
	uniqueCols := columns
	if len(uniqueCols) == 0 {
		uniqueCols = df.Cols
	}
	w := runtime.GOMAXPROCS(0)
	chunk := (df.Rows + w - 1) / w
	type shard struct {
		dups int
		seen map[string]struct{}
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
			loc := shard{seen: make(map[string]struct{}, (e-s)/2)}
			row := make(map[string]interface{}, len(uniqueCols))
			for i := s; i < e; i++ {
				for _, c := range uniqueCols {
					row[c] = df.Data[c][i]
				}
				b, _ := json.Marshal(row)
				k := string(b)
				if _, ok := loc.seen[k]; ok {
					loc.dups++
				} else {
					loc.seen[k] = struct{}{}
				}
			}
			shards[idx] = loc
		}(g, start, end)
	}
	wg.Wait()
	globalSeen := make(map[string]struct{})
	totalDups := 0
	for _, sh := range shards {
		for k := range sh.seen {
			if _, ok := globalSeen[k]; ok {
				totalDups++
			} else {
				globalSeen[k] = struct{}{}
			}
		}
		totalDups += sh.dups
	}
	return totalDups
}

// CountDistinct returns the count of unique values in given column(s)
func (df *DataFrame) CountDistinct(columns ...string) int {
	if df == nil || df.Rows == 0 {
		return 0
	}
	uniqueCols := columns
	if len(uniqueCols) == 0 {
		uniqueCols = df.Cols
	}

	w := runtime.GOMAXPROCS(0)
	chunk := (df.Rows + w - 1) / w
	type shardSet map[string]struct{}
	sets := make([]shardSet, w)

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
			local := make(shardSet, e-s)
			row := make(map[string]interface{}, len(uniqueCols))
			for i := s; i < e; i++ {
				for _, c := range uniqueCols {
					row[c] = df.Data[c][i]
				}
				b, _ := json.Marshal(row)
				local[string(b)] = struct{}{}
			}
			sets[idx] = local
		}(g, start, end)
	}
	wg.Wait()

	// merge
	union := make(map[string]struct{})
	for _, ss := range sets {
		for k := range ss {
			union[k] = struct{}{}
		}
	}
	return len(union)
}

func (df *DataFrame) Collect(c string) []interface{} {
    if df == nil || df.Rows == 0 {
        return []interface{}{}
    }
    col := strings.TrimSpace(c)
    if col == "" {
        return []interface{}{}
    }
    data, ok := df.Data[col]
    if !ok || data == nil {
        return []interface{}{}
    }
    return data
}

// Pure Go schema builder (no cgo types).
func GetSqliteSchema(path, tbl string) (map[string]interface{}, error) {
	if tbl == "" {
		return nil, fmt.Errorf("table is required")
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("open error: %w", err)
	}
	defer db.Close()

	var cnt int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, tbl).Scan(&cnt); err != nil {
		return nil, fmt.Errorf("exists check error: %w", err)
	}
	if cnt == 0 {
		return nil, fmt.Errorf("table %s not found", tbl)
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
				return nil, fmt.Errorf("table_info scan error: %w", err)
			}
			var dfltVal interface{}
			if dflt.Valid {
				dfltVal = dflt.String
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
			return nil, fmt.Errorf("table_info rows error: %w", err)
		}
	} else {
		return nil, fmt.Errorf("table_info error: %w", err)
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
				return nil, fmt.Errorf("fk scan error: %w", err)
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
			return nil, fmt.Errorf("fk rows error: %w", err)
		}
	}

	// indexes
	indexes := []map[string]interface{}{}
	qIdx := `PRAGMA index_list(` + quoteIdent(tbl) + `)`
	if rows, err := db.Query(qIdx); err == nil {
		defer rows.Close()
		for rows.Next() {
			var seq, unique int
			var name, origin string
			var partial sql.NullInt64
			errScan := rows.Scan(&seq, &name, &unique, &origin, &partial)
			if errScan != nil {
				// fallback (older sqlite with 4 columns)
				rows2, err2 := db.Query(qIdx)
				if err2 != nil {
					return nil, fmt.Errorf("index_list requery error: %w", err2)
				}
				defer rows2.Close()
				indexes = []map[string]interface{}{}
				for rows2.Next() {
					var seq2, unique2 int
					var name2, origin2 string
					if err := rows2.Scan(&seq2, &name2, &unique2, &origin2); err != nil {
						return nil, fmt.Errorf("index_list scan error: %w", err)
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
					return nil, fmt.Errorf("index_list rows error: %w", err)
				}
				break
			}
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
			return nil, fmt.Errorf("index_list rows error: %w", err)
		}
	}

	return map[string]interface{}{
		"table":        tbl,
		"columns":      cols,
		"foreign_keys": fks,
		"indexes":      indexes,
	}, nil
}

// Optional helper: JSON convenience (used by wrappers)
func GetSqliteSchemaJSON(dbPath, table string) string {
	m, err := GetSqliteSchema(dbPath, table)
	if err != nil {
		b, _ := json.Marshal(map[string]string{"error": err.Error()})
		return string(b)
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// GetSqliteTables returns all user tables (excludes internal sqlite_% tables).
func GetSqliteTables(dbPath string) ([]string, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open error: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	names := make([]string, 0, 16)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	sort.Strings(names)
	return names, nil
}

// GetSqliteTablesJSON wraps ListSqliteTables and returns {"tables":[...]} or {"error": "..."}.
func GetSqliteTablesJSON(dbPath string) string {
	names, err := GetSqliteTables(dbPath)
	if err != nil {
		b, _ := json.Marshal(map[string]string{"error": err.Error()})
		return string(b)
	}
	b, _ := json.Marshal(map[string]any{"tables": names})
	return string(b)
}

// Dtypes returns PySpark-like [(col, type)].
func (df *DataFrame) Dtypes() [][2]string {
    if df == nil || df.Rows == 0 || len(df.Cols) == 0 {
        return [][2]string{}
    }
    out := make([][2]string, len(df.Cols))

    var wg sync.WaitGroup
    for i, c := range df.Cols {
        wg.Add(1)
        go func(idx int, col string) {
            defer wg.Done()
            data := df.Data[col]
            t, _ := inferColumnType(data)
            out[idx] = [2]string{col, t}
        }(i, c)
    }
    wg.Wait()
    return out
}

// Schema returns detailed column schema.
func (df *DataFrame) Schema() []ColumnSchema {
    if df == nil || df.Rows == 0 || len(df.Cols) == 0 {
        return []ColumnSchema{}
    }
    out := make([]ColumnSchema, len(df.Cols))

    var wg sync.WaitGroup
    for i, c := range df.Cols {
        wg.Add(1)
        go func(idx int, col string) {
            defer wg.Done()
            data := df.Data[col]
            t, nullable := inferColumnType(data)
            out[idx] = ColumnSchema{Name: col, Type: t, Nullable: nullable}
        }(i, c)
    }
    wg.Wait()
    return out
}

// PrintSchema pretty-prints the schema (like PySpark printSchema).
func (df *DataFrame) PrintSchema() {
    s := df.Schema()
    fmt.Println("root")
    for _, cs := range s {
        nn := "true"
        if !cs.Nullable { nn = "false" }
        fmt.Printf(" |-- %s: %s (nullable = %s)\n", cs.Name, cs.Type, nn)
    }
}

// SchemaJSON returns the schema as JSON.
func (df *DataFrame) SchemaJSON() string {
    b, _ := json.Marshal(struct {
        Schema []ColumnSchema `json:"schema"`
        Rows   int            `json:"rows"`
        Cols   int            `json:"cols"`
    }{
        Schema: df.Schema(),
        Rows:   df.Rows,
        Cols:   len(df.Cols),
    })
    return string(b)
}

// --- helpers ---

// inferColumnType inspects all values in a column and returns a Spark-like type string and nullability.
func inferColumnType(col []interface{}) (string, bool) {
    nullable := false
    elemType := "" // unified type
    for _, v := range col {
        if v == nil {
            nullable = true
            continue
        }
        t := typeString(v)
        elemType = unifyType(elemType, t)
    }
    if elemType == "" {
        // all nulls
        return "null", true
    }
    return elemType, nullable
}

// typeString maps Go values to Spark-ish types.
func typeString(v interface{}) string {
    switch t := v.(type) {
    case string:
        return "string"
    case bool:
        return "boolean"
    case int, int32, int64:
        return "int"
    case float32, float64:
        return "float"
    case []string:
        // array<string>
        return "array<string>"
    case []interface{}:
        // infer element type
        et := ""
        for _, e := range t {
            et = unifyType(et, typeString(e))
        }
        if et == "" {
            et = "any"
        }
        return "array<" + et + ">"
    case map[string]interface{}:
        vt := ""
        for _, vv := range t {
            vt = unifyType(vt, typeString(vv))
        }
        if vt == "" {
            vt = "any"
        }
        return "map<string," + vt + ">"
    case map[interface{}]interface{}:
        // best-effort: keys coerced to string, values unified
        vt := ""
        for _, vv := range t {
            vt = unifyType(vt, typeString(vv))
        }
        if vt == "" {
            vt = "any"
        }
        return "map<string," + vt + ">"
    default:
        return "any"
    }
}

// unifyType promotes types to a common supertype.
func unifyType(a, b string) string {
    if a == "" {
        return b
    }
    if b == "" || a == b {
        return a
    }
    // numeric promotion
    if (a == "int" && b == "float") || (a == "float" && b == "int") {
        return "float"
    }
    // arrays/maps: if element/value types differ, fall back to any
    if strings.HasPrefix(a, "array<") && strings.HasPrefix(b, "array<") {
        ae := strings.TrimSuffix(strings.TrimPrefix(a, "array<"), ">")
        be := strings.TrimSuffix(strings.TrimPrefix(b, "array<"), ">")
        ue := unifyType(ae, be)
        return "array<" + ue + ">"
    }
    if strings.HasPrefix(a, "map<string,") && strings.HasPrefix(b, "map<string,") {
        ae := strings.TrimSuffix(strings.TrimPrefix(a, "map<string,"), ">")
        be := strings.TrimSuffix(strings.TrimPrefix(b, "map<string,"), ">")
        ue := unifyType(ae, be)
        return "map<string," + ue + ">"
    }
    // string wins over anything else for mixed types
    if a == "string" || b == "string" {
        return "string"
    }
    // mixed boolean/numeric -> string (to be safe)
    return "string"
}