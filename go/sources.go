package gophers

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
	"gopkg.in/yaml.v2"

	// "github.com/xitongsys/parquet-go/ParquetFile"
	// "github.com/xitongsys/parquet-go/Writer"
	_ "github.com/mattn/go-sqlite3"
)

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

	// // Initialize each column with a slice sized to the number of rows.
	// for _, col := range df.Cols {
	// 	df.Data[col] = make([]interface{}, df.Rows)
	// }

	// // Fill the DataFrame with data.
	// for i, row := range rows {
	// 	for _, col := range df.Cols {
	// 		val, ok := row[col]

	// 		if ok {
	// 			// Example conversion:
	// 			// JSON unmarshals numbers as float64 by default.
	// 			// If the float64 value is a whole number, convert it to int.
	// 			if f, isFloat := val.(float64); isFloat {
	// 				if f == float64(int(f)) {
	// 					val = int(f)
	// 				}
	// 			}
	// 			df.Data[col][i] = val
	// 		} else {
	// 			// If a column is missing in a row, set it to nil.
	// 			df.Data[col][i] = nil
	// 		}
	// 	}
	// }

	// Initialize each column slice
	for _, col := range df.Cols {
		df.Data[col] = make([]interface{}, df.Rows)
	}
	// Parallel per-column population
	var wg sync.WaitGroup
	for _, col := range df.Cols {
		c := col
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, row := range rows {
				if val, ok := row[c]; ok {
					if f, isFloat := val.(float64); isFloat && f == float64(int(f)) {
						val = int(f)
					}
					df.Data[c][i] = val
				} else {
					df.Data[c][i] = nil
				}
			}
		}()
	}
	wg.Wait()

	return df
}
func fileExists(filename string) bool {
	if filename == "" {
		return false
	}
	// If the input starts with "{" or "[", assume it is JSON and not a file path.
	if strings.HasPrefix(filename, "{") || strings.HasPrefix(filename, "[") || strings.HasPrefix(filename, "<") {
		return false
	}
	info, err := os.Stat(filename)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

// Functions for intaking data and returning dataframe
// ReadCSV parses CSV from a file path or raw CSV text and returns a DataFrame (pure Go).
func ReadCSV(input string) *DataFrame {
	// If input is a file path, read it; otherwise treat as raw CSV text.
	csvContent := input
	if fi, err := os.Stat(input); err == nil && !fi.IsDir() {
		b, err := os.ReadFile(input)
		if err != nil {
			log.Fatalf("ReadCSV: read file: %v", err)
		}
		csvContent = string(b)
	}

	r := csv.NewReader(strings.NewReader(csvContent))
	headers, err := r.Read()
	if err != nil {
		log.Fatalf("ReadCSV: read headers: %v", err)
	}

	rows := make([]map[string]interface{}, 0, 1024)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("ReadCSV: read record: %v", err)
		}
		row := make(map[string]interface{}, len(headers))
		for i, h := range headers {
			if i < len(record) {
				row[h] = record[i]
			} else {
				row[h] = ""
			}
		}
		rows = append(rows, row)
	}
	return Dataframe(rows)
}

// Pure Go: parse path-or-JSON into a DataFrame, no cgo types.
func ReadJSON(input string) *DataFrame {
	// Allow file path or raw JSON
	jsonContent := input
	if fileExists(input) {
		bytes, err := os.ReadFile(input)
		if err != nil {
			log.Fatalf("ReadJSONCore: read file: %v", err)
		}
		jsonContent = string(bytes)
	}

	trimmed := strings.TrimSpace(jsonContent)
	if len(trimmed) == 0 {
		return &DataFrame{Cols: []string{}, Data: map[string][]interface{}{}, Rows: 0}
	}

	// Single object -> wrap in array
	if trimmed[0] == '{' {
		jsonContent = "[" + trimmed + "]"
		trimmed = jsonContent
	}

	// Fast path: array of objects with concurrent unmarshal
	if trimmed[0] == '[' {
		dec := json.NewDecoder(strings.NewReader(jsonContent))
		tok, err := dec.Token()
		if err != nil || tok != json.Delim('[') {
			log.Fatalf("ReadJSONCore: decode start: %v", err)
		}

		raws := make([]json.RawMessage, 0, 1024)
		for dec.More() {
			var rm json.RawMessage
			if err := dec.Decode(&rm); err != nil {
				log.Fatalf("ReadJSONCore: decode element: %v", err)
			}
			raws = append(raws, rm)
		}
		if _, err := dec.Token(); err != nil {
			log.Fatalf("ReadJSONCore: decode end: %v", err)
		}

		rows := make([]map[string]interface{}, len(raws))
		if len(raws) > 0 {
			w := runtime.GOMAXPROCS(0)
			chunk := (len(raws) + w - 1) / w
			var wg sync.WaitGroup
			for g := 0; g < w; g++ {
				start := g * chunk
				end := start + chunk
				if start >= len(raws) {
					break
				}
				if end > len(raws) {
					end = len(raws)
				}
				wg.Add(1)
				go func(s, e int) {
					defer wg.Done()
					var tmp map[string]interface{}
					for i := s; i < e; i++ {
						if err := json.Unmarshal(raws[i], &tmp); err == nil {
							m := make(map[string]interface{}, len(tmp))
							for k, v := range tmp {
								m[k] = v
							}
							rows[i] = m
						}
					}
				}(start, end)
			}
			wg.Wait()
		}
		return Dataframe(rows)
	}

	// Fallback
	var rows []map[string]interface{}
	if err := json.Unmarshal([]byte(jsonContent), &rows); err != nil {
		log.Fatalf("ReadJSONCore: unmarshal: %v", err)
	}
	return Dataframe(rows)
}

// Pure Go NDJSON reader: path-or-string -> *DataFrame
func ReadNDJSON(input string) *DataFrame {
    // If input is a file path, load file contents.
    if fileExists(input) {
        b, err := os.ReadFile(input)
        if err != nil {
            log.Fatalf("ReadNDJSONCore: read file: %v", err)
        }
        input = string(b)
    }
    lines := strings.Split(input, "\n")
    n := len(lines)
    if n == 0 {
        return Dataframe([]map[string]interface{}{})
    }

    // Pass 1: build per-shard masks and counts (non-empty lines)
    w := runtime.GOMAXPROCS(0)
    if w < 1 {
        w = 1
    }
    chunk := (n + w - 1) / w

    masks := make([][]bool, w)
    counts := make([]int, w)

    var wg sync.WaitGroup
    for g := 0; g < w; g++ {
        start := g * chunk
        end := start + chunk
        if start >= n {
            break
        }
        if end > n {
            end = n
        }
        wg.Add(1)
        go func(idx, s, e int) {
            defer wg.Done()
            mask := make([]bool, e-s)
            cnt := 0
            for i := s; i < e; i++ {
                if strings.TrimSpace(lines[i]) != "" {
                    mask[i-s] = true
                    cnt++
                }
            }
            masks[idx] = mask
            counts[idx] = cnt
        }(g, start, end)
    }
    wg.Wait()

    // Prefix-sum to compute output offsets
    total := 0
    offsets := make([]int, w)
    for i := 0; i < w; i++ {
        offsets[i] = total
        total += counts[i]
    }
    rows := make([]map[string]interface{}, total)

    // Pass 2: scatter decoded rows in order
    for g := 0; g < w; g++ {
        start := g * chunk
        end := start + chunk
        if start >= n {
            break
        }
        if end > n {
            end = n
        }
        base := offsets[g]
        mask := masks[g]
        wg.Add(1)
        go func(s, e, outStart int, mask []bool) {
            defer wg.Done()
            out := outStart
            var tmp map[string]interface{}
            for i := s; i < e; i++ {
                if !mask[i-s] {
                    continue
                }
                raw := strings.TrimSpace(lines[i])
                // Decode; on error leave nil row (safe for Dataframe)
                if err := json.Unmarshal([]byte(raw), &tmp); err == nil {
                    // copy map to avoid races on tmp reuse
                    m := make(map[string]interface{}, len(tmp))
                    for k, v := range tmp {
                        m[k] = v
                    }
                    rows[out] = m
                }
                out++
            }
        }(start, end, base, mask)
    }
    wg.Wait()

    return Dataframe(rows)
}
// Pure Go: YAML path-or-string -> *DataFrame
func ReadYAML(input string) *DataFrame {
	// Treat input as a file path if it exists, else as raw YAML text.
	yamlContent := input
	if fileExists(input) {
		b, err := os.ReadFile(input)
		if err != nil {
			log.Fatalf("ReadYAMLCore: read file: %v", err)
		}
		yamlContent = string(b)
	}

	// Unmarshal into generic interface to support map or list roots.
	var any interface{}
	if err := yaml.Unmarshal([]byte(yamlContent), &any); err != nil {
		log.Fatalf("ReadYAMLCore: unmarshal: %v", err)
	}

	switch v := any.(type) {
	case map[interface{}]interface{}:
		// Single object -> one-row DataFrame
		rows := mapToRows(convertMapKeysToString(v))
		return Dataframe(rows)
	case []interface{}:
		// List of objects -> multi-row DataFrame
		rows := make([]map[string]interface{}, 0, len(v))
		for _, elem := range v {
			switch m := elem.(type) {
			case map[interface{}]interface{}:
				rows = append(rows, convertMapKeysToString(m))
			case map[string]interface{}:
				rows = append(rows, m)
			default:
				// Non-map element: put under a generic column
				rows = append(rows, map[string]interface{}{"value": m})
			}
		}
		return Dataframe(rows)
	default:
		// Scalar -> single-row DataFrame with generic column
		return Dataframe([]map[string]interface{}{{"value": v}})
	}
}

// ReadParquet reads a parquet file or newline-delimited JSON content (fallback) and builds a DataFrame.
// Concurrency is used to parse each line in parallel while preserving order.
func ReadParquet(input string) *DataFrame {
    // If input is a file path, load its contents.
    if fileExists(input) {
        bytes, err := os.ReadFile(input)
        if err != nil {
            log.Fatalf("ReadParquet: read file error: %v", err)
        }
        input = string(bytes)
    }

    lines := strings.Split(input, "\n")
    n := len(lines)
    if n == 0 {
        return Dataframe([]map[string]interface{}{})
    }

    // Pass 1: mask + counts (non-empty trimmed lines)
    w := runtime.GOMAXPROCS(0)
    if w < 1 {
        w = 1
    }
    chunk := (n + w - 1) / w

    masks := make([][]bool, w)
    counts := make([]int, w)

    var wg sync.WaitGroup
    for g := 0; g < w; g++ {
        start := g * chunk
        end := start + chunk
        if start >= n {
            break
        }
        if end > n {
            end = n
        }
        wg.Add(1)
        go func(idx, s, e int) {
            defer wg.Done()
            mask := make([]bool, e-s)
            cnt := 0
            for i := s; i < e; i++ {
                if strings.TrimSpace(lines[i]) != "" {
                    mask[i-s] = true
                    cnt++
                }
            }
            masks[idx] = mask
            counts[idx] = cnt
        }(g, start, end)
    }
    wg.Wait()

    // Prefix-sum to compute output offsets
    total := 0
    offsets := make([]int, w)
    for i := 0; i < w; i++ {
        offsets[i] = total
        total += counts[i]
    }
    rows := make([]map[string]interface{}, total)

    // Pass 2: scatter decoded rows in order
    for g := 0; g < w; g++ {
        start := g * chunk
        end := start + chunk
        if start >= n {
            break
        }
        if end > n {
            end = n
        }
        base := offsets[g]
        mask := masks[g]
        wg.Add(1)
        go func(s, e, outStart int, mask []bool) {
            defer wg.Done()
            out := outStart
            var tmp map[string]interface{}
            for i := s; i < e; i++ {
                if !mask[i-s] {
                    continue
                }
                raw := strings.TrimSpace(lines[i])
                if err := json.Unmarshal([]byte(raw), &tmp); err == nil {
                    m := make(map[string]interface{}, len(tmp))
                    for k, v := range tmp {
                        m[k] = v
                    }
                    rows[out] = m
                }
                out++
            }
        }(start, end, base, mask)
    }
    wg.Wait()

    return Dataframe(rows)
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

// ReadSqlite is pure Go. It returns a DataFrame from a sqlite DB given either a table or a query.
// If both table and query are empty, it reads all user tables and concatenates them (adds _table column).
func ReadSqlite(path, table, query string) (*DataFrame, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("ReadSqliteDF: open error: %w", err)
	}
	defer db.Close()

	var rows []map[string]interface{}
	switch {
	case strings.TrimSpace(query) != "":
		rs, err := fetchRows(db, query, "")
		if err != nil {
			return nil, fmt.Errorf("ReadSqliteDF: query error: %w", err)
		}
		rows = append(rows, rs...)
	case strings.TrimSpace(table) != "":
		q := fmt.Sprintf(`SELECT * FROM %q`, table)
		rs, err := fetchRows(db, q, "")
		if err != nil {
			return nil, fmt.Errorf("ReadSqliteDF: table read error: %w", err)
		}
		rows = append(rows, rs...)
	default:
		// read all user tables concurrently
		names := []string{}
		r, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
		if err != nil {
			return nil, fmt.Errorf("ReadSqliteDF: list tables error: %w", err)
		}
		for r.Next() {
			var name string
			if err := r.Scan(&name); err == nil {
				names = append(names, name)
			}
		}
		_ = r.Close()

		var wg sync.WaitGroup
		mu := sync.Mutex{}
		for _, name := range names {
			tbl := name
			wg.Add(1)
			go func() {
				defer wg.Done()
				rs, err := fetchRows(db, fmt.Sprintf(`SELECT * FROM %q`, tbl), tbl)
				if err != nil {
					// non-fatal; log and continue
					log.Printf("ReadSqliteDF: read table %s error: %v", tbl, err)
					return
				}
				mu.Lock()
				rows = append(rows, rs...)
				mu.Unlock()
			}()
		}
		wg.Wait()
	}

	return Dataframe(rows), nil
}

// Clone creates a deep copy of the DataFrame (new Cols slice and new per-column []interface{}).
func (df *DataFrame) Clone() *DataFrame {
	if df == nil {
		return &DataFrame{Cols: nil, Data: make(map[string][]interface{}), Rows: 0}
	}
	newCols := make([]string, len(df.Cols))
	copy(newCols, df.Cols)
	newData := make(map[string][]interface{}, len(df.Data))
	var wg sync.WaitGroup
	for _, c := range df.Cols {
		cLocal := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			src := df.Data[cLocal]
			dst := make([]interface{}, len(src))
			copy(dst, src)
			newData[cLocal] = dst
		}()
	}
	wg.Wait()
	return &DataFrame{Cols: newCols, Data: newData, Rows: df.Rows}
}

func CloneJSON(in string) string {
	var df DataFrame
	if err := json.Unmarshal([]byte(in), &df); err != nil {
		b, _ := json.Marshal(map[string]string{"error": "clone unmarshal: " + err.Error()})
		return string(b)
	}
	out, err := json.Marshal(df.Clone())
	if err != nil {
		b, _ := json.Marshal(map[string]string{"error": "clone marshal: " + err.Error()})
		return string(b)
	}
	return string(out)
}

// read delta table?

// read iceberg table?

// GetAPI performs a GET request to the specified API endpoint.
// headers is a map of header keys and values, and queryParams are appended to the URL.
// The JSON response is converted to a DataFrame via ReadJSON.
// Example: df, err := GetAPI("https://api.example.com/data", map[string]string{"Authorization":"Bearer X"}, map[string]string{"limit":"10"})
func GetAPI(endpoint string, headers map[string]string, queryParams map[string]string) (*DataFrame, error) {
	// Parse the endpoint URL.
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("GetAPI: parse endpoint: %w", err)
	}

	// Add query parameters.
	q := u.Query()
	for k, v := range queryParams {
		if k == "" || v == "" {
			continue
		}
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()

	// Create request and add headers.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("GetAPI: create request: %w", err)
	}
	for k, v := range headers {
		if k == "" || v == "" {
			continue
		}
		req.Header.Set(k, v)
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json")
	}

	// Execute request.
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GetAPI: do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GetAPI: bad status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	// Read body and convert JSON -> DataFrame using existing ReadJSON.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("GetAPI: read body: %w", err)
	}
	df := ReadJSON(string(body))
	return df, nil
}

// ReadHTML scrapes a URL / file / raw HTML and returns a DataFrame of element metadata.
// All HTML fragments are stored as escaped strings (safe for plain text display).
func ReadHTML(input string) *DataFrame {
	raw := input
	var baseURL *url.URL
	if strings.HasPrefix(input, "http://") || strings.HasPrefix(input, "https://") {
		resp, err := http.Get(input)
		if err != nil {
			log.Fatalf("ReadHTML: GET error: %v", err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatalf("ReadHTML: read body: %v", err)
		}
		raw = string(b)
		baseURL, _ = url.Parse(input)
	} else if fileExists(input) {
		b, err := os.ReadFile(input)
		if err != nil {
			log.Fatalf("ReadHTML: read file: %v", err)
		}
		raw = string(b)
	}

	doc, err := html.Parse(strings.NewReader(raw))
	if err != nil {
		log.Fatalf("ReadHTML: parse: %v", err)
	}

	nodes := make([]*nodeInfo, 0, 1024)
	var stack []struct {
		n         *html.Node
		parentIdx int
		depth     int
	}
	stack = append(stack, struct {
		n         *html.Node
		parentIdx int
		depth     int
	}{doc, -1, 0})

	for len(stack) > 0 {
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		cur := top.n

		if cur.Type == html.ElementNode {
			// Skip script/iframe entirely
			if cur.Data == "script" || cur.Data == "iframe" {
				continue
			}
			idx := len(nodes)
			// Direct text
			var tb strings.Builder
			for c := cur.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.TextNode {
					t := strings.TrimSpace(c.Data)
					if t != "" {
						if tb.Len() > 0 {
							tb.WriteByte(' ')
						}
						tb.WriteString(t)
					}
				}
			}
			var href, src string
			for _, a := range cur.Attr {
				switch a.Key {
				case "href":
					href = a.Val
				case "src":
					src = a.Val
				}
			}
			nodes = append(nodes, &nodeInfo{
				n:           cur,
				index:       idx,
				parentIndex: top.parentIdx,
				depth:       top.depth,
				tag:         cur.Data,
				textDirect:  tb.String(),
				href:        href,
				src:         src,
			})
			parent := idx

			// Push children (reverse for natural order)
			var rev []*html.Node
			for c := cur.FirstChild; c != nil; c = c.NextSibling {
				rev = append(rev, c)
			}
			for i := len(rev) - 1; i >= 0; i-- {
				stack = append(stack, struct {
					n         *html.Node
					parentIdx int
					depth     int
				}{rev[i], parent, top.depth + 1})
			}
		} else {
			// Traverse non-elements
			var rev []*html.Node
			for c := cur.FirstChild; c != nil; c = c.NextSibling {
				rev = append(rev, c)
			}
			for i := len(rev) - 1; i >= 0; i-- {
				stack = append(stack, struct {
					n         *html.Node
					parentIdx int
					depth     int
				}{rev[i], top.parentIdx, top.depth + 1})
			}
		}
	}

	if len(nodes) == 0 {
		return Dataframe([]map[string]interface{}{})
	}

	// Helpers
	renderNode := func(n *html.Node) string {
		var buf bytes.Buffer
		html.Render(&buf, n)
		return buf.String()
	}
	renderInner := func(n *html.Node) string {
		var buf bytes.Buffer
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			if c.Type == html.ElementNode && (c.Data == "script" || c.Data == "iframe") {
				continue
			}
			html.Render(&buf, c)
		}
		return buf.String()
	}
	resolve := func(raw string) string {
		if raw == "" || baseURL == nil {
			return raw
		}
		u, err := url.Parse(raw)
		if err != nil {
			return raw
		}
		if u.Scheme == "" && u.Host == "" {
			return baseURL.ResolveReference(u).String()
		}
		return raw
	}

	// Concurrency for rendering
	w := runtime.GOMAXPROCS(0)
	if w < 1 {
		w = 1
	}
	chunk := (len(nodes) + w - 1) / w

	out := make([]rendered, len(nodes))
	var wg sync.WaitGroup
	for g := 0; g < w; g++ {
		start := g * chunk
		end := start + chunk
		if start >= len(nodes) {
			break
		}
		if end > len(nodes) {
			end = len(nodes)
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			for i := s; i < e; i++ {
				n := nodes[i]
				rawOuter := renderNode(n.n)
				rawInner := renderInner(n.n)
				out[i] = rendered{
					outer: html.EscapeString(rawOuter),
					inner: html.EscapeString(rawInner),
					habs:  resolve(n.href),
					sabs:  resolve(n.src),
				}
			}
		}(start, end)
	}
	wg.Wait()

	rows := make([]map[string]interface{}, len(nodes))
	for i, n := range nodes {
		rows[i] = map[string]interface{}{
			"index":          n.index,
			"parent_index":   n.parentIndex,
			"depth":          n.depth,
			"tag":            n.tag,
			"text":           n.textDirect,
			"href":           n.href,
			"href_abs":       out[i].habs,
			"src":            n.src,
			"src_abs":        out[i].sabs,
			"outer_html_str": out[i].outer,
			"inner_html_str": out[i].inner,
		}
	}
	return Dataframe(rows)
}

// javascript request source? (django/flask?)
