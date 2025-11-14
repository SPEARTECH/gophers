package gophers

import (
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
)

func (df *DataFrame) ToCSVFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// headers
	if err := writer.Write(df.Cols); err != nil {
		return err
	}

	// build rows in parallel, then write sequentially
	rows := make([][]string, df.Rows)
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
			buf := make([]string, len(df.Cols))
			for i := s; i < e; i++ {
				// reuse buf capacity but must not share; copy per row
				for j, col := range df.Cols {
					buf[j] = fmt.Sprintf("%v", df.Data[col][i])
				}
				rowCopy := make([]string, len(buf))
				copy(rowCopy, buf)
				rows[i] = rowCopy
			}
		}(start, end)
	}
	wg.Wait()

	for i := 0; i < df.Rows; i++ {
		if rows[i] == nil {
			// should not happen; skip defensively
			continue
		}
		if err := writer.Write(rows[i]); err != nil {
			return err
		}
	}
	return nil
}

// dataframe to json file
func (df *DataFrame) ToJSONFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a slice of maps to hold the rows of data.
	rows := make([]map[string]interface{}, df.Rows)
	for i := 0; i < df.Rows; i++ {
		row := make(map[string]interface{})
		for _, col := range df.Cols {
			row[col] = df.Data[col][i]
		}
		rows[i] = row
	}

	// Marshal the rows into JSON format.
	jsonData, err := json.Marshal(rows)
	if err != nil {
		return err
	}

	// Write the JSON data to the file.
	_, err = file.Write(jsonData)
	if err != nil {
		return err
	}

	return nil
}

// dataframe to json string
func (df *DataFrame) ToJSON() string {

	// Create a slice of maps to hold the rows of data.
	rows := make([]map[string]interface{}, df.Rows)
	for i := 0; i < df.Rows; i++ {
		row := make(map[string]interface{})
		for _, col := range df.Cols {
			row[col] = df.Data[col][i]
		}
		rows[i] = row
	}

	// Marshal the rows into JSON format.
	jsonData, err := json.Marshal(rows)
	if err != nil {
		log.Fatalf("Error marshalling JSON: %v", err)
	}

	return string(jsonData)
}

// dataframe to ndjson file
func (df *DataFrame) ToNDJSONFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write each row as a separate JSON object on a new line.
	for i := 0; i < df.Rows; i++ {
		row := make(map[string]interface{})
		for _, col := range df.Cols {
			row[col] = df.Data[col][i]
		}

		// Marshal the row into JSON format.
		jsonData, err := json.Marshal(row)
		if err != nil {
			return err
		}

		// Write the JSON data to the file, followed by a newline character.
		_, err = file.Write(jsonData)
		if err != nil {
			return err
		}
		_, err = file.WriteString("\n")
		if err != nil {
			return err
		}
	}

	return nil
}

// write to parquet?
func (df *DataFrame) ToParquetFile(filename string) error {
	// fw, err := ParquetFile.NewLocalFileWriter(filename)
	// if err != nil {
	// 	return err
	// }
	// defer fw.Close()

	// pw, err := Writer.NewParquetWriter(fw, new(map[string]interface{}), 4)
	// if err != nil {
	// 	return err
	// }
	// defer pw.WriteStop()

	// for i := 0; i < df.Rows; i++ {
	// 	row := make(map[string]interface{})
	// 	for _, col := range df.Cols {
	// 		row[col] = df.Data[col][i]
	// 	}
	// 	if err := pw.Write(row); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

// write to table? (mongo, postgres, mysql, sqlite, etc)
// JDBC?

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
		if err != nil {
			return err
		}
		defer db.Close()
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		if err := ensureTableAndColumns(tx, table, df); err != nil {
			return err
		}
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

// ToRows converts the DataFrame's columnar storage into a slice of row maps (concurrent).
func (df *DataFrame) ToRows() []map[string]interface{} {
	if df == nil || df.Rows == 0 {
		return []map[string]interface{}{}
	}
	rows := make([]map[string]interface{}, df.Rows)
	w := runtime.GOMAXPROCS(0)
	if w < 1 {
		w = 1
	}
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
				m := make(map[string]interface{}, len(df.Cols))
				for _, c := range df.Cols {
					col := df.Data[c]
					if i < len(col) {
						m[c] = col[i]
					} else {
						m[c] = nil
					}
				}
				rows[i] = m
			}
		}(start, end)
	}
	wg.Wait()
	return rows
}

// PostAPI sends the DataFrame (as JSON array of row objects) via HTTP POST.
// Returns the raw response body (string) or an error. Does not parse the response.
func (df *DataFrame) PostAPI(endpoint string, headers map[string]string, queryParams map[string]string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("PostAPI: parse endpoint: %w", err)
	}
	q := u.Query()
	for k, v := range queryParams {
		if k == "" {
			continue
		}
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()

	rows := df.ToRows()
	bodyBytes, err := json.Marshal(rows)
	if err != nil {
		return "", fmt.Errorf("PostAPI: marshal rows: %w", err)
	}

	req, err := http.NewRequest("POST", u.String(), strings.NewReader(string(bodyBytes)))
	if err != nil {
		return "", fmt.Errorf("PostAPI: create request: %w", err)
	}
	hasCT := false
	for k, v := range headers {
		if k == "" {
			continue
		}
		if strings.ToLower(k) == "content-type" {
			hasCT = true
		}
		req.Header.Set(k, v)
	}
	if !hasCT {
		req.Header.Set("Content-Type", "application/json")
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "*/*")
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("PostAPI: do request: %w", err)
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return string(b), fmt.Errorf("PostAPI: bad status %d", resp.StatusCode)
	}
	return string(b), nil
}
