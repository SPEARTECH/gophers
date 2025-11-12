//go:build js && wasm

package main

import (
	"syscall/js"
)

// read sqlitetable not possible in javascript
// use official sqlite3 wasm, pass to go to make dataframe
// func ReadSqlite(this js.Value, args []js.Value) any {
// 	if len(args) < 3 {
// 		return "error: usage ReadSqlite(path, table, query)"
// 	}
// 	path := args[0].String()
// 	table := args[1].String()
// 	query := args[2].String()

// 	jsStr, err := g.ReadSqliteJSON(path, table, query)
// 	if err != nil {
// 		return `{"error":"` + err.Error() + `"}`
// 	}
// 	return jsStr
// }

func main() {
	// WARNING: sqlite3 driver (mattn/go-sqlite3) is not supported in wasm.
	// This binding compiles only if your build excludes sqlite usage or swaps the driver.
	root := js.Global()
	if root.Get("gophers").IsUndefined() {
		root.Set("gophers", js.ValueOf(map[string]any{}))
	}
	// root.Get("gophers").Set("ReadSqlite", js.FuncOf(ReadSqlite))
	select {}
}
