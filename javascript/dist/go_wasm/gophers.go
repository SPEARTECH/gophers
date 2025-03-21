// go_wasm/gophers.go
package main

import (
	"syscall/js"
	"fmt"
)

// add is a function that adds two integers passed from JavaScript.
func add(this js.Value, args []js.Value) interface{} {
	// Convert JS values to Go ints.
	a := args[0].Int()
	b := args[1].Int()
	sum := a + b
	fmt.Printf("Adding %d and %d to get %d\n", a, b, sum)
	return sum
}

func main() {
	fmt.Println("Go WebAssembly loaded and exposing functions.")

	// Register the add function on the global object.
	js.Global().Set("add", js.FuncOf(add))
	
	// Optionally, register more functions similarly:
	// js.Global().Set("multiply", js.FuncOf(multiply))

	// Prevent the Go program from exiting.
	select {}
}