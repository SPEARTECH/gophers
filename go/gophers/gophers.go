package gophers

import (
	"fmt"
)

// Add is a function that adds two integers passed from JavaScript.
func Add(args []int) interface{} {
	// Convert JS values to Go ints.
	a := args[0]
	b := args[1]
	sum := a + b
	fmt.Printf("Adding %d and %d to get %d\n", a, b, sum)
	return sum
}