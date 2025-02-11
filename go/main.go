
package main

import (
    "gophers/gophers/source"
    "fmt"
)

func main() {
	// Example JSON string.
	// Make sure your JSON uses double quotes.
	jsonStr := `
	  {"name": "Alice", "age": "30", "score": 85.5}
	  {"name": "Bob",   "age": "25", "score": 90.0}
      `


	// Create a DataFrame from the unmarshalled rows.
	df := gophers.READNDJSON(jsonStr)

	// Print the DataFrame.
	df.SHOW()

    df.HEAD()

    df.TAIL()

    fmt.Println(df.COLUMNS())
}
