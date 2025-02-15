
package main

import (
    gf "gophers/gophers"
	"fmt"
)

func main() {
	// Example JSON string.
	// Make sure your JSON uses double quotes.
	jsonStr := `
	  {"namythingamajigya": "Alicandro", "age": "30", "score": 85.5}
	  {"namythingamajigya": "Bob", "age": "25", "score": 90.0}
      `


	// Create a DataFrame from the unmarshalled rows.
	df := gf.ReadNDJSON(jsonStr)

	// Print the DataFrame.
	df.Show(5)

    // df.head()

    // df.Tail()

    // fmt.Println(df.Columns())

	// df.Vertical(80,1) // sort alphabetically?
}
