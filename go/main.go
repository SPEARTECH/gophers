package main

import (
	. "gophers/gophers"
)

func main() {
	// Example JSON string.
	// Make sure your JSON uses double quotes.
	// jsonStr := `
	//   {"namythingamajigya": "Alicandro", "age": "30", "score": 85.5}
	//   {"namythingamajigya": "Bob", "age": "25", "score": 90.0}
	//   `
	jsonStr := "test_data/jsontest.json"
	// Create a DataFrame from the unmarshalled rows.
	// df := gf.ReadNDJSON(jsonStr)
	df := ReadJSON(jsonStr)
	// df := gf.ReadCSV(jsonStr)

	// Print the DataFrame.
	// df.Show(7, 1)

	// df.Head(5)

	// df.Tail(8)

	// columns := df.Columns()
	// fmt.Println(columns)

	// df.Vertical(80, 1) // sort alphabetically?

	// df.ToJSON("newjson.json")

	// df = df.Select("age", "score")

	df = df.Rename("namythingamajigya", "name")
	// df = df.Column("name2", g.ValueFrom("name"))
	// data := df.ToJSON()
	// fmt.Println(data)
	// df = df.FillNA("tyler")
	// df.BrowserDisplay()
	// df = df.DropNA()
	df = df.Column("age", When(Col("age").IsNull(), Lit("tyler"), Col("age")))
	df.Show(10)
}
