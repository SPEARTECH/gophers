package main

import (
	
	. "gophers"
)

func main() {
	// Example JSON string.
	// Make sure your JSON uses double quotes.
	// jsonStr := `
	//   {"namythingamajigya": "Alicandro", "age": "30", "score": 85.5}
	//   {"namythingamajigya": "Bob", "age": "25", "score": 90.0}
	//   `
	// jsonStr := "test_data/jsontest.json"
	var jsonStr = `[{"name": "Gopher", "age": 5},{"name": "Gupy", "age": 15}]`

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

	// df.Vertical(25) // sort alphabetically?

	// df.ToJSON("newjson.json")

	// df = df.Select("age", "score")
	// df.Show(25)

	// df = df.Rename("namythingamajigya", "name")
	df.Show(5,5)
	// df = df.Column("name2", g.ValueFrom("name"))
	// data := df.ToJSON()
	// fmt.Println(data)
	// df = df.FillNA("tyler")
	// df.BrowserDisplay()
	// df = df.DropNA()

	// Example of doing logical operations on a column's values
	// df = df.Column("age", If(Col("age").IsNull(), Lit("is null"),
	// 	If(Col("age").IsNotNull(), Lit("not null"),
	// 		Lit("idk"))))

	// // Example of passing anonymous function to perform logical operations
	// df = df.Column("age", func(row map[string]interface{}) interface{} {
	// 	value := row["age"]
	// 	// Check if the value is nil or an empty/"null" string.
	// 	if value == nil {
	// 		return "is null"
	// 	}
	// 	if s, ok := value.(string); ok {
	// 		if s == "" || strings.ToLower(s) == "null" {
	// 			return "is null"
	// 		}
	// 		// You can add any other condition here.
	// 		return "not null"
	// 	}
	// 	// Default case if value is non-string.
	// 	return "idk"
	// })

	// df = df.Column("age", If(Col("age").Eq(Col("age")), Lit("is not null"),
	// 	Lit("idk")))
	// df.Show(10)

	// // Example filtering with or condition and multiline formatting
	// df.Filter(
	// 	Or(
	// 		Col("age").Ne("is null"),
	// 		Col("age").Eq("is stuff null"))).
	// 	Show(10)

	// Example concatenating columns
	// df = df.Column("newcol", Concat_WS("-", Col("name"), Col("age")))
	// df = df.Column("collected_col", Lit(df.Collect("name")))
	// df = df.Explode("collected_col")
	// df = df.Column("age", SHA256(Col("name")))
	// df.Show(10,10)
	// df.DisplayBrowser()
	// dash := CreateReport("My Analysis")
	// dash.AddPage("page1")
	// dash.AddPage("page2")
	// dash.AddHTML("page2", "<h1>NEW HTML!</h1>")
	// dash.AddText("page2", "this is example text")
	// dash.AddHeading("page1", "heading", 1)
	// dash.AddText("page1", "This is a lot of text that I cannot really think of right now to populate something meaningful yet here i am typing stuff anyways to test this")
	// dash.AddChart("page1", df.StackedPercentChart("name age title", "subtitle", "name", Agg(Unique("age"), Sum("score"))))

// 	dash.AddSubText("page1", "This is a subtext for the chart above maybe")
// 	dash.AddText("page1", `Here is some test text to see if this works well 
// but if not then ill try something else`)
// 	dash.AddBullets("page1", "bullets", "This is a bullet point", "This is another bullet point")
// 	dash.AddDataframe("page2", df)
// 	dash.Open()
	// df.Show(10)
	// df.Select("collected_col").Show(25)
	// df = df.DropDuplicates()
	// df = df.Explode("collected_col")
	// df.Vertical(50)
	// join example
	// df2 := df
	// df = df.Join(df2, "name", "name", "inner")
	// fmt.Println(df.Display())
}
