package gophers

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// Print displays the DataFrame in a simple tabular format.
func (df *DataFrame) Show(chars int, record_count ...int) {
	var records int
	if len(record_count) > 0 {
		records = record_count[0]
	} else {
		records = df.Rows
	}
	if chars <= 0 {
		chars = 25
	} else if chars < 5 {
		chars = 5
	}

	for _, col := range df.Cols {
		if len(col) > chars {
			fmt.Printf("%-15s", col[:chars-3]+"...")
		} else {
			fmt.Printf("%-15s", col)
		}
	}
	fmt.Println()

	// Print each row.
	for i := 0; i < records; i++ {
		for _, col := range df.Cols {
			value := df.Data[col][i]
			var strvalue string
			switch v := value.(type) {
			case int:
				strvalue = strconv.Itoa(v)
			case float64:
				strvalue = strconv.FormatFloat(v, 'f', 2, 64)
			case bool:
				strvalue = strconv.FormatBool(v)
			case string:
				strvalue = v
			default:
				strvalue = fmt.Sprintf("%v", v)
			}

			if len(strvalue) > chars {
				fmt.Printf("%-15v", strvalue[:chars-3]+"...")
			} else {
				fmt.Printf("%-15v", strvalue)
			}
		}
		fmt.Println()
	}
}

// Show the top 5 rows of data
func (df *DataFrame) Head(chars int) {
	var records int
	if df.Rows < 5 {
		records = df.Rows
	} else {
		records = 5
	}
	if chars <= 0 {
		chars = 25
	} else if chars < 5 {
		chars = 5
	}

	for _, col := range df.Cols {
		if len(col) >= chars {
			fmt.Printf("%-15s", col[:chars-3]+"...")
		} else {
			fmt.Printf("%-15s", col)
		}
	}
	fmt.Println()

	// Print each row.
	for i := 0; i < records; i++ {
		for _, col := range df.Cols {
			value := df.Data[col][i]
			var strvalue string
			switch v := value.(type) {
			case int:
				strvalue = strconv.Itoa(v)
			case float64:
				strvalue = strconv.FormatFloat(v, 'f', 2, 64)
			case bool:
				strvalue = strconv.FormatBool(v)
			case string:
				strvalue = v
			default:
				strvalue = fmt.Sprintf("%v", v)
			}

			if len(strvalue) > chars {
				fmt.Printf("%-15v", strvalue[:chars-3]+"...")
			} else {
				fmt.Printf("%-15v", strvalue)
			}
		}
		fmt.Println()
	}
}

// Show the bottom 5 rows of data
func (df *DataFrame) Tail(chars int) {
	var records int
	if df.Rows < 5 {
		records = df.Rows
	} else {
		records = 5
	}
	if chars <= 0 {
		chars = 25
	} else if chars < 5 {
		chars = 5
	}

	for _, col := range df.Cols {
		if len(col) >= chars {
			fmt.Printf("%-15s", col[:chars-3]+"...")
		} else {
			fmt.Printf("%-15s", col)
		}
	}
	fmt.Println()

	// Print each row.
	for i := df.Rows - records; i < df.Rows; i++ {
		for _, col := range df.Cols {
			value := df.Data[col][i]
			var strvalue string
			switch v := value.(type) {
			case int:
				strvalue = strconv.Itoa(v)
			case float64:
				strvalue = strconv.FormatFloat(v, 'f', 2, 64)
			case bool:
				strvalue = strconv.FormatBool(v)
			case string:
				strvalue = v
			default:
				strvalue = fmt.Sprintf("%v", v)
			}

			if len(strvalue) > chars {
				fmt.Printf("%-15v", strvalue[:chars-3]+"...")
			} else {
				fmt.Printf("%-15v", strvalue)
			}
		}
		fmt.Println()
	}

}

// Show data in a vertical format
func (df *DataFrame) Vertical(chars int, record_count ...int) {
	var records int
	if len(record_count) > 0 {
		records = record_count[0]
	} else {
		records = df.Rows
	}
	if chars <= 0 {
		chars = 25
	}
	count := 0
	max_len := 0
	for count < df.Rows && count < records {
		fmt.Println("------------", "Record", count, "------------")
		for _, col := range df.Cols {
			if len(col) > max_len {
				max_len = len(col)
			}
		}
		for _, col := range df.Cols {
			values, exists := df.Data[col]
			if !exists {
				fmt.Println("Column not found:", col)
				continue
			}

			if count < len(values) {
				var item1 string
				if chars >= len(col) {
					item1 = col
				} else {
					item1 = fmt.Sprint(col[:chars-3], "...")
				}
				var item2 string
				switch v := values[count].(type) {
				case int:
					item2 = strconv.Itoa(v)
				case float64:
					item2 = strconv.FormatFloat(v, 'f', 2, 64)
				case bool:
					item2 = strconv.FormatBool(v)
				case string:
					item2 = v
				default:
					item2 = fmt.Sprintf("%v", v)
				}
				if chars < len(item2) {
					item2 = item2[:chars]
				}
				space := "\t"
				var num int
				num = (max_len - len(item1)) / 5
				if num > 0 {
					for i := 0; i < num; i++ { //fix math
						// if
						space += "\t"
					}
				}
				fmt.Println(item1, space, item2)
			}
		}
		count++
	}
}

// DisplayHTML returns a value that gophernotes recognizes as rich HTML output.
func DisplayHTML(html string) map[string]interface{} {
	return map[string]interface{}{
		"text/html": html,
	}
}

func DisplayChart(chart Chart) map[string]interface{} {
	html := chart.htmlpreid + chart.htmldivid + chart.htmlpostid + chart.jspreid + chart.htmldivid + chart.jspostid
	return map[string]interface{}{
		"text/html": html,
	}
}

// QuoteArray returns a string representation of a Go array with quotes around the values.
func QuoteArray(arr []string) string {
	quoted := make([]string, len(arr))
	for i, v := range arr {
		quoted[i] = fmt.Sprintf("%q", v)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// DisplayHTML returns a value that gophernotes recognizes as rich HTML output.
func (df *DataFrame) DisplayBrowser() error {
	// display an html table of the dataframe for analysis, filtering, sorting, etc
	html := `
	<!DOCTYPE html>
	<html>
		<head>
			<script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
			<link href="https://cdn.jsdelivr.net/npm/daisyui@4.7.2/dist/full.min.css" rel="stylesheet" type="text/css" />
			<script src="https://cdn.tailwindcss.com"></script>
			<script src="https://code.highcharts.com/highcharts.js"></script>
			<script src="https://code.highcharts.com/modules/boost.js"></script>
			<script src="https://code.highcharts.com/modules/exporting.js"></script>
			<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200" />
			<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, minimal-ui">
		</head>
		<body>
			<div id="app" style="text-align: center;" class="overflow-x-auto">
				<table class="table table-xs">
	  				<thead>
						<tr>
							<th></th>
							<th v-for="col in cols"><a class="btn btn-sm btn-ghost justify justify-start">[[ col ]]<span class="material-symbols-outlined">arrow_drop_down</span></a></th>
						</tr>
					</thead>
					<tbody>
					<tr v-for="i in Array.from({length:` + strconv.Itoa(df.Rows) + `}).keys()" :key="i">
							<th class="pl-5">[[ i ]]</th>
							<td v-for="col in cols" :key="col" class="pl-5">[[ data[col][i] ]]</td>
						</tr>
					</tbody>
				</table>
			</div>
		</body>
		<script>
			const { createApp } = Vue
			createApp({
			delimiters : ['[[', ']]'],
				data(){
					return {
						cols: ` + QuoteArray(df.Cols) + `,
						data: ` + mapToString(df.Data) + `,
						selected_col: {},
						page: 1,
						pages: [],
						total_pages: 0
					}
				},
				methods: {

				},
				watch: {

				},
				created(){
					this.total_pages = Math.ceil(Object.keys(this.data).length / 100)
				},

				mounted() {

				},
				computed:{

				}

			}).mount('#app')
		</script>
	</html>
	`
	// Create a temporary file
	tmpFile, err := os.CreateTemp(os.TempDir(), "temp-*.html")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %v", err)
	}
	defer tmpFile.Close()

	// Write the HTML string to the temporary file
	if _, err := tmpFile.Write([]byte(html)); err != nil {
		return fmt.Errorf("failed to write to temporary file: %v", err)
	}

	// Open the temporary file in the default web browser
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("cmd", "/c", "start", tmpFile.Name())
	case "darwin":
		cmd = exec.Command("open", tmpFile.Name())
	default: // "linux", "freebsd", "openbsd", "netbsd"
		cmd = exec.Command("xdg-open", tmpFile.Name())
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to open file in browser: %v", err)
	}

	return nil
}

// mapToString converts the DataFrame data to a JSON-like string with quoted values.
func mapToString(data map[string][]interface{}) string {
	var builder strings.Builder

	builder.WriteString("{")
	first := true
	for key, values := range data {
		if !first {
			builder.WriteString(", ")
		}
		first = false

		builder.WriteString(fmt.Sprintf("%q: [", key))
		for i, value := range values {
			if i > 0 {
				builder.WriteString(", ")
			}
			switch v := value.(type) {
			case int, float64, bool:
				builder.WriteString(fmt.Sprintf("%v", v))
			case string:
				builder.WriteString(fmt.Sprintf("%q", v))
			default:
				builder.WriteString(fmt.Sprintf("%q", fmt.Sprintf("%v", v)))
			}
		}
		builder.WriteString("]")
	}
	builder.WriteString("}")

	return builder.String()
}

// Display an html table of the data
func (df *DataFrame) Display() map[string]interface{} {
	// display an html table of the dataframe for analysis, filtering, sorting, etc
	html := `
<!DOCTYPE html>
<html>
	<head>
		<script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
		<link href="https://cdn.jsdelivr.net/npm/daisyui@4.7.2/dist/full.min.css" rel="stylesheet" type="text/css" />
		<script src="https://cdn.tailwindcss.com"></script>
		<script src="https://code.highcharts.com/highcharts.js"></script>
		<script src="https://code.highcharts.com/modules/boost.js"></script>
		<script src="https://code.highcharts.com/modules/exporting.js"></script>
		<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200" />
		<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, minimal-ui">
	</head>
	<body>
		<div id="table" style="text-align: center;" class="overflow-x-auto">
			<table class="table">
				<thead>
					<tr>
						<th></th>
						<th v-for="col in cols">[[ col ]]</th>
					</tr>
				</thead>
				<tbody>
				<tr v-for="i in Array.from({length:` + strconv.Itoa(df.Rows) + `}).keys()" :key="i">
						<th>[[ i ]]</th>
						<td v-for="col in cols">[[ data[col][i] ]]</td>
					</tr>
				</tbody>
			</table>
		</div>
	</body>
	<script>
		const { createApp } = Vue
		createApp({
		delimiters :  ["[[", "]]"],
			data(){
				return {
					cols: ` + QuoteArray(df.Cols) + `,
					data: ` + mapToString(df.Data) + `,
				}
			},
			methods: {

			},
			watch: {

			},
			created(){

			},

			mounted() {

			},
			computed:{

			}

		}).mount("#table")
	</script>
</html>	
`
	return map[string]interface{}{
		"text/html": html,
	}
}

// write an html display, chart, or dashboard to a file
func (df *DataFrame) DisplayToFile(path string) error {
	// Ensure the path ends with .html
	if !strings.HasSuffix(path, ".html") {
		path += ".html"
	}
	html := df.Display()["text/html"].(string)

	// Write the HTML string to the specified file path
	err := os.WriteFile(path, []byte(html), 0644)
	if err != nil {
		return fmt.Errorf("failed to write to file: %v", err)
	}

	return nil
}

// DataTypes ?

// Create and display an example dataframe; the smallest dataframe possible to display a value for each column
