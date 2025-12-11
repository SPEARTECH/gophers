package gophers

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"encoding/json"
)

// Show returns a plain-text table representation of the DataFrame.
// Pure Go: no cgo. Used by c-shared wrapper and Go callers.
func (df *DataFrame) Show(chars, recordCount int) string {
	if df == nil {
		return ""
	}
	// clamp display width
	if chars <= 0 {
		chars = 25
	} else if chars < 5 {
		chars = 5
	}
	// clamp records
	records := df.Rows
	if recordCount > 0 && recordCount < records {
		records = recordCount
	}

	var b strings.Builder

	// headers
	for _, col := range df.Cols {
		name := col
		if len(name) > chars {
			name = name[:chars-3] + "..."
		}
		fmt.Fprintf(&b, "%-15s", name)
	}
	b.WriteString("\n")

	// rows
	for i := 0; i < records; i++ {
		for _, col := range df.Cols {
			if i >= len(df.Data[col]) {
				log.Fatalf("Show: index out of range: row %d, column %s", i, col)
			}
			val := df.Data[col][i]
			var s string
			switch v := val.(type) {
			case int:
				s = strconv.Itoa(v)
			case float64:
				s = strconv.FormatFloat(v, 'f', 2, 64)
			case bool:
				s = strconv.FormatBool(v)
			case string:
				s = v
			default:
				s = fmt.Sprintf("%v", v)
			}
			if len(s) > chars {
				fmt.Fprintf(&b, "%-15s", s[:chars-3]+"...")
			} else {
				fmt.Fprintf(&b, "%-15s", s)
			}
		}
		b.WriteString("\n")
	}
	fmt.Print(b.String())
	return b.String()
}

// HeadText returns a plain-text table of the first 5 rows.
func (df *DataFrame) Head(chars int) string {
	if df == nil {
		return ""
	}
	// clamp records to 5
	records := df.Rows
	if records > 5 {
		records = 5
	}
	// clamp display width
	if chars <= 0 {
		chars = 25
	} else if chars < 5 {
		chars = 5
	}

	var b strings.Builder

	// headers
	for _, col := range df.Cols {
		name := col
		if len(name) >= chars {
			name = name[:chars-3] + "..."
		}
		fmt.Fprintf(&b, "%-15s", name)
	}
	b.WriteString("\n")

	// rows
	for i := 0; i < records; i++ {
		for _, col := range df.Cols {
			values := df.Data[col]
			if i >= len(values) {
				fmt.Fprintf(&b, "%-15s", "<err>")
				continue
			}
			val := values[i]
			var s string
			switch v := val.(type) {
			case int:
				s = strconv.Itoa(v)
			case float64:
				s = strconv.FormatFloat(v, 'f', 2, 64)
			case bool:
				s = strconv.FormatBool(v)
			case string:
				s = v
			default:
				s = fmt.Sprintf("%v", v)
			}
			if len(s) > chars {
				s = s[:chars-3] + "..."
			}
			fmt.Fprintf(&b, "%-15s", s)
		}
		b.WriteString("\n")
	}
	fmt.Print(b.String())
	return b.String()
}

// Tail returns a plain-text table of the last 5 rows (or fewer if <5).
func (df *DataFrame) Tail(chars int) string {
	if df == nil || df.Rows == 0 {
		return ""
	}
	// clamp display width
	if chars <= 0 {
		chars = 25
	} else if chars < 5 {
		chars = 5
	}
	// number of rows to show
	records := df.Rows
	if records > 5 {
		records = 5
	}

	var b strings.Builder

	// headers
	for _, col := range df.Cols {
		name := col
		if len(name) >= chars {
			name = name[:chars-3] + "..."
		}
		fmt.Fprintf(&b, "%-15s", name)
	}
	b.WriteString("\n")

	// rows (last N)
	start := df.Rows - records
	for i := start; i < df.Rows; i++ {
		for _, col := range df.Cols {
			values := df.Data[col]
			var s string
			if i >= len(values) {
				s = "<err>"
			} else {
				switch v := values[i].(type) {
				case int:
					s = strconv.Itoa(v)
				case float64:
					s = strconv.FormatFloat(v, 'f', 2, 64)
				case bool:
					s = strconv.FormatBool(v)
				case string:
					s = v
				default:
					s = fmt.Sprintf("%v", v)
				}
			}
			if len(s) > chars {
				s = s[:chars-3] + "..."
			}
			fmt.Fprintf(&b, "%-15s", s)
		}
		b.WriteString("\n")
	}
	fmt.Print(b.String())
	return b.String()
}

// Vertical returns a vertically aligned string view of up to recordCount rows.
// Labels are padded to the max header width and values are truncated to 'chars'.
func (df *DataFrame) Vertical(chars int, recordCount int) string {
	if df == nil || df.Rows == 0 {
		return ""
	}
	// clamp display width
	if chars <= 0 {
		chars = 25
	} else if chars < 5 {
		chars = 5
	}
	// clamp records
	records := df.Rows
	if recordCount > 0 && recordCount < records {
		records = recordCount
	}

	// Precompute label display and max width (after truncation to chars)
	labels := make([]string, len(df.Cols))
	maxLabel := 0
	for i, col := range df.Cols {
		lab := col
		if len(lab) > chars {
			lab = lab[:chars-3] + "..."
		}
		labels[i] = lab
		if len(lab) > maxLabel {
			maxLabel = len(lab)
		}
	}

	var b strings.Builder
	for r := 0; r < records; r++ {
		fmt.Fprintf(&b, "------------ Record %d ------------\n", r)
		for i, col := range df.Cols {
			values := df.Data[col]
			var v string
			if r < len(values) {
				switch t := values[r].(type) {
				case int:
					v = strconv.Itoa(t)
				case int32:
					v = strconv.Itoa(int(t))
				case int64:
					v = strconv.FormatInt(t, 10)
				case float32:
					v = strconv.FormatFloat(float64(t), 'f', 2, 32)
				case float64:
					v = strconv.FormatFloat(t, 'f', 2, 64)
				case bool:
					v = strconv.FormatBool(t)
				case string:
					v = t
				default:
					v = fmt.Sprintf("%v", t)
				}
			} else {
				v = ""
			}
			if len(v) > chars {
				v = v[:chars]
			}
			fmt.Fprintf(&b, "%-*s : %s\n", maxLabel, labels[i], v)
		}
		b.WriteString("\n")
	}
	fmt.Print(b.String())
	return b.String()
}

func DisplayChart(chart Chart) map[string]interface{} {
	html := chart.Htmlpreid + chart.Htmldivid + chart.Htmlpostid + chart.Jspreid + chart.Htmldivid + chart.Jspostid
	return map[string]interface{}{
		"text/html": html,
	}

}

// DisplayHTML returns a value that gophernotes recognizes as rich HTML output.
func DisplayHTML(html string) map[string]interface{} {
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

// rowsJSONString marshals the DataFrame into an array of row objects (JSON).
func rowsJSONString(df *DataFrame) string {
    rows := make([]map[string]interface{}, df.Rows)
    for i := 0; i < df.Rows; i++ {
        row := make(map[string]interface{}, len(df.Cols))
        for _, col := range df.Cols {
            vals := df.Data[col]
            var v interface{}
            if i < len(vals) {
                v = vals[i]
            } else {
                v = nil
            }
            row[col] = v
        }
        rows[i] = row
    }
    b, _ := json.Marshal(rows)
    return string(b)
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
			<div id="app" style="text-align: center;" class=" h-screen pt-12">
				<button class="btn btn-sm fixed top-2 left-2 z-50" onclick="openInNewTab()">Open in Browser</button>
				<button class="btn btn-sm fixed top-2 right-2 z-50" @click="exportCSV()">Export to CSV</button>

 <!-- center, fixed; container ignores pointer events -->
 <div class="fixed top-2 left-1/2 -translate-x-1/2 z-50 pointer-events-none">
   <!-- only this inner group is clickable -->
   <div class="join pointer-events-auto">
      <div v-if="current_page > 1">
       <button class="btn btn-ghost btn-sm join-item" @click="first_page"><span class="material-symbols-outlined">first_page</span></button>
       <button class="btn btn-ghost btn-sm join-item" @click="prev_page"><span class="material-symbols-outlined">chevron_left</span></button>
      </div>

      <span v-if="pages <= 6" v-for="page in page_list" class="join">
        <button v-if="current_page === page" class="btn btn-sm btn-active no-animation join-item"><a href="#!">[[ current_page ]]</a></button>
        <button v-else class="btn btn-sm join-item" @click="pagefunc(page)"><a href="#!">[[ page ]]</a></button>
      </span>

      <span class="join" v-else>
       <span v-if="current_page > 3" class="btn btn-ghost btn-sm join-item pointer-events-none hover:bg-transparent focus:bg-transparent active:bg-transparent no-animation cursor-default select-none">…</span>
        <span v-for="page in page_list" class="join">
          <button v-if="current_page === page" class="btn btn-sm btn-active no-animation join-item"><a href="#!">[[ current_page ]]</a></button>
          <button v-else class="btn btn-sm join-item" @click="pagefunc(page)"><a href="#!">[[ page ]]</a></button>
        </span>
       <span v-if="current_page <= pages - 3" class="btn btn-ghost btn-sm join-item pointer-events-none hover:bg-transparent focus:bg-transparent active:bg-transparent no-animation cursor-default select-none">…</span>
      </span>

      <div v-if="current_page < pages">
       <button class="btn btn-ghost btn-sm join-item" @click="next_page"><span class="material-symbols-outlined">chevron_right</span></button>
       <button class="btn btn-ghost btn-sm join-item" @click="last_page"><span class="material-symbols-outlined">last_page</span></button>
      </div>
   </div>
 </div>			<!-- spacer to account for fixed toolbar height (~3rem) -->
			<!-- <div class="h-12"></div> -->
				<table class="table table-xs table-pin-rows w-full">
	  				<thead>
						<tr>
							<th class="sticky top-12 z-40 bg-base-100 p-2"></th>
						<th v-for="col in cols"  class="sticky top-12 z-40 bg-base-100 p-2"><div class="dropdown dropdown-hover"><div tabindex="0" role="button" class="btn btn-sm btn-ghost justify justify-start">[[ col ]]</div>
							<ul tabindex="0" class="dropdown-content menu bg-base-100 rounded-box z-[1] w-52 p-2 shadow">
								<li>
									<details closed>
									<summary class="btn-sm">Sort</summary>
									<ul>
										<li><a @click="sortColumnAsc(col)" class="flex justify-between items-center btn-sm">Ascending<span class="material-symbols-outlined">north</span></a></li>
										<li><a @click="sortColumnDesc(col)" class="flex justify-between items-center btn-sm">Descending<span class="material-symbols-outlined">south</span></a></li>
									</ul>
									</details>
								</li>
							</ul>
						</div></th>
						</tr>
					</thead>
					<tbody>
					<tr v-for="i in pageRowIndices" :key="i">
							<th class="pl-5">[[ i + 1 ]]</th>
							<td v-for="col in cols" :key="col" class="pl-5">[[ data[i]?.[col] ]]</td>
						</tr>
					</tbody>
				</table>
			</div>
		</body>
		<script type="module">
            // Use Blob + anchor; fallback to window.open write.
            function openInNewTab() {
                try {
                    const htmlContent = document.documentElement.outerHTML;
                    const blob = new Blob([htmlContent], { type: 'text/html' });
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.target = '_blank';
                    a.rel = 'noopener';
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                    setTimeout(() => URL.revokeObjectURL(url), 1000);
                } catch (e) {
                    const w = window.open('', '_blank');
                    if (!w) { alert('Popup blocked'); return; }
                    w.document.open();
                    w.document.write(document.documentElement.outerHTML);
                    w.document.close();
                }
            }

			const { createApp } = Vue
			import { Gophers } from 'https://cdn.jsdelivr.net/npm/gophers/gophers.js'
			createApp({
			delimiters : ['[[', ']]'],
				data(){
					return {
						cols: ` + QuoteArray(df.Cols) + `,
						data: ` + rowsJSONString(df) + `,
						selected_col: {},
						pages: 0,
						page_list: [],
						current_page: 1,
						pageSize: 50
					}
				},
				methods: {
				      openInNewTab() {
        const htmlContent = document.documentElement.outerHTML;
        const blob = new Blob([htmlContent], { type: 'text/html' });
        const url = URL.createObjectURL(blob);
        window.open(url, '_blank');
      },
      recomputePagination() {
        this.pages = Math.max(1, Math.ceil(this.rowCount / this.pageSize));
        if (this.current_page > this.pages) this.current_page = this.pages;
        if (this.current_page < 1) this.current_page = 1;
        this.updatePageList();
      },
      updatePageList() {
        const p = this.pages;
        const cur = this.current_page;
        if (p <= 6) {
          this.page_list = Array.from({length: p}, (_, i) => i + 1);
          return;
        }
        // sliding window of up to 6 pages
        let start = Math.max(1, cur - 2);
        let end = Math.min(p, start + 5);
        start = Math.max(1, end - 5);
        this.page_list = Array.from({length: end - start + 1}, (_, i) => start + i);
      },
      first_page(){ this.current_page = 1; },
      prev_page(){ if (this.current_page > 1) this.current_page -= 1; },
      pagefunc(page){ this.current_page = page; },
      next_page(){ if (this.current_page < this.pages) this.current_page += 1; },
      last_page(){ this.current_page = this.pages; },
					
       exportCSV() {
			const rows = Array.isArray(this.data) ? this.data : JSON.parse(
              typeof this.data === 'string' ? this.data : JSON.stringify(this.data)
            );
			var df = ReadJSON(this.data);        
            df.ToCSVFile('test_js_dataframe.csv');

       },					
	   sortColumnAsc(col) {
						// Create an array of row indices
						const rowIndices = Array.from({ length: this.data[col].length }, (_, i) => i);

						// Sort the row indices based on the values in the specified column (ascending)
						rowIndices.sort((a, b) => {
							if (this.data[col][a] < this.data[col][b]) return -1;
							if (this.data[col][a] > this.data[col][b]) return 1;
							return 0;
						});

						// Reorder all columns based on the sorted row indices
						for (const key in this.data) {
							this.data[key] = rowIndices.map(i => this.data[key][i]);
						}

						// Update the selected column
						this.selected_col = col;
					},
					sortColumnDesc(col) {
						// Create an array of row indices
						const rowIndices = Array.from({ length: this.data[col].length }, (_, i) => i);

						// Sort the row indices based on the values in the specified column (descending)
						rowIndices.sort((a, b) => {
							if (this.data[col][a] > this.data[col][b]) return -1;
							if (this.data[col][a] < this.data[col][b]) return 1;
							return 0;
						});

						// Reorder all columns based on the sorted row indices
						for (const key in this.data) {
							this.data[key] = rowIndices.map(i => this.data[key][i]);
						}

						// Update the selected column
						this.selected_col = col;
					}
				},
				watch: {
					rowCount() {
						// this.recomputePagination();
						return Array.isArray(this.data) ? this.data.length : 0;
					},
					pageSize() {
						this.recomputePagination();
					},
					current_page() {
						// keep page_list window centered
						this.updatePageList();
					}
				},
				created(){
					this.recomputePagination();

				},

				async mounted() {
                    const gophers = await Gophers();
                    Object.assign(globalThis, gophers);
				},
				computed:{
					// Max row count across all columns
					rowCount() {
						let rc = 0;
						for (const c of this.cols || []) {
						const len = (this.data[c] || []).length;
						if (len > rc) rc = len;
						}
						return rc;
					},
					// Indices for the current page
					pageRowIndices() {
						const start = (this.current_page - 1) * this.pageSize;
						const end = Math.min(start + this.pageSize, this.rowCount);
						const n = Math.max(end - start, 0);
						return Array.from({ length: n }, (_, i) => start + i);
					}				
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
			<div id="app" style="text-align: center;" class=" h-screen pt-12">
				<button class="btn btn-sm fixed top-2 left-2 z-50" onclick="openInNewTab()">Open in Browser</button>
				<button class="btn btn-sm fixed top-2 right-2 z-50" @click="exportCSV()">Export to CSV</button>

 <!-- center, fixed; container ignores pointer events -->
 <div class="fixed top-2 left-1/2 -translate-x-1/2 z-50 pointer-events-none">
   <!-- only this inner group is clickable -->
   <div class="join pointer-events-auto">
      <div v-if="current_page > 1">
       <button class="btn btn-ghost btn-sm join-item" @click="first_page"><span class="material-symbols-outlined">first_page</span></button>
       <button class="btn btn-ghost btn-sm join-item" @click="prev_page"><span class="material-symbols-outlined">chevron_left</span></button>
      </div>

      <span v-if="pages <= 6" v-for="page in page_list" class="join">
        <button v-if="current_page === page" class="btn btn-sm btn-active no-animation join-item"><a href="#!">[[ current_page ]]</a></button>
        <button v-else class="btn btn-sm join-item" @click="pagefunc(page)"><a href="#!">[[ page ]]</a></button>
      </span>

      <span class="join" v-else>
       <span v-if="current_page > 3" class="btn btn-ghost btn-sm join-item pointer-events-none hover:bg-transparent focus:bg-transparent active:bg-transparent no-animation cursor-default select-none">…</span>
        <span v-for="page in page_list" class="join">
          <button v-if="current_page === page" class="btn btn-sm btn-active no-animation join-item"><a href="#!">[[ current_page ]]</a></button>
          <button v-else class="btn btn-sm join-item" @click="pagefunc(page)"><a href="#!">[[ page ]]</a></button>
        </span>
       <span v-if="current_page <= pages - 3" class="btn btn-ghost btn-sm join-item pointer-events-none hover:bg-transparent focus:bg-transparent active:bg-transparent no-animation cursor-default select-none">…</span>
      </span>

      <div v-if="current_page < pages">
       <button class="btn btn-ghost btn-sm join-item" @click="next_page"><span class="material-symbols-outlined">chevron_right</span></button>
       <button class="btn btn-ghost btn-sm join-item" @click="last_page"><span class="material-symbols-outlined">last_page</span></button>
      </div>
   </div>
 </div>			<!-- spacer to account for fixed toolbar height (~3rem) -->
			<!-- <div class="h-12"></div> -->
				<table class="table table-xs table-pin-rows w-full">
	  				<thead>
						<tr>
							<th class="sticky top-12 z-40 bg-base-100 p-2"></th>
						<th v-for="col in cols"  class="sticky top-12 z-40 bg-base-100 p-2"><div class="dropdown dropdown-hover"><div tabindex="0" role="button" class="btn btn-sm btn-ghost justify justify-start">[[ col ]]</div>
							<ul tabindex="0" class="dropdown-content menu bg-base-100 rounded-box z-[1] w-52 p-2 shadow">
								<li>
									<details closed>
									<summary class="btn-sm">Sort</summary>
									<ul>
										<li><a @click="sortColumnAsc(col)" class="flex justify-between items-center btn-sm">Ascending<span class="material-symbols-outlined">north</span></a></li>
										<li><a @click="sortColumnDesc(col)" class="flex justify-between items-center btn-sm">Descending<span class="material-symbols-outlined">south</span></a></li>
									</ul>
									</details>
								</li>
							</ul>
						</div></th>
						</tr>
					</thead>
					<tbody>
					<tr v-for="i in pageRowIndices" :key="i">
							<th class="pl-5">[[ i + 1 ]]</th>
							<td v-for="col in cols" :key="col" class="pl-5">[[ data[i]?.[col] ]]</td>
						</tr>
					</tbody>
				</table>
			</div>
		</body>
		<script type="module">
            // Use Blob + anchor; fallback to window.open write.
            function openInNewTab() {
                try {
                    const htmlContent = document.documentElement.outerHTML;
                    const blob = new Blob([htmlContent], { type: 'text/html' });
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.target = '_blank';
                    a.rel = 'noopener';
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                    setTimeout(() => URL.revokeObjectURL(url), 1000);
                } catch (e) {
                    const w = window.open('', '_blank');
                    if (!w) { alert('Popup blocked'); return; }
                    w.document.open();
                    w.document.write(document.documentElement.outerHTML);
                    w.document.close();
                }
            }
			const { createApp } = Vue
            import { Gophers } from 'https://cdn.jsdelivr.net/npm/gophers/gophers.js'
			createApp({
			delimiters : ['[[', ']]'],
				data(){
					return {
						cols: ` + QuoteArray(df.Cols) + `,
						data: ` + rowsJSONString(df) + `,
						selected_col: {},
						pages: 0,
						page_list: [],
						current_page: 1,
						pageSize: 50
					}
				},
				methods: {
      recomputePagination() {
        this.pages = Math.max(1, Math.ceil(this.rowCount / this.pageSize));
        if (this.current_page > this.pages) this.current_page = this.pages;
        if (this.current_page < 1) this.current_page = 1;
        this.updatePageList();
      },
      updatePageList() {
        const p = this.pages;
        const cur = this.current_page;
        if (p <= 6) {
          this.page_list = Array.from({length: p}, (_, i) => i + 1);
          return;
        }
        // sliding window of up to 6 pages
        let start = Math.max(1, cur - 2);
        let end = Math.min(p, start + 5);
        start = Math.max(1, end - 5);
        this.page_list = Array.from({length: end - start + 1}, (_, i) => start + i);
      },
      first_page(){ this.current_page = 1; },
      prev_page(){ if (this.current_page > 1) this.current_page -= 1; },
      pagefunc(page){ this.current_page = page; },
      next_page(){ if (this.current_page < this.pages) this.current_page += 1; },
      last_page(){ this.current_page = this.pages; },
					
       exportCSV() {
			const rows = Array.isArray(this.data) ? this.data : JSON.parse(
              typeof this.data === 'string' ? this.data : JSON.stringify(this.data)
            );
			var df = ReadJSON(this.data);        
            df.ToCSVFile('test_js_dataframe.csv');

       },					
	   sortColumnAsc(col) {
						// Create an array of row indices
						const rowIndices = Array.from({ length: this.data[col].length }, (_, i) => i);

						// Sort the row indices based on the values in the specified column (ascending)
						rowIndices.sort((a, b) => {
							if (this.data[col][a] < this.data[col][b]) return -1;
							if (this.data[col][a] > this.data[col][b]) return 1;
							return 0;
						});

						// Reorder all columns based on the sorted row indices
						for (const key in this.data) {
							this.data[key] = rowIndices.map(i => this.data[key][i]);
						}

						// Update the selected column
						this.selected_col = col;
					},
					sortColumnDesc(col) {
						// Create an array of row indices
						const rowIndices = Array.from({ length: this.data[col].length }, (_, i) => i);

						// Sort the row indices based on the values in the specified column (descending)
						rowIndices.sort((a, b) => {
							if (this.data[col][a] > this.data[col][b]) return -1;
							if (this.data[col][a] < this.data[col][b]) return 1;
							return 0;
						});

						// Reorder all columns based on the sorted row indices
						for (const key in this.data) {
							this.data[key] = rowIndices.map(i => this.data[key][i]);
						}

						// Update the selected column
						this.selected_col = col;
					}
				},
				watch: {
					rowCount() {
						// this.recomputePagination();
						return Array.isArray(this.data) ? this.data.length : 0;
					},
					pageSize() {
						this.recomputePagination();
					},
					current_page() {
						// keep page_list window centered
						this.updatePageList();
					}
				},
				created(){
					this.recomputePagination();

				},

				async mounted() {
                    const gophers = await Gophers();
                    Object.assign(globalThis, gophers);

				},
				computed:{
					// Max row count across all columns
					rowCount() {
						let rc = 0;
						for (const c of this.cols || []) {
						const len = (this.data[c] || []).length;
						if (len > rc) rc = len;
						}
						return rc;
					},
					// Indices for the current page
					pageRowIndices() {
						const start = (this.current_page - 1) * this.pageSize;
						const end = Math.min(start + this.pageSize, this.rowCount);
						const n = Math.max(end - start, 0);
						return Array.from({ length: n }, (_, i) => start + i);
					}				
				}

			}).mount('#app')
		</script>
	</html>
	
`
	return map[string]interface{}{
		"text/html": html,
	}
}

// write an html display, chart, or report to a file
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
