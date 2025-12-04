package gophers

import (
	"fmt"
	"html"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

func (report *Report) init() {
	if report.Pageshtml == nil {
		report.Pageshtml = make(map[string]map[string]string)
	}
	if report.Pagesjs == nil {
		report.Pagesjs = make(map[string]map[string]string)
	}
}

// report create
func CreateReport(title string) *Report {
	HTMLTop := `
	<!DOCTYPE html>
	<html>
		<head>
			<script>
			tailwind.config = {
				theme: {
				extend: {
					colors: {`
	HTMLHeading := `	
				}
			}
		}
		</script>
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

	`
	ScriptHeading := `
			</div>
		</div>
	</body>
	<script>
		const { createApp } = Vue
		createApp({
		delimiters : ['[[', ']]'],
			data(){
				return {
					page: `

	ScriptMiddle := `
          }
        },
        methods: {

        },
        watch: {

        },
        created(){
		},
		  mounted() {
`

	HTMLBottom := `
        },
        computed:{

        }

    }).mount('#app')
  </script>
</html>
`

	newReport := &Report{
		Top:           HTMLTop,
		Primary:       `primary: "#0000ff",`,
		Secondary:     `secondary: "#00aaff",`,
		Accent:        `accent: "#479700",`,
		Neutral:       `neutral: "#250e0d",`,
		Base100:       `"base-100": "#fffaff",`,
		Info:          `info: "#00c8ff",`,
		Success:       `success: "#00ec6a",`,
		Warning:       `warning: "#ffb900",`,
		Err:           `error: "#f00027",`,
		Htmlheading:   HTMLHeading,
		Title:         title,
		Htmlelements:  "",
		Scriptheading: ScriptHeading,
		Scriptmiddle:  ScriptMiddle,
		Bottom:        HTMLBottom,
		Pageshtml:     make(map[string]map[string]string),
		Pagesjs:       make(map[string]map[string]string),
	}
	// fmt.Println("CreateReport: Initialized report:", newReport)
	return newReport
}


// Primary - set the primary color of the report
func (report *Report) Primary(primary string) error {
	report.Primary = fmt.Sprintf(`primary: "%s",`, primary)
	return nil
}

// Secondary - set the secondary color of the report
func (report *Report) Secondary(secondary string) error {
	report.Secondary = fmt.Sprintf(`secondary: "%s",`, secondary)
	return nil
}

// Accent - set the accent color of the report
func (report *Report) Accent(accent string) error {
	report.Accent = fmt.Sprintf(`accent: "%s",`, accent	)
	return nil
}

// Neutral - set the neutral color of the report
func (report *Report) Neutral(neutral string) error {
	report.Neutral = fmt.Sprintf(`neutral: "%s",`, neutral	)
	return nil
}

// Base100 - set the base100 color of the report
func (report *Report) Base100(base100 string) error {
	report.Base100 = fmt.Sprintf(`"base-100": "%s",`, base100		)
	return nil
}

// Info - set the info color of the report
func (report *Report) Info(info string) error {
	report.Info = fmt.Sprintf(`info: "%s",`, info	)
	return nil
}

// Success - set the success color of the report
func (report *Report) Success(success string) error {
	report.Success = fmt.Sprintf(`success: "%s",`, success	)
	return nil
}

// Warning - set the warning color of the report
func (report *Report) Warning(warning string) error {
	report.Warning = fmt.Sprintf(`warning: "%s",`, warning	)
	return nil
}

// Err - set the error color of the report
func (report *Report) Err(err string) error {
	report.Err = fmt.Sprintf(`error: "%s",`, err	)
	return nil
}

// Open - open the report in browser
func (report *Report) Open() error {
	// add html element for page
	html := report.Top +
		report.Primary +
		report.Secondary +
		report.Accent +
		report.Neutral +
		report.Base100 +
		report.Info +
		report.Success +
		report.Warning +
		report.Err +
		report.Htmlheading
	if len(report.Pageshtml) > 1 {
		html += `
        <div id="app"  style="text-align: center;" class="drawer w-full lg:drawer-open">
            <input id="my-drawer-2" type="checkbox" class="drawer-toggle" />
            <div class="drawer-content flex flex-col">
                <!-- Navbar -->
                <div class="w-full navbar bg-neutral text-neutral-content shadow-lg ">
            ` +
			fmt.Sprintf(`<div class="flex-1 px-2 mx-2 btn btn-sm btn-neutral normal-case text-xl shadow-none hover:bg-neutral hover:border-neutral flex content-center"><a class="lg:ml-0 ml-14 text-4xl">%s</a></div>`, report.Title) +
			`
                <div class="flex-none lg:hidden">
                    <label for="my-drawer-2" class="btn btn-neutral btn-square shadow-lg hover:shadow-xl hover:-translate-y-0.5 no-animation">
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"
                        class="inline-block w-6 h-6 stroke-current">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"></path>
                    </svg>
                    </label>
                    </div>
                </div>
                <!-- content goes here! -->
                <div  class="w-full lg:w-3/4 md:w-3/4 sm:w-5/6 mx-auto flex-col justify-self-center">
            `

	} else {
		html += `
        <div id="app"  style="text-align: center;">
            <!-- Navbar -->
            <div class="w-full navbar bg-neutral text-neutral-content shadow-lg ">
        ` +
			fmt.Sprintf(`<div class="flex-1 px-2 mx-2 btn btn-sm btn-neutral normal-case text-xl shadow-none hover:bg-neutral hover:border-neutral flex content-center"><a class=" text-4xl">%s</a></div>
            </div>`, report.Title) +
			`<div  class="w-full lg:w-3/4 md:w-3/4 sm:w-5/6 mx-auto flex-col justify-self-center">`

	}
	// iterate over pageshtml and add each stored HTML snippet
	for _, pageMap := range report.Pageshtml {
		// iterate in order
		// fmt.Println(pageMap)
		for i := 0; i < len(pageMap); i++ {
			html += pageMap[strconv.Itoa(i)]
		}
	}
	if len(report.Pageshtml) > 1 {
		html += `
            </div>
        </div>
        <!-- <br> -->
        <div class="drawer-side">
            <label for="my-drawer-2" class="drawer-overlay bg-neutral"></label>
            <ul class="menu p-4 w-80 bg-neutral h-full overflow-y-auto min-h-screen text-base-content shadow-none space-y-2 ">
            <div class="card w-72 bg-base-100 shadow-xl">
                <div class="card-body">
                    <div class="flex space-x-6 place-content-center">
                        <h2 class="card-title black-text-shadow-sm flex justify">Pages</h2>
                    </div>
                <div class="flex flex-col w-full h-1px">
                    <div class="divider"></div>
                </div>
                <div class="space-y-4">
        `
		for page, _ := range report.Pageshtml {
			html += fmt.Sprintf(`
            <button v-if="page == '%s' " @click="page = '%s' " class="btn btn-block btn-sm btn-neutral text-white bg-neutral shadow-lg  hover:shadow-xl hover:-translate-y-0.5 no-animation " >%s</button>
            <button v-else @click="page = '%s' " class="btn btn-block btn-sm bg-base-100 btn-outline btn-neutral hover:text-white shadow-lg hover:shadow-xl hover:-translate-y-0.5 no-animation " >%s</button>
            
            `, page, page, page, page, page)
		}
	} else {
		html += `
            </div>
        </div>
        `
	}
	html += report.Scriptheading
	pages := `pages: [`
	count := 0
	for page, _ := range report.Pageshtml {
		if count == 0 {
			html += fmt.Sprintf("%q", page) + ","
		}
		pages += fmt.Sprintf("%q", page) + ", "
		count++
	}
	pages = strings.TrimSuffix(pages, ", ") + `],`
	html += pages
	html += report.Scriptmiddle
	// iterate over pagesjs similarly
	for _, jsMap := range report.Pagesjs {
		// fmt.Println("printing jsMap")
		// fmt.Println(jsMap)
		for i := 0; i < len(jsMap); i++ {
			html += jsMap[strconv.Itoa(i)]
		}
	}

	html += report.Bottom
	// fmt.Println("printing html:")
	// fmt.Println(html)
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

// Save - save report to html file
func (report *Report) Save(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// add html element for page
	html := report.Top +
		report.Primary +
		report.Secondary +
		report.Accent +
		report.Neutral +
		report.Base100 +
		report.Info +
		report.Success +
		report.Warning +
		report.Err +
		report.Htmlheading

	if len(report.Pageshtml) > 1 {
		html += `
		<div id="app"  style="text-align: center;" class="drawer w-full lg:drawer-open">
			<input id="my-drawer-2" type="checkbox" class="drawer-toggle" />
			<div class="drawer-content flex flex-col">
				<!-- Navbar -->
				<div class="w-full navbar bg-neutral text-neutral-content shadow-lg ">
			` +
			fmt.Sprintf(`<div class="flex-1 px-2 mx-2 btn btn-sm btn-neutral normal-case text-xl shadow-none hover:bg-neutral hover:border-neutral flex content-center"><a class="lg:ml-0 ml-14 text-4xl">%s</a></div>`, report.Title) +
			`
				<div class="flex-none lg:hidden">
					<label for="my-drawer-2" class="btn btn-neutral btn-square shadow-lg hover:shadow-xl hover:-translate-y-0.5 no-animation">
					<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"
						class="inline-block w-6 h-6 stroke-current">
						<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"></path>
					</svg>
					</label>
					</div>
				</div>
				<!-- content goes here! -->
				<div  class="w-full lg:w-3/4 md:w-3/4 sm:w-5/6 mx-auto flex-col justify-self-center">
			`

	} else {
		html += `
		<div id="app"  style="text-align: center;">
			<!-- Navbar -->
			<div class="w-full navbar bg-neutral text-neutral-content shadow-lg ">
		` +
			fmt.Sprintf(`<div class="flex-1 px-2 mx-2 btn btn-sm btn-neutral normal-case text-xl shadow-none hover:bg-neutral hover:border-neutral flex content-center"><a class=" text-4xl">%s</a></div>
			</div>`, report.Title) +
			`<div  class="w-full lg:w-3/4 md:w-3/4 sm:w-5/6 mx-auto flex-col justify-self-center">`

	}
	// iterate over pageshtml and add each stored HTML snippet
	for _, pageMap := range report.Pageshtml {
		// iterate in order
		// fmt.Println(pageMap)
		for i := 0; i < len(pageMap); i++ {
			html += pageMap[strconv.Itoa(i)]
		}
	}
	if len(report.Pageshtml) > 1 {
		html += `
			</div>
		</div>
		<!-- <br> -->
		<div class="drawer-side">
			<label for="my-drawer-2" class="drawer-overlay bg-neutral"></label>
			<ul class="menu p-4 w-80 bg-neutral h-full overflow-y-auto min-h-screen text-base-content shadow-none space-y-2 ">
			<div class="card w-72 bg-base-100 shadow-xl">
				<div class="card-body">
					<div class="flex space-x-6 place-content-center">
						<h2 class="card-title black-text-shadow-sm flex justify">Pages</h2>
					</div>
				<div class="flex flex-col w-full h-1px">
					<div class="divider"></div>
				</div>
				<div class="space-y-4">
		`
		for page, _ := range report.Pageshtml {
			html += fmt.Sprintf(`
			<button v-if="page == '%s' " @click="page = '%s' " class="btn btn-block btn-sm btn-neutral text-white bg-neutral shadow-lg  hover:shadow-xl hover:-translate-y-0.5 no-animation " >%s</button>
			<button v-else @click="page = '%s' " class="btn btn-block btn-sm bg-base-100 btn-outline btn-neutral hover:text-white shadow-lg hover:shadow-xl hover:-translate-y-0.5 no-animation " >%s</button>
			
			`, page, page, page, page, page)
		}
	} else {
		html += `
			</div>
		</div>
		`
	}
	html += report.Scriptheading
	pages := `pages: [`
	count := 0
	for page, _ := range report.Pageshtml {
		if count == 0 {
			html += fmt.Sprintf("%q", page) + ","
		}
		pages += fmt.Sprintf("%q", page) + ", "
		count++
	}
	pages = strings.TrimSuffix(pages, ", ") + `],`
	html += pages
	html += report.Scriptmiddle
	// iterate over pagesjs similarly
	for _, jsMap := range report.Pagesjs {
		// fmt.Println("printing jsMap")
		// fmt.Println(jsMap)
		for i := 0; i < len(jsMap); i++ {
			html += jsMap[strconv.Itoa(i)]
		}
	}

	html += report.Bottom

	// Write the HTML string to the file
	if _, err := file.Write([]byte(html)); err != nil {
		return fmt.Errorf("failed to write to file: %v", err)
	}

	return nil
}

// AddPage adds a new page to the report.
func (report *Report) AddPage(name string) {
	report.init() // Ensure maps are initialized

	// Check if the page already exists.
	if _, exists := report.Pageshtml[name]; !exists {
		report.Pageshtml[name] = make(map[string]string)
	}
	if _, exists := report.Pagesjs[name]; !exists {
		report.Pagesjs[name] = make(map[string]string)
	}

	html := `<h1 v-if="page == '` + name + `' " class="text-8xl pt-24 pb-24"> ` + name + `</h1>` // Page Title at top of page
	report.Pageshtml[name][strconv.Itoa(len(report.Pageshtml[name]))] = html

	// fmt.Println("AddPage: Added page:", name)
	// fmt.Println("AddPage: Updated pageshtml:", report.Pageshtml)
}

// spacing for stuff? card or no card? background?

// add text input

// add slider

// add dropdown (array of selections) - adds variable and updates charts + uses in new charts...

// add iframe
// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (report *Report) AddHTML(page string, text string) {
	report.init() // Ensure maps are initialized

	// Check if the page exists
	if _, exists := report.Pageshtml[page]; !exists {
		report.Pageshtml[page] = make(map[string]string)
	}
	escapedtext := html.EscapeString(text)
	// println(escapedtext)
	texthtml := `<iframe v-if="page == '` + page + `' " class="p-8 flex justify-self-center w-full h-screen" sandbox="allow-scripts allow-popups allow-downloads allow-top-navigation-by-user-activation" srcdoc='` + escapedtext + `'></iframe>`
	// texthtml := `<div v-if="page == '` + page + `' " class="p-8 flex justify-self-center w-full h-screen" >` + escapedtext + `</div>`

	report.Pageshtml[page][strconv.Itoa(len(report.Pageshtml[page]))] = texthtml

	// fmt.Println("AddHTML: Added HTML to page:", page)
	// fmt.Println("AddHTML: Updated pageshtml:", report.Pageshtml)
}

// add df (paginate + filter + sort)
func (report *Report) AddDataframe(page string, df *DataFrame) {
	text := df.Display()["text/html"].(string)
	// add html to page map
	if _, exists := report.Pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPage()")
		return
	}
	report.AddHTML(page, text)
}

// // AddDataframe embeds the DataFrame HTML directly on the page (no iframe)
// // to prevent navigation inside an iframe that reloads the entire app.
// func (report *Report) AddDataframe(page string, df *DataFrame) {
// 	report.init()
// 	if _, exists := report.Pageshtml[page]; !exists {
// 		fmt.Println("Page does not exist. Use AddPage()")
// 		return
// 	}
// 	htmlSnippetIface := df.Display()
// 	tableHTML, ok := htmlSnippetIface["text/html"].(string)
// 	if !ok {
// 		fmt.Println("AddDataframe: invalid HTML content")
// 		return
// 	}
// 	fmt.Println(tableHTML)
// 	// Wrap in page conditional container.
// 	wrapped := `<div v-if="page == '` + page + `'">` + tableHTML + `</div>`
// 	report.Pageshtml[page][strconv.Itoa(len(report.Pageshtml[page]))] = wrapped
// }

// AddChart adds a chart to the specified page in the report.
func (report *Report) AddChart(page string, chart Chart) {
	report.init() // Ensure maps are initialized

	// Check if the page exists
	if _, exists := report.Pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPAge().")
		return
	}
	if _, exists := report.Pagesjs[page]; !exists {
		fmt.Println("Page content does not exist.")
		return
	}

	idhtml := strconv.Itoa(len(report.Pageshtml[page]))
	chartId := chart.Htmldivid + idhtml
	idjs := strconv.Itoa(len(report.Pagesjs[page]))

	if chart.Htmlpostid == "" {
		chart.Htmlpostid = ` class="flex justify-center mx-auto p-4"></div>`
	}

	html := fmt.Sprintf(`<div v-show="page == '%s'" id="%s"%s`, page, chartId, chart.Htmlpostid)
	js := fmt.Sprintf(`%s%s%s`, chart.Jspreid, chartId, chart.Jspostid)

	report.Pageshtml[page][idhtml] = html
	report.Pagesjs[page][idjs] = js

	// fmt.Println("DASH:", report.Pageshtml)
	// fmt.Printf("AddChart: Added chart to page %s at index %s\n", page, idhtml)
	// fmt.Println("AddChart: Updated pageshtml:", report.Pageshtml)
	// fmt.Println("AddChart: Updated pagesjs:", report.Pagesjs)
}

// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
// AddHeading adds a heading to the specified page in the report.
func (report *Report) AddHeading(page string, heading string, size int) {
	report.init() // Ensure maps are initialized

	// Check if the page exists
	if _, exists := report.Pageshtml[page]; !exists {
		report.Pageshtml[page] = make(map[string]string)
	}

	var text_size string
	switch size {
	case 1:
		text_size = "text-6xl"
	case 2:
		text_size = "text-5xl"
	case 3:
		text_size = "text-4xl"
	case 4:
		text_size = "text-3xl"
	case 5:
		text_size = "text-2xl"
	case 6:
		text_size = "text-xl"
	case 7:
		text_size = "text-lg"
	case 8:
		text_size = "text-md"
	case 9:
		text_size = "text-sm"
	case 10:
		text_size = "text-xs"
	default:
		text_size = "text-md"
	}

	html := `<h1 v-if="page == '` + page + fmt.Sprintf(`' " class="%s p-8 flex justify-start"> `, text_size) + heading + `</h1>`
	report.Pageshtml[page][strconv.Itoa(len(report.Pageshtml[page]))] = html

	// fmt.Printf("AddHeading: Added heading to page %s with size %d\n", page, size)
	// fmt.Println("AddHeading: Updated pageshtml:", report.Pageshtml)
}

// AddText function fix
func (report *Report) AddText(page string, text string) {
	report.init() // Ensure maps are initialized

	// Check if the page exists
	if _, exists := report.Pageshtml[page]; !exists {
		report.Pageshtml[page] = make(map[string]string)
	}

	text_size := "text-md"
	html := `<h1 v-if="page == '` + page + fmt.Sprintf(`' " class="%s pl-12 pr-12 flex justify-start text-left"> `, text_size) + text + `</h1>`
	idx := strconv.Itoa(len(report.Pageshtml[page]))
	report.Pageshtml[page][idx] = html

	// fmt.Printf("AddText: Added text to page %s at index %s\n", page, idx)
	// fmt.Println("AddText: Updated pageshtml:", report.Pageshtml)
}

// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (report *Report) AddSubText(page string, text string) {
	report.init() // Ensure maps are initialized

	// Check if the page exists
	if _, exists := report.Pageshtml[page]; !exists {
		report.Pageshtml[page] = make(map[string]string)
	}

	text_size := "text-sm"
	html := `<h1 v-if="page == '` + page + fmt.Sprintf(`' " class="%s pl-12 pr-12 pb-8 flex justify-center"> `, text_size) + text + `</h1>`
	report.Pageshtml[page][strconv.Itoa(len(report.Pageshtml[page]))] = html

	fmt.Println("AddSubText: Added subtext to page:", page)
	fmt.Println("AddSubText: Updated pageshtml:", report.Pageshtml)
}

// add bullet list
// add html to page map
// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (report *Report) AddBullets(page string, text ...string) {

	// Check if the page exists
	if _, exists := report.Pageshtml[page]; !exists {
		report.Pageshtml[page] = make(map[string]string)
	}
	text_size := "text-md"
	html := `<ul v-if="page == '` + page + `' " class="list-disc flex-col justify-self-start pl-24 pr-12 py-2"> `
	for _, bullet := range text {
		html += fmt.Sprintf(`<li class="text-left %s">`, text_size) + bullet + `</li>`
	}
	html += `</ul>`
	report.Pageshtml[page][strconv.Itoa(len(report.Pageshtml[page]))] = html

	fmt.Println("AddBullets: Added bullets to page:", page)
	fmt.Println("AddBullets: Updated pageshtml:", report.Pageshtml)

}
