package gophers

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// main dashboard object for adding html pages, charts, and inputs for a single html output
type Dashboard struct {
	top           string
	primary       string
	secondary     string
	accent        string
	neutral       string
	base100       string
	info          string
	success       string
	warning       string
	err           string
	htmlheading   string
	title         string
	htmlelements  string
	scriptheading string
	scriptmiddle  string
	bottom        string
	pageshtml     map[string]interface{}
	pagesjs       map[string]interface{}
}

// dashboard create
func CreateDashboard(title string) *Dashboard {
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

	newDashboard := &Dashboard{
		top:           HTMLTop,
		primary:       `primary: "#0000ff",`,
		secondary:     `secondary: "#00aaff",`,
		accent:        `accent: "#479700",`,
		neutral:       `neutral: "#250e0d",`,
		base100:       `"base-100": "#fffaff",`,
		info:          `info: "#00c8ff",`,
		success:       `success: "#00ec6a",`,
		warning:       `warning: "#ffb900",`,
		err:           `error: "#f00027",`,
		htmlheading:   HTMLHeading,
		title:         title,
		htmlelements:  "",
		scriptheading: ScriptHeading,
		scriptmiddle:  ScriptMiddle,
		bottom:        HTMLBottom,
		pageshtml:     make(map[string]interface{}),
		pagesjs:       make(map[string]interface{}),
	}

	return newDashboard
}

// Open - open the dashboard in browser
func (dash *Dashboard) Open() error {
	// add html element for page
	html := dash.top +
		dash.primary +
		dash.secondary +
		dash.accent +
		dash.neutral +
		dash.base100 +
		dash.info +
		dash.success +
		dash.warning +
		dash.err +
		dash.htmlheading
	if len(dash.pageshtml) > 1 {
		html += fmt.Sprintf(`
		<div id="app"  style="text-align: center;" class="drawer w-full lg:drawer-open">
			<input id="my-drawer-2" type="checkbox" class="drawer-toggle" />
			<div class="drawer-content flex flex-col">
				<!-- Navbar -->
				<div class="w-full navbar bg-neutral text-neutral-content shadow-lg ">
			`) +
			fmt.Sprintf(`<div class="flex-1 px-2 mx-2 btn btn-sm btn-neutral normal-case text-xl shadow-none hover:bg-neutral hover:border-neutral flex content-center"><a class="ml-14 text-4xl">%s</a></div>`, dash.title) +
			fmt.Sprintf(`
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
			`)

	} else {
		html += fmt.Sprintf(`
		<div id="app"  style="text-align: center;">
			<!-- Navbar -->
			<div class="w-full navbar bg-neutral text-neutral-content shadow-lg ">
		`) +
			fmt.Sprintf(`<div class="flex-1 px-2 mx-2 btn btn-sm btn-neutral normal-case text-xl shadow-none hover:bg-neutral hover:border-neutral flex content-center"><a class=" text-4xl">%s</a></div>
			</div>`, dash.title) +
			fmt.Sprintf(`<div  class="w-full lg:w-3/4 md:w-3/4 sm:w-5/6 mx-auto flex-col justify-self-center">`)

	}
	// iterate over pageshtml and add each stored HTML snippet
	for _, elem := range dash.pageshtml {
		if pageMap, ok := elem.(map[int]string); ok {
			// iterate in order
			for i := 0; i < len(pageMap); i++ {
				html += pageMap[i]
			}
		} else {
			html += fmt.Sprintf("%v", elem)
		}
	}
	if len(dash.pageshtml) > 1 {
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
		for page, _ := range dash.pageshtml {
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
	html += dash.scriptheading
	pages := `pages: [`
	count := 0
	for page, _ := range dash.pageshtml {
		if count == 0 {
			html += fmt.Sprintf("%q", page) + ","
		}
		pages += fmt.Sprintf("%q", page) + ", "
		count++
	}
	pages = strings.TrimSuffix(pages, ", ") + `],`
	html += pages
	html += dash.scriptmiddle
	// iterate over pagesjs similarly
	for _, elem := range dash.pagesjs {
		if jsMap, ok := elem.(map[int]string); ok {
			for i := 0; i < len(jsMap); i++ {
				html += jsMap[i]
			}
		} else {
			html += fmt.Sprintf("%v", elem)
		}
	}
	html += dash.bottom

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

// Save - save dashboard to html file
func (dash *Dashboard) Save(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// add html element for page
	html := dash.top +
		dash.primary +
		dash.secondary +
		dash.accent +
		dash.neutral +
		dash.base100 +
		dash.info +
		dash.success +
		dash.warning +
		dash.err +
		dash.htmlheading

	// iterate over pageshtml and add each stored HTML snippet
	for _, elem := range dash.pageshtml {
		if pageMap, ok := elem.(map[int]string); ok {
			// iterate in order
			for i := 0; i < len(pageMap); i++ {
				html += pageMap[i]
			}
		} else {
			html += fmt.Sprintf("%v", elem)
		}
	}
	html += dash.scriptheading
	pages := `pages: [`
	count := 0
	for page, _ := range dash.pageshtml {
		if count == 0 {
			html += fmt.Sprintf("%q", page) + ","
			count++
		}
		pages += fmt.Sprintf("%q", page) + ", "
	}
	pages = strings.TrimSuffix(pages, ", ") + `],`
	html += pages
	html += dash.scriptmiddle
	// iterate over pagesjs similarly
	for _, elem := range dash.pagesjs {
		if jsMap, ok := elem.(map[int]string); ok {
			for i := 0; i < len(jsMap); i++ {
				html += jsMap[i]
			}
		} else {
			html += fmt.Sprintf("%v", elem)
		}
	}
	html += dash.bottom

	// Write the HTML string to the file
	if _, err := file.Write([]byte(html)); err != nil {
		return fmt.Errorf("failed to write to file: %v", err)
	}

	return nil
}

// create page & add a column
func (dash *Dashboard) AddPage(name string) {
	// check if already exists
	if _, exists := dash.pageshtml[name]; exists {
		fmt.Println("Page already exists.")
		return
	}
	// add page to dashboard type
	dash.pageshtml[name] = map[int]string{}
	// Assert the type of the value in pageshtml and pagesjs
	pageHTML, ok := dash.pageshtml[name].(map[int]string)
	if !ok {
		fmt.Println("Invalid type for pageshtml[page]")
		return
	}
	html := `<h1 v-if="page == '` + name + `' " class="text-8xl pt-24 pb-24"> ` + name + `</h1>` // Page Title at top of page
	pageHTML[len(pageHTML)] = html
	// fmt.Println(dash.pageshtml)
	dash.pagesjs[name] = map[int]string{}
	// fmt.Println("Page added successfully: "+name)
	// for page, _ := range dash.pageshtml {
	// 	fmt.Println(page)
	// }
}

// spacing for stuff? card or no card? background?

// add text input

// add slider

// add dropdown (array of selections)

// add iframe
// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (dash *Dashboard) AddHTML(page string, text string) {
	// add html to page map
	if _, exists := dash.pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPage()")
		return
	}
	// Assert the type of the value in pageshtml and pagesjs
	pageHTML, ok := dash.pageshtml[page].(map[int]string)
	if !ok {
		fmt.Println("Invalid type for pageshtml[page]")
		return
	}
	texthtml := `<iframe v-if="page == '` + page + `' " class="p-8 flex justify-self-center sm:w-7/8 w-3/4" srcdoc='` + text + `'></iframe>`
	pageHTML[len(pageHTML)] = texthtml
	// Update the maps with the new values
	dash.pageshtml[page] = pageHTML

}

// add df (paginate + filter + sort)
func (dash *Dashboard) AddDataframe(page string, df *DataFrame) {
	text := df.Display()["text/html"].(string)
	// add html to page map
	if _, exists := dash.pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPage()")
		return
	}
	dash.AddHTML("page2", text)
}

// add chart to dashboard page
func (dash *Dashboard) AddChart(page string, chart Chart) {
	// add html to page map
	if _, exists := dash.pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPage()")
		return
	}

	// Assert the type of the value in pageshtml and pagesjs
	pageHTML, ok := dash.pageshtml[page].(map[int]string)
	if !ok {
		fmt.Println("Invalid type for pageshtml[page]")
		return
	}

	pageJS, ok := dash.pagesjs[page].(map[int]string)
	if !ok {
		fmt.Println("Invalid type for pagesjs[page]")
		return
	}

	newdivid := chart.Htmldivid + strconv.Itoa(len(pageHTML))
	html := `<div v-show="page == '` + page + `' " id="` + newdivid + chart.Htmlpostid
	js := chart.Jspreid + newdivid + chart.Jspostid
	pageHTML[len(pageHTML)] = html
	pageJS[len(pageJS)] = js

	// Update the maps with the new values
	dash.pageshtml[page] = pageHTML
	dash.pagesjs[page] = pageJS
}

// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (dash *Dashboard) AddHeading(page string, heading string, size int) {
	// add html to page map
	if _, exists := dash.pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPage()")
		return
	}
	// Assert the type of the value in pageshtml and pagesjs
	pageHTML, ok := dash.pageshtml[page].(map[int]string)
	if !ok {
		fmt.Println("Invalid type for pageshtml[page]")
		return
	}
	var text_size string
	if size == 1 {
		text_size = "text-6xl"
	} else if size == 2 {
		text_size = "text-5xl"
	} else if size == 3 {
		text_size = "text-4xl"
	} else if size == 4 {
		text_size = "text-3xl"
	} else if size == 5 {
		text_size = "text-2xl"
	} else if size == 6 {
		text_size = "text-xl"
	} else if size == 7 {
		text_size = "text-lg"
	} else if size == 8 {
		text_size = "text-md"
	} else if size == 9 {
		text_size = "text-sm"
	} else if size == 10 {
		text_size = "text-xs"
	} else {
		text_size = "text-md"
	}
	html := `<h1 v-if="page == '` + page + fmt.Sprintf(`' " class="%s p-8 flex justify-start"> `, text_size) + heading + `</h1>`
	pageHTML[len(pageHTML)] = html
	// Update the maps with the new values
	dash.pageshtml[page] = pageHTML

}

// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (dash *Dashboard) AddText(page string, text string) {
	// add html to page map
	if _, exists := dash.pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPage()")
		return
	}
	// Assert the type of the value in pageshtml and pagesjs
	pageHTML, ok := dash.pageshtml[page].(map[int]string)
	if !ok {
		fmt.Println("Invalid type for pageshtml[page]")
		return
	}
	text_size := "text-md"
	html := `<h1 v-if="page == '` + page + fmt.Sprintf(`' " class="%s pl-12 pr-12 flex justify-start text-left"> `, text_size) + text + `</h1>`
	pageHTML[len(pageHTML)] = html
	// Update the maps with the new values
	dash.pageshtml[page] = pageHTML

}

// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (dash *Dashboard) AddSubText(page string, text string) {
	// add html to page map
	if _, exists := dash.pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPage()")
		return
	}
	// Assert the type of the value in pageshtml and pagesjs
	pageHTML, ok := dash.pageshtml[page].(map[int]string)
	if !ok {
		fmt.Println("Invalid type for pageshtml[page]")
		return
	}
	text_size := "text-sm"
	html := `<h1 v-if="page == '` + page + fmt.Sprintf(`' " class="%s pl-12 pr-12 pb-8 flex justify-center"> `, text_size) + text + `</h1>`
	pageHTML[len(pageHTML)] = html
	// Update the maps with the new values
	dash.pageshtml[page] = pageHTML

}

// add bullet list
// add html to page map
// add title text-2xl - this should just be the page name and automatically populate at the top of the page...
// add html to page map
func (dash *Dashboard) AddBullets(page string, text ...string) {
	// add html to page map
	if _, exists := dash.pageshtml[page]; !exists {
		fmt.Println("Page does not exist. Use AddPage()")
		return
	}
	// Assert the type of the value in pageshtml and pagesjs
	pageHTML, ok := dash.pageshtml[page].(map[int]string)
	if !ok {
		fmt.Println("Invalid type for pageshtml[page]")
		return
	}
	text_size := "text-md"
	html := `<ul v-if="page == '` + page + fmt.Sprintf(`' " class="list-disc flex-col justify-self-start pl-24 pr-12 py-2"> `)
	for _, bullet := range text {
		html += fmt.Sprintf(`<li class="text-left %s">`, text_size) + bullet + `</li>`
	}
	html += `</ul>`
	pageHTML[len(pageHTML)] = html
	// Update the maps with the new values
	dash.pageshtml[page] = pageHTML

}

// AddRow() - take in any amt of text, image, bullets, charts, etc. - flex & justify

// AddCol() - take in any amt of text, image, bullets, charts, etc. - flex & justify

// add button

// // set primary color
// func (dash *Dashboard) SetPrimaryColor(color string) {
// 	// check regex of hex value
// 	dash.primary = `primary: "` + color + `",`
// }

// // set secondary color
// func (dash *Dashboard) SetSecondaryColor(color string) {
// 	// check regex of hex value
// 	dash.secondary = `secondary: "` + color + `",`
// }

// // set accent color
// func (dash *Dashboard) SetAccentColor(color string) {
// 	// check regex of hex value
// 	dash.accent = `accent: "` + color + `",`
// }

// // set neutral color
// func (dash *Dashboard) SetNeutralColor(color string) {
// 	// check regex of hex value
// 	dash.neutral = `neutral: "` + color + `",`
// }

//
