package gophers

// main dashboard object for adding html pages, charts, and inputs for a single html output
type Dashboard struct {
	primary        string
	secondary      string
	accent         string
	neutral        string
	base100        string
	info           string
	success        string
	warning        string
	err            string
	htmlheading    string
	htmlelements   string
	scriptheading  string
	scriptelements string
	pages          []string
	pageshtml      map[string]interface{}
	pagesjs        map[string]interface{}
}

var ScriptHeading string = `
		<script>
			const { createApp } = Vue
			createApp({
			delimiters : ['[[', ']]'],
				data(){
					return {
`

// dashboard create
func (df *DataFrame) CreateDashboard() *Dashboard {
	newDashboard := &Dashboard{
		primary:        "#0000ff",
		secondary:      "#00aaff",
		accent:         "#479700",
		neutral:        "#250e0d",
		base100:        "#fffaff",
		info:           "#00c8ff",
		success:        "#00ec6a",
		warning:        "#ffb900",
		err:            "#f00027",
		htmlheading:    "",
		htmlelements:   "",
		scriptheading:  ScriptHeading,
		scriptelements: "",
		pages:          []string{},
		pageshtml:      make(map[string]interface{}),
		pagesjs:        make(map[string]interface{}),
	}
	HTMLHeading := `
<!DOCTYPE html>
<html>
	<head>
		<script>
		tailwind.config = {
			theme: {
			extend: {
				colors: {
				primary: "` + newDashboard.primary + `",
				secondary: "` + newDashboard.secondary + `",
				accent: "` + newDashboard.accent + `",
				neutral: "` + newDashboard.neutral + `",
				"base-100": "` + newDashboard.base100 + `",
				info: "` + newDashboard.info + `",
				success: "` + newDashboard.success + `",
				warning: "` + newDashboard.warning + `",
				error: "` + newDashboard.err + `",
				}
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
		<div id="app" style="text-align: center;">
	`
	newDashboard.htmlheading = HTMLHeading

	return newDashboard
}

// create page & add a column
func (dash *Dashboard) AddPage(name string) {
	// add page to dashboard type
	// check if already exists
	// add html element for page
	// add html to page map
}

// add row

// add column

// spacing for stuff? card or no card? background?

// add text input

// add slider

// add dropdown (array of selections)

// add chart

// add title

// add header1

// add header2

// add header3

// add header4

// add normal text (paragraph)

// add button

// set primary color

// set secondary color

// set tertiary color

// set neutral color

//
