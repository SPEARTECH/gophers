package gophers

var outer_html string = ``
var bar_chart string = ``
var mixed_bar_chart string = ``
var pie_chart string = ``
var column_chart string = ``
var line_chart string = ``
var scatter_plot string = ``
var word_cloud string = ``
var area_chart string = ``
var data_table string = ``

func (df *DataFrame) BarChart(x string, y string) string {
	html := bar_chart
	return html
}

func (df *DataFrame) MixedBarChart(x string, y string, avg string) string {
	html := mixed_bar_chart
	return html
}

func (df *DataFrame) PieChart(column string) string {
	html := pie_chart
	return html
}

func (df *DataFrame) ColumnChart(x string, y string) string {
	html := pie_chart
	return html
}

func (df *DataFrame) LineChart(x string, y string) string {
	html := pie_chart
	return html
}

func (df *DataFrame) ScatterPlot(x string, y string) string {
	html := pie_chart
	return html
}

func (df *DataFrame) WordCloud(columns ...string) string {
	html := pie_chart
	return html
}

func (df *DataFrame) AreaChart(x string, y string) string {
	html := pie_chart
	return html
}

func (df *DataFrame) DataTable(columns ...string) string {
	html := pie_chart
	return html
}
