package gophers

import (
	"fmt"
	"encoding/json"
)
type Chart struct {
	htmlpreid  string
	htmldivid  string
	htmlpostid string	
	jspreid    string
	jspostid   string
}
// return Bar Chart html for dataframe
// pass columns for x and y
// if columns are strings, they will be
func (df *DataFrame) BarChart(x string, y string, title string, subtitle string) Chart { // add functionality for grouped bar chart + counting values if string
	GroupedSeriesJSON := df.GroupedSeriesJSON(x,y)
	htmlpreid := `<div id="`
	htmldivid := `barchart`
	htmlpostid := `" class="flex justify-center mx-auto p-4"></div>`
	jspreid := `Highcharts.chart('`
	jspostid := fmt.Sprintf(`', {
    chart: {
        type: 'bar'
    },
    title: {
        text: '%s'
    },
    subtitle: {
        text: '%s'
    },
    xAxis: {
        categories: '',
        title: {
            text: '%s'
        },
        gridLineWidth: 1,
        lineWidth: 0
    },
    yAxis: {
        min: 0,
        title: {
            text: '%s',
            align: 'middle'
        },
        labels: {
            overflow: 'justify'
        },
        gridLineWidth: 0
    },
    tooltip: {
        valueSuffix: ' millions'
    },
    plotOptions: {
        bar: {
            borderRadius: '50%%',
            dataLabels: {
                enabled: true
            },
            groupPadding: 0.1
        }
    },
    // legend: {
    //     layout: 'vertical',
    //     align: 'right',
    //     verticalAlign: 'top',
    //     x: -5,
    //     y: 20,
    //     floating: true,
    //     borderWidth: 1,
    //     backgroundColor:
    //         Highcharts.defaultOptions.legend.backgroundColor || '#FFFFFF',
    //     shadow: true
    // },
    credits: {
        enabled: false
    },
    series: %s
});	`, title, subtitle, x, y, GroupedSeriesJSON) // need to show chart within app div + fix series data (make function for this?)
	newChart := Chart{htmlpreid, htmldivid, htmlpostid, jspreid, jspostid}
	return newChart
}

func (df *DataFrame) GroupedSeriesJSON(x string, y string) string {
    // Assume df.Collect returns a slice for the given column
    xData := df.Collect(x)
    yData := df.Collect(y)

    groups := make(map[string][]interface{})
    // Group by x values; assume both slices have same length.
    for i := 0; i < len(xData) && i < len(yData); i++ {
        groupKey := fmt.Sprintf("%v", xData[i])
        groups[groupKey] = append(groups[groupKey], yData[i])
    }

    // Build the series slice where each object has a "name" and "data" field.
    var series []map[string]interface{}
    for key, data := range groups {
        series = append(series, map[string]interface{}{
            "name": key,
            "data": data,
        })
    }

    jsonBytes, err := json.Marshal(series)
    if err != nil {
        return "[]"
    }
    return string(jsonBytes)
}

func (df *DataFrame) StackedBarChart(x string, y string, avg string) string {
	html := ``
	return html
}

func (df *DataFrame) MixedBarChart(x string, y string, avg string) string {
	html := ``
	return html
}

func (df *DataFrame) PieChart(columns ...string) string {
	html := ``
	return html
}

func (df *DataFrame) ColumnChart(x string, y string) string {
	html := ``
	return html
}

func (df *DataFrame) LineChart(x string, y string) string {
	html := ``
	return html
}

func (df *DataFrame) ScatterPlot(x string, y string) string {
	html := ``
	return html
}

func (df *DataFrame) BubbleChart(x string, y string) string {
	html := ``
	return html
}

func (df *DataFrame) WordCloud(columns ...string) string {
	html := ``
	return html
}

func (df *DataFrame) AreaChart(x string, y string) string {
	html := ``
	return html
}

func (df *DataFrame) DataTable(columns ...string) string {
	html := ``
	return html
}
