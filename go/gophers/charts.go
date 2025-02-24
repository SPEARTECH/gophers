package gophers

import (
	"encoding/json"
	"fmt"
)

type Chart struct {
	htmlpreid  string
	htmldivid  string
	htmlpostid string
	jspreid    string
	jspostid   string
}

// BarChart returns Bar Chart HTML for the DataFrame.
// It takes a title, subtitle, group column, and one or more aggregations.
func (df *DataFrame) BarChart(title string, subtitle string, groupcol string, aggs []Aggregation) Chart {
	// Group the DataFrame by the specified column and apply the aggregations.
	df = df.GroupBy(groupcol, aggs...)
	// df.Show(25)

	// Extract categories and series data.
	categories := []string{}
	for _, val := range df.Data[groupcol] {
		categories = append(categories, fmt.Sprintf("%v", val))
	}

	series := []map[string]interface{}{}
	for _, agg := range aggs {
		data := []interface{}{}
		for _, val := range df.Data[agg.ColumnName] {
			data = append(data, val)
		}
		series = append(series, map[string]interface{}{
			"name": agg.ColumnName,
			"data": data,
		})
	}

	// Convert categories and series to JSON.
	categoriesJSON, _ := json.Marshal(categories)
	seriesJSON, _ := json.Marshal(series)

	// Build the HTML and JavaScript for the chart.
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
        categories: %s,
        title: {
            text: '%s'
        },
        gridLineWidth: 1,
        lineWidth: 0
    },
    yAxis: {
        min: 0,
        title: {
            text: '',
            align: 'middle'
        },
        labels: {
            overflow: 'justify'
        },
        gridLineWidth: 0
    },
    tooltip: {
        valueSuffix: ''
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
    credits: {
        enabled: false
    },
    series: %s
});`, title, subtitle, categoriesJSON, groupcol, seriesJSON)

	newChart := Chart{htmlpreid, htmldivid, htmlpostid, jspreid, jspostid}
	return newChart
}

// ColumnChart returns Column Chart HTML for the DataFrame.
// It takes a title, subtitle, group column, and one or more aggregations.
func (df *DataFrame) ColumnChart(title string, subtitle string, groupcol string, aggs []Aggregation) Chart {
	// Group the DataFrame by the specified column and apply the aggregations.
	df = df.GroupBy(groupcol, aggs...)
	// df.Show(25)

	// Extract categories and series data.
	categories := []string{}
	for _, val := range df.Data[groupcol] {
		categories = append(categories, fmt.Sprintf("%v", val))
	}

	series := []map[string]interface{}{}
	for _, agg := range aggs {
		data := []interface{}{}
		for _, val := range df.Data[agg.ColumnName] {
			data = append(data, val)
		}
		series = append(series, map[string]interface{}{
			"name": agg.ColumnName,
			"data": data,
		})
	}

	// Convert categories and series to JSON.
	categoriesJSON, _ := json.Marshal(categories)
	seriesJSON, _ := json.Marshal(series)

	// Build the HTML and JavaScript for the chart.
	htmlpreid := `<div id="`
	htmldivid := `columnchart`
	htmlpostid := `" class="flex justify-center mx-auto p-4"></div>`
	jspreid := `Highcharts.chart('`
	jspostid := fmt.Sprintf(`', {
    chart: {
        type: 'column'
    },
    title: {
        text: '%s'
    },
    subtitle: {
        text: '%s'
    },
    xAxis: {
        categories: %s,
        title: {
            text: '%s'
        },
        gridLineWidth: 1,
        lineWidth: 0
    },
    yAxis: {
        min: 0,
        title: {
            text: '',
            align: 'middle'
        },
        labels: {
            overflow: 'justify'
        },
        gridLineWidth: 0
    },
    tooltip: {
        valueSuffix: ''
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
    credits: {
        enabled: false
    },
    series: %s
});`, title, subtitle, categoriesJSON, groupcol, seriesJSON)

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

// StackedBarChart returns Stacked Bar Chart HTML for the DataFrame.
// It takes a title, subtitle, group column, and one or more aggregations.
func (df *DataFrame) StackedBarChart(title string, subtitle string, groupcol string, aggs []Aggregation) Chart {
	// Group the DataFrame by the specified column and apply the aggregations.
	df = df.GroupBy(groupcol, aggs...)
	// df.Show(25)

	// Extract categories and series data.
	categories := []string{}
	for _, val := range df.Data[groupcol] {
		categories = append(categories, fmt.Sprintf("%v", val))
	}

	series := []map[string]interface{}{}
	for _, agg := range aggs {
		data := []interface{}{}
		for _, val := range df.Data[agg.ColumnName] {
			data = append(data, val)
		}
		series = append(series, map[string]interface{}{
			"name": agg.ColumnName,
			"data": data,
		})
	}

	// Convert categories and series to JSON.
	categoriesJSON, _ := json.Marshal(categories)
	seriesJSON, _ := json.Marshal(series)

	// Build the HTML and JavaScript for the chart.
	htmlpreid := `<div id="`
	htmldivid := `stackedbarchart`
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
        categories: %s,
        title: {
            text: '%s'
        },
        gridLineWidth: 1,
        lineWidth: 0
    },
    yAxis: {
        min: 0,
        title: {
            text: '',
            align: 'middle'
        },
    },
    plotOptions: {
        series: {
            stacking: 'normal',
            dataLabels: {
                enabled: true
            }
        }
    },
    series: %s
});`, title, subtitle, categoriesJSON, groupcol, seriesJSON)

	newChart := Chart{htmlpreid, htmldivid, htmlpostid, jspreid, jspostid}
	return newChart
}

// StackedPercentChart returns Stacked Percent Column Chart HTML for the DataFrame.
// It takes a title, subtitle, group column, and one or more aggregations.
func (df *DataFrame) StackedPercentChart(title string, subtitle string, groupcol string, aggs []Aggregation) Chart {
	// Group the DataFrame by the specified column and apply the aggregations.
	df = df.GroupBy(groupcol, aggs...)
	// df.Show(25)

	// Extract categories and series data.
	categories := []string{}
	for _, val := range df.Data[groupcol] {
		categories = append(categories, fmt.Sprintf("%v", val))
	}

	series := []map[string]interface{}{}
	for _, agg := range aggs {
		data := []interface{}{}
		for _, val := range df.Data[agg.ColumnName] {
			data = append(data, val)
		}
		series = append(series, map[string]interface{}{
			"name": agg.ColumnName,
			"data": data,
		})
	}

	// Convert categories and series to JSON.
	categoriesJSON, _ := json.Marshal(categories)
	seriesJSON, _ := json.Marshal(series)

	// Build the HTML and JavaScript for the chart.
	htmlpreid := `<div id="`
	htmldivid := `stackedpercentchart`
	htmlpostid := `" class="flex justify-center mx-auto p-4"></div>`
	jspreid := `Highcharts.chart('`
	jspostid := fmt.Sprintf(`', {
    chart: {
        type: 'column'
    },
    title: {
        text: '%s'
    },
    subtitle: {
        text: '%s'
    },
    xAxis: {
        categories: %s,
        title: {
            text: '%s'
        },
        gridLineWidth: 1,
        lineWidth: 0
    },
    yAxis: {
        min: 0,
        title: {
            text: 'Percent',
            align: 'middle'
        },
    },
    tooltip: {
        pointFormat: '<span style="color:{series.color}">{series.name}</span>' +
            ': <b>{point.y}</b> ({point.percentage:.0f}%%)<br/>',
        shared: true
    },
    plotOptions: {
        column: {
            stacking: 'percent',
            dataLabels: {
                enabled: true,
                format: '{point.percentage:.0f}%%'
            }
        }
    },
    series: %s
});`, title, subtitle, categoriesJSON, groupcol, seriesJSON)

	newChart := Chart{htmlpreid, htmldivid, htmlpostid, jspreid, jspostid}
	return newChart
}

// func (df *DataFrame) MixedBarChart(x string, y string, avg string) string {
// 	html := ``
// 	return html
// }

func (df *DataFrame) PieChart(columns ...string) string {
	html := `
Highcharts.chart('container', {
    chart: {
        type: 'pie'
    },
    title: {
        text: 'Egg Yolk Composition'
    },
    tooltip: {
        valueSuffix: '%'
    },
    subtitle: {
        text:
        'Source:<a href="https://www.mdpi.com/2072-6643/11/3/684/htm" target="_default">MDPI</a>'
    },
    plotOptions: {
        series: {
            allowPointSelect: true,
            cursor: 'pointer',
            dataLabels: [{
                enabled: true,
                distance: 20
            }, {
                enabled: true,
                distance: -40,
                format: '{point.percentage:.1f}%',
                style: {
                    fontSize: '1.2em',
                    textOutline: 'none',
                    opacity: 0.7
                },
                filter: {
                    operator: '>',
                    property: 'percentage',
                    value: 10
                }
            }]
        }
    },
    series: [
        {
            name: 'Percentage',
            colorByPoint: true,
            data: [
                {
                    name: 'Water',
                    y: 55.02
                },
                {
                    name: 'Fat',
                    sliced: true,
                    selected: true,
                    y: 26.71
                },
                {
                    name: 'Carbohydrates',
                    y: 1.09
                },
                {
                    name: 'Protein',
                    y: 15.5
                },
                {
                    name: 'Ash',
                    y: 1.68
                }
            ]
        }
    ]
});
    `
	return html
}

// *
func (df *DataFrame) LineChart(x string, y string) string {
	html := `
Highcharts.chart('container', {

    title: {
        text: 'U.S Solar Employment Growth',
        align: 'left'
    },

    subtitle: {
        text: 'By Job Category. Source: <a href="https://irecusa.org/programs/solar-jobs-census/" target="_blank">IREC</a>.',
        align: 'left'
    },

    yAxis: {
        title: {
            text: 'Number of Employees'
        }
    },

    xAxis: {
        accessibility: {
            rangeDescription: 'Range: 2010 to 2022'
        }
    },

    legend: {
        layout: 'vertical',
        align: 'right',
        verticalAlign: 'middle'
    },

    plotOptions: {
        series: {
            label: {
                connectorAllowed: false
            },
            pointStart: 2010
        }
    },

    series: [{
        name: 'Installation & Developers',
        data: [
            43934, 48656, 65165, 81827, 112143, 142383,
            171533, 165174, 155157, 161454, 154610, 168960, 171558
        ]
    }, {
        name: 'Manufacturing',
        data: [
            24916, 37
    `
	return html
}

// *
func (df *DataFrame) ScatterPlot(x string, y string) string {
	html := `
Highcharts.setOptions({
    colors: [
        'rgba(5,141,199,0.5)', 'rgba(80,180,50,0.5)', 'rgba(237,86,27,0.5)'
    ]
});

const series = [{
    name: 'Basketball',
    id: 'basketball',
    marker: {
        symbol: 'circle'
    }
},
{
    name: 'Triathlon',
    id: 'triathlon',
    marker: {
        symbol: 'triangle'
    }
},
{
    name: 'Volleyball',
    id: 'volleyball',
    marker: {
        symbol: 'square'
    }
}];


async function getData() {
    const response = await fetch(
        'https://www.highcharts.com/samples/data/olympic2012.json'
    );
    return response.json();
}


getData().then(data => {
    const getData = sportName => {
        const temp = [];
        data.forEach(elm => {
            if (elm.sport === sportName && elm.weight > 0 && elm.height > 0) {
                temp.push([elm.height, elm.weight]);
            }
        });
        return temp;
    };
    series.forEach(s => {
        s.data = getData(s.id);
    });

    Highcharts.chart('container', {
        chart: {
            type: 'scatter',
            zooming: {
                type: 'xy'
            }
        },
        title: {
            text: 'Olympics athletes by height and weight'
        },
        subtitle: {
            text:
          'Source: <a href="https://www.theguardian.com/sport/datablog/2012/aug/07/olympics-2012-athletes-age-weight-height">The Guardian</a>'
        },
        xAxis: {
            title: {
                text: 'Height'
            },
            labels: {
                format: '{value} m'
            },
            startOnTick: true,
            endOnTick: true,
            showLastLabel: true
        },
        yAxis: {
            
    `
	return html
}

// *
func (df *DataFrame) BubbleChart(x string, y string) string {
	html := `
Highcharts.chart('container', {

    chart: {
        type: 'bubble',
        plotBorderWidth: 1,
        zooming: {
            type: 'xy'
        }
    },

    legend: {
        enabled: false
    },

    title: {
        text: 'Sugar and fat intake per country'
    },

    subtitle: {
        text: 'Source: <a href="http://www.euromonitor.com/">Euromonitor</a> and <a href="https://data.oecd.org/">OECD</a>'
    },

    accessibility: {
        point: {
            valueDescriptionFormat: '{index}. {point.name}, fat: {point.x}g, ' +
                'sugar: {point.y}g, obesity: {point.z}%.'
        }
    },

    xAxis: {
        gridLineWidth: 1,
        title: {
            text: 'Daily fat intake'
        },
        labels: {
            format: '{value} gr'
        },
        plotLines: [{
            color: 'black',
            dashStyle: 'dot',
            width: 2,
            value: 65,
            label: {
                rotation: 0,
                y: 15,
                style: {
                    fontStyle: 'italic'
                },
                text: 'Safe fat intake 65g/day'
            },
            zIndex: 3
        }],
        accessibility: {
            rangeDescription: 'Range: 60 to 100 grams.'
        }
    },

    yAxis: {
        startOnTick: false,
        endOnTick: false,
        title: {
            text: 'Daily sugar intake'
        },
        labels: {
            format: '{value} gr'
        },
        maxPadding: 0.2,
        plotLines: [{
            color: 'black',
            dashStyle: 'dot',
            width: 2,
            value: 50,
            label: {
                align: 'right',
                style: {
        
    `
	return html
}

// *
func (df *DataFrame) TreeMap(columns ...string) string {
	html := ``
	return html
}

// *
func (df *DataFrame) AreaChart(x string, y string) string {
	html := `
// Data retrieved from https://fas.org/issues/nuclear-weapons/status-world-nuclear-forces/
Highcharts.chart('container', {
    chart: {
        type: 'area'
    },
    accessibility: {
        description: 'Image description: An area chart compares the nuclear ' +
            'stockpiles of the USA and the USSR/Russia between 1945 and ' +
            '2024. The number of nuclear weapons is plotted on the Y-axis ' +
            'and the years on the X-axis. The chart is interactive, and the ' +
            'year-on-year stockpile levels can be traced for each country. ' +
            'The US has a stockpile of 2 nuclear weapons at the dawn of the ' +
            'nuclear age in 1945. This number has gradually increased to 170 ' +
            'by 1949 when the USSR enters the arms race with one weapon. At ' +
            'this point, the US starts to rapidly build its stockpile ' +
            'culminating in 31,255 warheads by 1966 compared to the USSR’s 8,' +
            '400. From this peak in 1967, the US stockpile gradually ' +
            'decreases as the USSR’s stockpile expands. By 1978 the USSR has ' +
            'closed the nuclear gap at 25,393. The USSR stockpile continues ' +
            'to grow until it reaches a peak of 40,159 in 1986 compared to ' +
            'the US arsenal of 24,401. From 1986, the nuclear stockpiles of ' +
            'both countries start to fall. By 2000, the numbers have fallen ' +
            'to 10,577 and 12,188 for the US and Russia, respectively. The ' +
            'decreases continue slowly after plateauing in the 2010s, and in ' +
            '2024 the US has 3,708 weapons compared to Russia’s 4,380.'
    },
    title: {
        text: 'US and USSR nuclear stockpiles'
    },
    subtitle: {
        text: 'Source: <a href="https://fas.org/issues/nuclear-weapons/status-world-nuclear-forces/" ' +
            'target="_blank">FAS</a>'
    },
    xAxis: {
        allowDecimals: false,
        accessibility: {
            rangeDescription: 'Range: 1940 to 2024.'
        }
    },
    yAxis: {
        title: {
            text: 'Nuclear weapon states'
        }
    },
    tooltip: {
        pointFormat: '{series.name} had stockpiled <b>{point.y:,.0f}</b><br/>' +
            'warheads in {point.x}'
    },
    plotOptions: {
        area: {
            pointStart: 1940,
            marker: {
                enabled: false,
                symbol: 'circle',
                radius: 2,
                states: {
                    hover: {
                        enabled: true
                    }
                }
            }
        }
    },
    series: [{
        name: 'USA',
        data: [
            null, null, null, null, null, 2, 9, 13, 50, 170, 299, 438, 841,
            1169, 1703, 2422, 3692, 5543, 7345, 12298, 18638, 22229, 25540,
            28133, 29463, 31139, 31175, 31255, 29561, 27552, 26008, 25830,
            26516, 27835, 28537, 27519, 25914, 25542, 24418, 24138, 24104,
            23208, 22886, 23305, 23459, 23368, 23317, 23575, 23205, 22217,
            21392, 19008, 13708, 11511, 10979, 10904, 11011, 10903, 10732,
            10685, 10577, 10526, 10457, 10027, 8570, 8360, 7853, 5709, 5273,
            5113, 5066, 4897, 4881, 4804, 4717, 4571, 4018, 3822, 3785, 3805,
            3750, 3708, 3708, 3708, 3708
        ]
    }, {
        name: 'USSR/Russia',
        data: [
            null, null, null, null, null, null, null, null, null,
            1, 5, 25, 50, 120, 150, 200, 426, 660, 863, 1048, 1627, 2492,
            3346, 4259, 5242, 6144, 7091, 8400, 9490, 10671, 11736, 13279,
            14600, 15878, 17286, 19235, 22165, 24281, 26169, 28258, 30665,
            32146, 33486, 35130, 36825, 38582, 40159, 38107, 36538, 35078,
            32980, 29154, 26734, 24403, 21339, 18179, 15942, 15442, 14368,
            13188, 12188, 11152, 10114, 9076, 8038, 7000, 6643, 6286, 5929,
            5527, 5215, 4858, 4750, 4650, 4600, 4500, 4490, 4300, 4350, 4330,
            4310, 4495, 4477, 4489, 4380
        ]
    }]
});

    `
	return html
}

// stacked area chart

// stacked percent area chart

// donut chart

// *
func (df *DataFrame) DataTable(columns ...string) string {
	html := `` // call display with only html return
	return html
}

// histogram *

//
