package gophers

import (
	"encoding/json"

	"golang.org/x/net/html"
)

// DataFrame represents a very simple dataframe structure.
type DataFrame struct {
	Cols []string
	Data map[string][]interface{}
	Rows int
}

// Help returns a help string listing available DataFrame methods.
func (df *DataFrame) Help() string {
	help := `DataFrame Help:
		BarChart(title, subtitle, groupcol, aggs)
		Clone()
		Column(col_name, col_spec)
		ColumnChart(title, subtitle, groupcol, aggs)
		Columns()
		Collect(col_name)
		Count()
		CountDistinct(cols)
		CountDuplicates(cols)
		CreateReport(title)
		Display()
		DisplayBrowser()
		DisplayToFile(file_path)
		Drop(*cols)
		DropDuplicates(cols)
		DropNA(cols)
		FillNA(value)
		Filter(condition)
		Flatten(*cols)
		GroupBy(groupCol, aggs)
		Head(chars)
		Join(df2, col1, col2, how)
		OrderBy(col, asc)
		PostAPI(endpoint, headers, query_params)
		Select(*cols)
		Show(chars, record_count)
		Sort(*cols)
		StackedBarChart(title, subtitle, groupcol, aggs)
		StackedPercentChart(title, subtitle, groupcol, aggs)
		StringArrayConvert(col_name)
		Tail(chars)
		ToCSVFile(filename)
		Union(df2)
		Vertical(chars, record_count)
		WriteSqlite(db_path, table_name, mode, key_cols)`
	fmt.Println(help)
	return help
}


type ColumnExpr struct {
	Type      string          `json:"type"`
	Name      string          `json:"name,omitempty"`
	Value     interface{}     `json:"value,omitempty"`
	Expr      json.RawMessage `json:"expr,omitempty"`
	Left      json.RawMessage `json:"left,omitempty"`
	Right     json.RawMessage `json:"right,omitempty"`
	Cond      json.RawMessage `json:"cond,omitempty"`
	True      json.RawMessage `json:"true,omitempty"`
	False     json.RawMessage `json:"false,omitempty"`
	Cols      json.RawMessage `json:"cols,omitempty"`
	Col       string          `json:"col,omitempty"`
	Delimiter string          `json:"delimiter,omitempty"`
	Datatype  string          `json:"datatype,omitempty"`
	// string ops
	Old     interface{} `json:"old,omitempty"`
	New     interface{} `json:"new,omitempty"`
	Substr  interface{} `json:"substr,omitempty"`
	Prefix  interface{} `json:"prefix,omitempty"`
	Suffix  interface{} `json:"suffix,omitempty"`
	Pattern interface{} `json:"pattern,omitempty"`
}

func (ce *ColumnExpr) Help() string {
	help := `Column Help:
    Contains(substr)
    EndsWith(suffix)
    Eq(other)
    Ge(other)
    Gt(other)
    HtmlUnescape()
    IsBetween(lower, upper)
    IsIn(values)
    IsNotNull()
    IsNull()
    Le(other)
    Like(pattern)
    Lower()
    Lt(other)
    LTrim()
    Ne(other)
    NotContains(substr)
    NotLike(pattern)
    Replace(old, new)
    RTrim()
    StartsWith(prefix)
    Substr(start, length)
    Title()
    Trim()
    Upper()`
	fmt.Println(help)
	return help
}


// add other methods that modify the chart (no menu icon, no horizontal lines, highcharts vs apexcharts, colors, etc)?
type Chart struct {
	Htmlpreid  string
	Htmldivid  string
	Htmlpostid string
	Jspreid    string
	Jspostid   string
}

// AggregatorFn defines a function that aggregates a slice of values.
type AggregatorFn func([]interface{}) interface{}

// Aggregation holds a target column name and the aggregation function to apply.
type Aggregation struct {
	ColumnName string
	Fn         AggregatorFn
}

type SimpleAggregation struct {
	ColumnName string
}

// Report object for adding html pages, charts, and inputs for a single html output
type Report struct {
	Top           string
	Primary       string
	Secondary     string
	Accent        string
	Neutral       string
	Base100       string
	Info          string
	Success       string
	Warning       string
	Err           string
	Htmlheading   string
	Title         string
	Htmlelements  string
	Scriptheading string
	Scriptmiddle  string
	Bottom        string
	Pageshtml     map[string]map[string]string
	Pagesjs       map[string]map[string]string
}

// Help returns a help string listing available Report methods.
func (report *Report) Help() string {
	help := `Report Help:
		Accent(color)
		AddBullets(page, bullets)
		AddChart(page, chart)
		AddDataframe(page, df)
		AddHeading(page, text, size)
		AddHTML(page, text)
		AddPage(name)
		AddSubText(page, text)
		AddText(page, text)
		Base100(color)
		Err(color)
		Info(color)
		Neutral(color)
		Primary(color)
		Secondary(color)
		Success(color)
		Warning(color)
		Open()
		Save(filename)`
	fmt.Println(help)
	return help
}



// ColumnFunc is a function type that takes a row and returns a value.
// type Column func(row map[string]interface{}) interface{}
// Column represents a column in the DataFrame.
type Column struct {
	Name string
	Fn   func(row map[string]interface{}) interface{}
}

type nodeInfo struct {
	n           *html.Node
	index       int
	parentIndex int
	depth       int
	tag         string
	textDirect  string
	href        string
	src         string
}

type rendered struct {
	outer string
	inner string
	habs  string
	sabs  string
}
