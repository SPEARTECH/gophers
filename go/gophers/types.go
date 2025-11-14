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
