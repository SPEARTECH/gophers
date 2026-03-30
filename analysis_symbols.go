package gophers

import (
	"reflect"

	"github.com/traefik/yaegi/interp"
)

// gophersSymbols returns the yaegi symbol exports for the gophers package,
// allowing the embedded interpreter to use all public gophers functions and types.
func gophersSymbols() interp.Exports {
	return interp.Exports{
		"github.com/speartech/gophers/gophers": map[string]reflect.Value{
			// Types
			"DataFrame":         reflect.ValueOf((*DataFrame)(nil)),
			"ColumnExpr":        reflect.ValueOf((*ColumnExpr)(nil)),
			"Column":            reflect.ValueOf((*Column)(nil)),
			"Aggregation":       reflect.ValueOf((*Aggregation)(nil)),
			"SimpleAggregation": reflect.ValueOf((*SimpleAggregation)(nil)),
			"Chart":             reflect.ValueOf((*Chart)(nil)),
			"Report":            reflect.ValueOf((*Report)(nil)),
			"LLM":               reflect.ValueOf((*LLM)(nil)),
			"ColumnSchema":      reflect.ValueOf((*ColumnSchema)(nil)),
			"AggregatorFn":      reflect.ValueOf((*AggregatorFn)(nil)),

			// DataFrame creation / source functions
			"Dataframe":   reflect.ValueOf(Dataframe),
			"ReadJSON":    reflect.ValueOf(ReadJSON),
			"ReadCSV":     reflect.ValueOf(ReadCSV),
			"ReadNDJSON":  reflect.ValueOf(ReadNDJSON),
			"ReadYAML":    reflect.ValueOf(ReadYAML),
			"ReadParquet":  reflect.ValueOf(ReadParquet),
			"ReadHTML":     reflect.ValueOf(ReadHTML),
			"ReadHTMLTop":  reflect.ValueOf(ReadHTMLTop),
			"ReadSqlite":   reflect.ValueOf(ReadSqlite),
			"GetAPI":       reflect.ValueOf(GetAPI),
			"SqliteSQL":    reflect.ValueOf(SqliteSQL),
			"CloneJSON":    reflect.ValueOf(CloneJSON),

			// Column / expression functions
			"Col":              reflect.ValueOf(Col),
			"Lit":              reflect.ValueOf(Lit),
			"Concat":           reflect.ValueOf(Concat),
			"CurrentTimestamp":  reflect.ValueOf(CurrentTimestamp),
			"CurrentDate":      reflect.ValueOf(CurrentDate),
			"DateDiff":         reflect.ValueOf(DateDiff),
			"SHA256":           reflect.ValueOf(SHA256),
			"SHA512":           reflect.ValueOf(SHA512),
			"UDF":              reflect.ValueOf(UDF),
			"Compile":          reflect.ValueOf(Compile),
			"If":               reflect.ValueOf(If),
			"Or":               reflect.ValueOf(Or),
			"And":              reflect.ValueOf(And),

			// Aggregation functions
			"Agg":         reflect.ValueOf(Agg),
			"Sum":         reflect.ValueOf(Sum),
			"Max":         reflect.ValueOf(Max),
			"Min":         reflect.ValueOf(Min),
			"Median":      reflect.ValueOf(Median),
			"Mean":        reflect.ValueOf(Mean),
			"Mode":        reflect.ValueOf(Mode),
			"Unique":      reflect.ValueOf(Unique),
			"First":       reflect.ValueOf(First),
			"CollectList":  reflect.ValueOf(CollectList),
			"CollectSet":   reflect.ValueOf(CollectSet),

			// SQLite helpers
			"ListSqliteTables":    reflect.ValueOf(ListSqliteTables),
			"GetSqliteSchema":     reflect.ValueOf(GetSqliteSchema),
			"GetSqliteSchemaJSON": reflect.ValueOf(GetSqliteSchemaJSON),
			"GetSqliteTables":     reflect.ValueOf(GetSqliteTables),
			"GetSqliteTablesJSON": reflect.ValueOf(GetSqliteTablesJSON),

			// Report / display / misc
			"CreateReport": reflect.ValueOf(CreateReport),
			"DisplayChart": reflect.ValueOf(DisplayChart),
			"DisplayHTML":  reflect.ValueOf(DisplayHTML),
			"QuoteArray":   reflect.ValueOf(QuoteArray),
			"ConnectLLM":   reflect.ValueOf(ConnectLLM),
			"CustomLLM":    reflect.ValueOf(CustomLLM),
		},
	}
}
