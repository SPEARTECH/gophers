package gophers

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"html"
	"reflect"
	"regexp"
	"strings"
)

func asFloat(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case float32:
		return float64(x), true
	case float64:
		return x, true
	default:
		return 0, false
	}
}

func eqValues(a, b interface{}) bool {
	// numeric path
	if fa, okA := asFloat(a); okA {
		if fb, okB := asFloat(b); okB {
			return fa == fb
		}
	}
	// fallback string compare
	return fmt.Sprint(a) == fmt.Sprint(b)
}

func cmpNumbers(a, b interface{}, op string) bool {
	fa, okA := asFloat(a)
	fb, okB := asFloat(b)
	if !okA || !okB {
		return false
	}
	switch op {
	case "gt":
		return fa > fb
	case "lt":
		return fa < fb
	case "ge":
		return fa >= fb
	case "le":
		return fa <= fb
	default:
		return false
	}
}

// func Evaluate(expr ColumnExpr, row map[string]interface{}) interface{} {
// 	switch expr.Type {
// 	case "col":
// 		return row[expr.Name]
// 	case "lit":
// 		return expr.Value
// 	case "isnull":
// 		// Check if the sub-expression is provided.
// 		if len(expr.Expr) == 0 {
// 			return true // or false depending on how you want to handle it
// 		}
// 		var subExpr ColumnExpr
// 		json.Unmarshal(expr.Expr, &subExpr)
// 		val := Evaluate(subExpr, row)
// 		if val == nil {
// 			return true
// 		}
// 		switch v := val.(type) {
// 		case string:
// 			return v == "" || strings.ToLower(v) == "null"
// 		case *string:
// 			return v == nil || *v == "" || strings.ToLower(*v) == "null"
// 		default:
// 			return false
// 		}
// 	case "isnotnull":
// 		if len(expr.Expr) == 0 {
// 			return false
// 		}
// 		var subExpr ColumnExpr
// 		json.Unmarshal(expr.Expr, &subExpr)
// 		val := Evaluate(subExpr, row)
// 		if val == nil {
// 			return true
// 		}
// 		switch v := val.(type) {
// 		case string:
// 			return !(v == "" || strings.ToLower(v) == "null")
// 		case *string:
// 			return !(v == nil || *v == "" || strings.ToLower(*v) == "null")
// 		default:
// 			return true
// 		}
// 		// case "gt":
// 		// 	var leftExpr, rightExpr ColumnExpr
// 		// 	json.Unmarshal(expr.Left, &leftExpr)
// 		// 	json.Unmarshal(expr.Right, &rightExpr)
// 		// 	return Evaluate(leftExpr, row).(float64) > Evaluate(rightExpr, row).(float64)
// 		// case "lt":
// 		// 	var leftExpr, rightExpr ColumnExpr
// 		// 	json.Unmarshal(expr.Left, &leftExpr)
// 		// 	json.Unmarshal(expr.Right, &rightExpr)
// 		// 	return Evaluate(leftExpr, row).(float64) < Evaluate(rightExpr, row).(float64)
// 		// case "le":
// 		// 	var leftExpr, rightExpr ColumnExpr
// 		// 	json.Unmarshal(expr.Left, &leftExpr)
// 		// 	json.Unmarshal(expr.Right, &rightExpr)
// 		// 	return Evaluate(leftExpr, row).(float64) <= Evaluate(rightExpr, row).(float64)
// 		// case "ge":
// 		// 	var leftExpr, rightExpr ColumnExpr
// 		// 	json.Unmarshal(expr.Left, &leftExpr)
// 		// 	json.Unmarshal(expr.Right, &rightExpr)
// 		// 	return Evaluate(leftExpr, row).(float64) >= Evaluate(rightExpr, row).(float64)
// 		// case "eq":
// 		// 	var leftExpr, rightExpr ColumnExpr
// 		// 	json.Unmarshal(expr.Left, &leftExpr)
// 		// 	json.Unmarshal(expr.Right, &rightExpr)
// 		// 	return Evaluate(leftExpr, row).(float64) == Evaluate(rightExpr, row).(float64)
// 		// case "ne":
// 		// 	var leftExpr, rightExpr ColumnExpr
// 		// 	json.Unmarshal(expr.Left, &leftExpr)
// 		// 	json.Unmarshal(expr.Right, &rightExpr)
// 		// 	return Evaluate(leftExpr, row).(float64) != Evaluate(rightExpr, row).(float64)
// 	case "gt", "lt", "le", "ge":
// 		var leftExpr, rightExpr ColumnExpr
// 		json.Unmarshal(expr.Left, &leftExpr)
// 		json.Unmarshal(expr.Right, &rightExpr)
// 		left := Evaluate(leftExpr, row)
// 		right := Evaluate(rightExpr, row)
// 		return cmpNumbers(left, right, expr.Type)

// 	case "eq":
// 		var leftExpr, rightExpr ColumnExpr
// 		json.Unmarshal(expr.Left, &leftExpr)
// 		json.Unmarshal(expr.Right, &rightExpr)
// 		left := Evaluate(leftExpr, row)
// 		right := Evaluate(rightExpr, row)
// 		return eqValues(left, right)

// 	case "ne":
// 		var leftExpr, rightExpr ColumnExpr
// 		json.Unmarshal(expr.Left, &leftExpr)
// 		json.Unmarshal(expr.Right, &rightExpr)
// 		left := Evaluate(leftExpr, row)
// 		right := Evaluate(rightExpr, row)
// 		return !eqValues(left, right)
// 	case "or":
// 		var leftExpr, rightExpr ColumnExpr
// 		json.Unmarshal(expr.Left, &leftExpr)
// 		json.Unmarshal(expr.Right, &rightExpr)
// 		return Evaluate(leftExpr, row).(bool) || Evaluate(rightExpr, row).(bool)
// 	case "and":
// 		var leftExpr, rightExpr ColumnExpr
// 		json.Unmarshal(expr.Left, &leftExpr)
// 		json.Unmarshal(expr.Right, &rightExpr)
// 		return Evaluate(leftExpr, row).(bool) && Evaluate(rightExpr, row).(bool)
// 	case "if":
// 		var condExpr, trueExpr, falseExpr ColumnExpr
// 		json.Unmarshal(expr.Cond, &condExpr)
// 		json.Unmarshal(expr.True, &trueExpr)
// 		json.Unmarshal(expr.False, &falseExpr)
// 		if Evaluate(condExpr, row).(bool) {
// 			return Evaluate(trueExpr, row)
// 		} else {
// 			return Evaluate(falseExpr, row)
// 		}
// 	case "sha256":
// 		var cols []ColumnExpr
// 		json.Unmarshal(expr.Cols, &cols)
// 		var values []string
// 		for _, col := range cols {
// 			values = append(values, fmt.Sprintf("%v", Evaluate(col, row)))
// 		}
// 		return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(values, ""))))
// 	case "sha512":
// 		var cols []ColumnExpr
// 		json.Unmarshal(expr.Cols, &cols)
// 		var values []string
// 		for _, col := range cols {
// 			values = append(values, fmt.Sprintf("%v", Evaluate(col, row)))
// 		}
// 		return fmt.Sprintf("%x", sha512.Sum512([]byte(strings.Join(values, ""))))
// 	case "collectlist":
// 		colName := expr.Col
// 		return row[colName]
// 	case "collectset":
// 		colName := expr.Col
// 		return row[colName]
// 	case "split":
// 		colName := expr.Col
// 		delimiter := expr.Delimiter
// 		val := row[colName].(string)
// 		return strings.Split(val, delimiter)
// 	case "concat":
// 		// "concat_ws" expects a "Delimiter" field (string) and a "Cols" JSON array.
// 		delim := expr.Delimiter
// 		var cols []ColumnExpr
// 		if err := json.Unmarshal(expr.Cols, &cols); err != nil {
// 			fmt.Printf("concat_ws unmarshal error: %v\n", err)
// 			return ""
// 		}
// 		var parts []string
// 		for _, col := range cols {
// 			parts = append(parts, fmt.Sprintf("%v", Evaluate(col, row)))
// 		}
// 		return strings.Join(parts, delim)
// 	case "cast":
// 		// "cast" expects a "Col" field with a JSON object and a "Datatype" field.
// 		var subExpr ColumnExpr
// 		if err := json.Unmarshal([]byte(expr.Col), &subExpr); err != nil {
// 			fmt.Printf("cast unmarshal error (sub expression): %v\n", err)
// 			return nil
// 		}
// 		datatype := expr.Datatype
// 		val := Evaluate(subExpr, row)
// 		switch datatype {
// 		case "int":
// 			casted, err := toInt(val)
// 			if err != nil {
// 				fmt.Printf("cast to int error: %v\n", err)
// 				return nil
// 			}
// 			return casted
// 		case "float":
// 			casted, err := toFloat64(val)
// 			if err != nil {
// 				fmt.Printf("cast to float error: %v\n", err)
// 				return nil
// 			}
// 			return casted
// 		case "string":
// 			casted, err := toString(val)
// 			if err != nil {
// 				fmt.Printf("cast to string error: %v\n", err)
// 				return nil
// 			}
// 			return casted
// 		default:
// 			fmt.Printf("unsupported cast type: %s\n", datatype)
// 			return nil
// 		}
// 	case "arrays_zip":
// 		// "arrays_zip" expects a "Cols" field with a JSON array of column names.
// 		var cols []ColumnExpr
// 		if err := json.Unmarshal(expr.Cols, &cols); err != nil {
// 			fmt.Printf("arrays_zip unmarshal error: %v\n", err)
// 			return nil
// 		}
// 		var zipped []interface{}
// 		for _, col := range cols {
// 			zipped = append(zipped, Evaluate(col, row))
// 		}
// 		return zipped
// 	case "keys":
// 		colName := expr.Col
// 		var keys []string
// 		val := row[colName]
// 		if val == nil {
// 			return keys
// 		}
// 		switch t := val.(type) {
// 		case map[string]interface{}:
// 			for k := range t {
// 				keys = append(keys, k)
// 			}
// 		case map[interface{}]interface{}:
// 			nested := convertMapKeysToString(t)
// 			for k := range nested {
// 				keys = append(keys, k)
// 			}
// 		default:
// 			return keys
// 		}
// 		return keys
// 	case "lookup":
// 		// Evaluate the key expression from the Left field.
// 		var keyExpr ColumnExpr
// 		if err := json.Unmarshal(expr.Left, &keyExpr); err != nil {
// 			return nil
// 		}
// 		keyInterf := Evaluate(keyExpr, row)
// 		keyStr, err := toString(keyInterf)
// 		if err != nil {
// 			return nil
// 		}
// 		// fmt.Printf("Lookup key: %s\n", keyStr)

// 		// Evaluate the nested map expression from the Right field.
// 		var nestedExpr ColumnExpr
// 		if err := json.Unmarshal(expr.Right, &nestedExpr); err != nil {
// 			return nil
// 		}
// 		nestedInterf := Evaluate(nestedExpr, row)
// 		// fmt.Printf("Nested value: %#v\n", nestedInterf)
// 		if nestedInterf == nil {
// 			return nil
// 		}

// 		switch t := nestedInterf.(type) {
// 		case map[string]interface{}:
// 			return t[keyStr]
// 		case map[interface{}]interface{}:
// 			m := convertMapKeysToString(t)
// 			return m[keyStr]
// 		default:
// 			return nil
// 		}
// 	case "lower":
// 		if len(expr.Expr) == 0 {
// 			return ""
// 		}
// 		var sub ColumnExpr
// 		json.Unmarshal(expr.Expr, &sub)
// 		val := Evaluate(sub, row)
// 		return strings.ToLower(fmt.Sprint(val))
// 	case "upper":
// 		if len(expr.Expr) == 0 {
// 			return ""
// 		}
// 		var sub ColumnExpr
// 		json.Unmarshal(expr.Expr, &sub)
// 		val := Evaluate(sub, row)
// 		return strings.ToUpper(fmt.Sprint(val))
// 	case "trim":
// 		if len(expr.Expr) == 0 {
// 			return ""
// 		}
// 		var sub ColumnExpr
// 		json.Unmarshal(expr.Expr, &sub)
// 		val := Evaluate(sub, row)
// 		return strings.TrimSpace(fmt.Sprint(val))
// 	case "ltrim":
// 		if len(expr.Expr) == 0 {
// 			return ""
// 		}
// 		var sub ColumnExpr
// 		json.Unmarshal(expr.Expr, &sub)
// 		val := Evaluate(sub, row)
// 		return strings.TrimLeft(fmt.Sprint(val), " \t\r\n")
// 	case "rtrim":
// 		if len(expr.Expr) == 0 {
// 			return ""
// 		}
// 		var sub ColumnExpr
// 		json.Unmarshal(expr.Expr, &sub)
// 		val := Evaluate(sub, row)
// 		return strings.TrimRight(fmt.Sprint(val), " \t\r\n")
// 	case "replace":
// 		// expects fields: expr (sub expression), old, new
// 		if len(expr.Expr) == 0 {
// 			return ""
// 		}
// 		var sub ColumnExpr
// 		json.Unmarshal(expr.Expr, &sub)
// 		source := fmt.Sprint(Evaluate(sub, row))
// 		return strings.ReplaceAll(source, fmt.Sprint(expr.Old), fmt.Sprint(expr.New))
// 	case "contains":
// 		if len(expr.Expr) == 0 {
// 			return false
// 		}
// 		var sub ColumnExpr
// 		json.Unmarshal(expr.Expr, &sub)
// 		return strings.Contains(fmt.Sprint(Evaluate(sub, row)), fmt.Sprint(expr.Substr))
// 	case "notcontains":
// 		if len(expr.Expr) == 0 {
// 			return false
// 		}
// 		var sub ColumnExpr
// 		json.Unmarshal(expr.Expr, &sub)
// 		return !strings.Contains(fmt.Sprint(Evaluate(sub, row)), fmt.Sprint(expr.Substr))
// 	case "startswith":
// 		if len(expr.Expr) == 0 {
// 			return false
// 		}
// 		var sub ColumnExpr
// 		json.Unmarshal(expr.Expr, &sub)
// 		return strings.HasPrefix(fmt.Sprint(Evaluate(sub, row)), fmt.Sprint(expr.Prefix))
// 	case "endswith":
// 		if len(expr.Expr) == 0 {
// 			return false
// 		}
// 		var sub ColumnExpr
// 		json.Unmarshal(expr.Expr, &sub)
// 		return strings.HasSuffix(fmt.Sprint(Evaluate(sub, row)), fmt.Sprint(expr.Suffix))
// 	case "like":
// 		// simple wildcard * -> contains, ? -> single char
// 		if len(expr.Expr) == 0 {
// 			return false
// 		}
// 		var sub ColumnExpr
// 		json.Unmarshal(expr.Expr, &sub)
// 		text := fmt.Sprint(Evaluate(sub, row))
// 		pat := fmt.Sprint(expr.Pattern)
// 		pat = strings.ReplaceAll(pat, "?", ".")
// 		pat = strings.ReplaceAll(pat, "*", ".*")
// 		ok, _ := regexp.MatchString("^"+pat+"$", text)
// 		return ok
// 	case "notlike":
// 		if len(expr.Expr) == 0 {
// 			return false
// 		}
// 		var sub ColumnExpr
// 		json.Unmarshal(expr.Expr, &sub)
// 		text := fmt.Sprint(Evaluate(sub, row))
// 		pat := fmt.Sprint(expr.Pattern)
// 		pat = strings.ReplaceAll(pat, "?", ".")
// 		pat = strings.ReplaceAll(pat, "*", ".*")
// 		ok, _ := regexp.MatchString("^"+pat+"$", text)
// 		return !ok
// 	case "html_unescape":
// 		if len(expr.Expr) == 0 {
// 			return ""
// 		}
// 		var sub ColumnExpr
// 		json.Unmarshal(expr.Expr, &sub)
// 		val := Evaluate(sub, row)
// 		return html.UnescapeString(fmt.Sprint(val))
// 	default:
// 		return nil
// 	}
// }

// Compile turns a ColumnExpr tree into a Column (closure) once, avoiding
// per-row json.Unmarshal done by Evaluate(expr, row). This is the fast path
// and the single source of truth for expression semantics.
func Compile(e ColumnExpr) Column {
    switch e.Type {
    case "col":
        return Col(e.Name)
    case "lit":
        return Lit(e.Value)
    case "index":
        // Compile sub-expression, then apply Column.Index(i)
        var sub ColumnExpr
        if len(e.Expr) > 0 {
            _ = json.Unmarshal(e.Expr, &sub)
            return Compile(sub).Index(e.Index)
        }
        // Fallback: index a plain column by name
        base := e.Name
        if base == "" { base = e.Col }
        return Col(base).Index(e.Index)
    case "isnull":
        if len(e.Expr) == 0 {
            return Lit(true)
        }
        var sub ColumnExpr
        _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).IsNull()
    case "isnotnull":
        if len(e.Expr) == 0 {
            return Lit(false)
        }
        var sub ColumnExpr
        _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).IsNotNull()
    case "gt", "ge", "lt", "le", "eq", "ne":
        var L, R ColumnExpr
        _ = json.Unmarshal(e.Left, &L)
        _ = json.Unmarshal(e.Right, &R)
        lc, rc := Compile(L), Compile(R)
        switch e.Type {
        case "gt":
            return Column{Name: "gt", Fn: func(row map[string]interface{}) interface{} { return cmpNumbers(lc.Fn(row), rc.Fn(row), "gt") }}
        case "ge":
            return Column{Name: "ge", Fn: func(row map[string]interface{}) interface{} { return cmpNumbers(lc.Fn(row), rc.Fn(row), "ge") }}
        case "lt":
            return Column{Name: "lt", Fn: func(row map[string]interface{}) interface{} { return cmpNumbers(lc.Fn(row), rc.Fn(row), "lt") }}
        case "le":
            return Column{Name: "le", Fn: func(row map[string]interface{}) interface{} { return cmpNumbers(lc.Fn(row), rc.Fn(row), "le") }}
        case "eq":
            return Column{Name: "eq", Fn: func(row map[string]interface{}) interface{} { return eqValues(lc.Fn(row), rc.Fn(row)) }}
        default: // "ne"
            return Column{Name: "ne", Fn: func(row map[string]interface{}) interface{} { return !eqValues(lc.Fn(row), rc.Fn(row)) }}
        }
    case "and", "or":
        var L, R ColumnExpr
        _ = json.Unmarshal(e.Left, &L)
        _ = json.Unmarshal(e.Right, &R)
        lc, rc := Compile(L), Compile(R)
        if e.Type == "and" {
            return And(lc, rc)
        }
        return Or(lc, rc)
    case "if":
        var C, T, F ColumnExpr
        _ = json.Unmarshal(e.Cond, &C)
        _ = json.Unmarshal(e.True, &T)
        _ = json.Unmarshal(e.False, &F)
        return If(Compile(C), Compile(T), Compile(F))
    case "sha256":
        var cols []ColumnExpr
        _ = json.Unmarshal(e.Cols, &cols)
        cs := make([]Column, len(cols))
        for i := range cols { cs[i] = Compile(cols[i]) }
        return Column{Name: "sha256", Fn: func(row map[string]interface{}) interface{} {
            parts := make([]string, len(cs))
            for i, c := range cs { parts[i] = fmt.Sprint(c.Fn(row)) }
            return fmt.Sprintf("%x", sha256Sum(strings.Join(parts, "")))
        }}
    case "sha512":
        var cols []ColumnExpr
        _ = json.Unmarshal(e.Cols, &cols)
        cs := make([]Column, len(cols))
        for i := range cols { cs[i] = Compile(cols[i]) }
        return Column{Name: "sha512", Fn: func(row map[string]interface{}) interface{} {
            parts := make([]string, len(cs))
            for i, c := range cs { parts[i] = fmt.Sprint(c.Fn(row)) }
            return fmt.Sprintf("%x", sha512Sum(strings.Join(parts, "")))
        }}
    case "collectlist", "collectset":
        // Row-wise returns the value; aggregation handled elsewhere.
        return Col(e.Col)
    case "split":
        // Support method payload (Expr) and legacy (Col)
        if len(e.Expr) > 0 {
            var sub ColumnExpr
            _ = json.Unmarshal(e.Expr, &sub)
            return Compile(sub).Split(e.Delimiter)
        }
        return Col(e.Col).Split(e.Delimiter)
    case "concat":
        var cols []ColumnExpr
        _ = json.Unmarshal(e.Cols, &cols)
        cs := make([]Column, len(cols))
        for i := range cols { cs[i] = Compile(cols[i]) }
        return Concat(e.Delimiter, cs...)
    case "cast":
        // Legacy payload stores sub-expression JSON as string in Col
        var sub ColumnExpr
        _ = json.Unmarshal([]byte(e.Col), &sub)
        return Compile(sub).Cast(e.Datatype)
    case "arrays_zip":
        var cols []ColumnExpr
        _ = json.Unmarshal(e.Cols, &cols)
        cs := make([]Column, len(cols))
        for i := range cols { cs[i] = Compile(cols[i]) }
        return Column{Name: "arrays_zip", Fn: func(row map[string]interface{}) interface{} {
            out := make([]interface{}, len(cs))
            for i, c := range cs { out[i] = c.Fn(row) }
            return out
        }}
    case "keys":
        // Prefer method-style Expr; fallback to legacy Col
        if len(e.Expr) > 0 {
            var sub ColumnExpr
            _ = json.Unmarshal(e.Expr, &sub)
            return Compile(sub).Keys()
        }
        return Column{Name: "keys(" + e.Col + ")", Fn: func(row map[string]interface{}) interface{} {
            val := row[e.Col]
            switch t := val.(type) {
            case map[string]interface{}:
                keys := make([]string, 0, len(t))
                for k := range t { keys = append(keys, k) }
                return keys
            case map[interface{}]interface{}:
                m := convertMapKeysToString(t)
                keys := make([]string, 0, len(m))
                for k := range m { keys = append(keys, k) }
                return keys
            default:
                return []string{}
            }
        }}
    case "lookup":
        // Method form: nested.Lookup(key)
        var left, right ColumnExpr
        _ = json.Unmarshal(e.Left, &left)   // key expr
        _ = json.Unmarshal(e.Right, &right) // nested map expr
        return Compile(right).Lookup(Compile(left))
    case "lower":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).Lower()
    case "upper":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).Upper()
    case "trim":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        c := Compile(sub)
        return Column{Name: "trim", Fn: func(row map[string]interface{}) interface{} { return strings.TrimSpace(fmt.Sprint(c.Fn(row))) }}
    case "ltrim":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        c := Compile(sub)
        return Column{Name: "ltrim", Fn: func(row map[string]interface{}) interface{} { return strings.TrimLeft(fmt.Sprint(c.Fn(row)), " \t\r\n") }}
    case "rtrim":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        c := Compile(sub)
        return Column{Name: "rtrim", Fn: func(row map[string]interface{}) interface{} { return strings.TrimRight(fmt.Sprint(c.Fn(row)), " \t\r\n") }}
    case "replace":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        // If Index (count) is provided and >0, use Replace; else ReplaceAll
        oldV, newV := fmt.Sprint(e.Old), fmt.Sprint(e.New)
        if e.Index > 0 {
            return Compile(sub).Replace(oldV, newV, e.Index)
        }
        return Compile(sub).ReplaceAll(oldV, newV)
    case "replace_all":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).ReplaceAll(fmt.Sprint(e.Old), fmt.Sprint(e.New))
    case "contains":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).Contains(fmt.Sprint(e.Substr))
    case "notcontains":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).NotContains(fmt.Sprint(e.Substr))
    case "icontains":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).IContains(fmt.Sprint(e.Substr))
    case "inotcontains":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).INotContains(fmt.Sprint(e.Substr))
    case "startswith":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).StartsWith(fmt.Sprint(e.Prefix))
    case "endswith":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).EndsWith(fmt.Sprint(e.Suffix))
    case "like":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).Like(fmt.Sprint(e.Pattern))
    case "notlike":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).NotLike(fmt.Sprint(e.Pattern))
    case "rlike":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).RLike(fmt.Sprint(e.Pattern))
    case "notrlike":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).NotRLike(fmt.Sprint(e.Pattern))
    case "regexp_replace":
        // pattern in e.Pattern, replacement in e.New (reuse existing field)
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).RegexpReplace(fmt.Sprint(e.Pattern), fmt.Sprint(e.New))
    case "regexp_extract":
        // pattern in e.Pattern, group index reuses e.Index to avoid types change
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).RegexpExtract(fmt.Sprint(e.Pattern), e.Index)
    case "length":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        return Compile(sub).Length()
    case "html_unescape":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        c := Compile(sub)
        return Column{Name: "html_unescape", Fn: func(row map[string]interface{}) interface{} { return html.UnescapeString(fmt.Sprint(c.Fn(row))) }}  
    case "array_join":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        if e.New != nil {
            return Compile(sub).ArrayJoin(e.Delimiter, fmt.Sprint(e.New))
        }
        return Compile(sub).ArrayJoin(e.Delimiter)  
    case "extract_html":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        var field string
        if e.Pattern != nil { field = fmt.Sprint(e.Pattern) } // optional
        return Compile(sub).ExtractHTML(field)
    case "extract_html_top":
        var sub ColumnExpr; _ = json.Unmarshal(e.Expr, &sub)
        var field string
        if e.Pattern != nil { field = fmt.Sprint(e.Pattern) } // optional
        return Compile(sub).ExtractHTMLTop(field)
    default:
        // Unknown -> literal nil
        return Lit(nil)
    }
}

// tiny wrappers to avoid importing crypto here if you prefer; or reuse directly
func sha256Sum(s string) [32]byte { return sha256.Sum256([]byte(s)) }
func sha512Sum(s string) [64]byte { return sha512.Sum512([]byte(s)) }

// If implements conditional logic similar to PySpark's when.
// It returns fn1 if condition returns true for a row, else fn2.
func If(condition Column, fn1 Column, fn2 Column) Column {
	return Column{
		Name: "If",
		Fn: func(row map[string]interface{}) interface{} {
			cond, ok := condition.Fn(row).(bool)
			if !ok {
				return nil
			}
			if cond {
				return fn1.Fn(row)
			}
			return fn2.Fn(row)
		},
	}
}

// IsNull returns a new Column that, when applied to a row,
// returns true if the original column value is nil, an empty string, or "null".
func (c Column) IsNull() Column {
	return Column{
		Name: c.Name + "_isnull",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			if val == nil {
				return true
			}
			switch v := val.(type) {
			case string:
				return v == "" || strings.ToLower(v) == "null"
			case *string:
				return v == nil || *v == "" || strings.ToLower(*v) == "null"
			default:
				return false
			}
		},
	}
}

// IsNotNull returns a new Column that, when applied to a row,
// returns true if the original column value is not nil, not an empty string, and not "null".
func (c Column) IsNotNull() Column {
	return Column{
		Name: c.Name + "_isnotnull",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			if val == nil {
				return false
			}
			switch v := val.(type) {
			case string:
				return !(v == "" || strings.ToLower(v) == "null")
			case *string:
				return !(v == nil || *v == "" || strings.ToLower(*v) == "null")
			default:
				return true
			}
		},
	}
}

// Gt returns a Column that compares the numeric value at col with the given threshold.
// The threshold can be of any numeric type (int, float32, float64, etc.).
func (c Column) Gt(threshold interface{}) Column {
	return Column{
		Name: c.Name + "_gt",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			fVal, err := toFloat64(val)
			if err != nil {
				return false
			}
			fThreshold, err := toFloat64(threshold)
			if err != nil {
				return false
			}
			return fVal > fThreshold
		},
	}
}

// Ge returns a Column that compares the numeric value at col with the given threshold.
// The threshold can be of any numeric type (int, float32, float64, etc.).
func (c Column) Ge(threshold interface{}) Column {
	return Column{
		Name: c.Name + "_ge",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			fVal, err := toFloat64(val)
			if err != nil {
				return false
			}
			fThreshold, err := toFloat64(threshold)
			if err != nil {
				return false
			}
			return fVal >= fThreshold
		},
	}
}

// Lt returns a Column that compares the numeric value at col with the given threshold.
// The threshold can be of any numeric type (int, float32, float64, etc.).
func (c Column) Lt(threshold interface{}) Column {
	return Column{
		Name: c.Name + "_lt",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			fVal, err := toFloat64(val)
			if err != nil {
				return false
			}
			fThreshold, err := toFloat64(threshold)
			if err != nil {
				return false
			}
			return fVal < fThreshold
		},
	}
}

// Le returns a Column that compares the numeric value at col with the given threshold.
// The threshold can be of any numeric type (int, float32, float64, etc.).
func (c Column) Le(threshold interface{}) Column {
	return Column{
		Name: c.Name + "_le",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			fVal, err := toFloat64(val)
			if err != nil {
				return false
			}
			fThreshold, err := toFloat64(threshold)
			if err != nil {
				return false
			}
			return fVal <= fThreshold
		},
	}
}

// Eq returns a Column that, when evaluated on a row,
// checks if the value from col is equal (same type and value) to threshold.
func (c Column) Eq(threshold interface{}) Column {
	return Column{
		Name: c.Name + "_eq",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			// If either is nil, return equality directly.
			if val == nil || threshold == nil {
				return val == threshold
			}
			// Check that both values are of the same type.
			if reflect.TypeOf(val) != reflect.TypeOf(threshold) {
				return false
			}
			// Use Go's native equality.
			return val == threshold
		},
	}
}

// Ne returns a Column that, when evaluated on a row,
// checks if the value from col is NOT equal (diff type or value) to threshold.
func (c Column) Ne(threshold interface{}) Column {
	return Column{
		Name: c.Name + "_ne",
		Fn: func(row map[string]interface{}) interface{} {
			val := c.Fn(row)
			// If either is nil, return equality directly.
			if val == nil || threshold == nil {
				return val != threshold
			}
			// Check that both values are of the same type.
			if reflect.TypeOf(val) != reflect.TypeOf(threshold) {
				return true
			}
			// Use Go's native equality.
			return val != threshold
		},
	}
}

// Or returns true if any of the provided conditions is true.
func Or(conds ...Column) Column {
    return Column{
        Name: "or",
        Fn: func(row map[string]interface{}) interface{} {
            for _, c := range conds {
                v, ok := c.Fn(row).(bool)
                if ok && v {
                    return true
                }
            }
            return false
        },
    }
}

// And returns true if all provided conditions are true.
func And(conds ...Column) Column {
    return Column{
        Name: "and",
        Fn: func(row map[string]interface{}) interface{} {
            for _, c := range conds {
                v, ok := c.Fn(row).(bool)
                if !ok || !v {
                    return false
                }
            }
            return true
        },
    }
}
