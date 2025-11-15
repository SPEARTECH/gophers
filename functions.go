package gophers

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"strings"
	"strconv"
)

// fastToString avoids fmt.Sprint for common types.
func fastToString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		s, _ := toString(v) // fallback to your helper
		return s
	}
}

// // functions for manipulating dataframes, take in and return dataframe
// // .iloc = select column of the dataframe by name
// func Iloc(df map[string]string, index int) map[string]string {
// 	return df
// }

// // .loc = select columns of the dataframe by index
// func Loc(df map[string]string, index int) map[string]string {
// 	return df
// }

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
// 	case "gt":
// 		var leftExpr, rightExpr ColumnExpr
// 		json.Unmarshal(expr.Left, &leftExpr)
// 		json.Unmarshal(expr.Right, &rightExpr)
// 		return Evaluate(leftExpr, row).(float64) > Evaluate(rightExpr, row).(float64)
// 	case "lt":
// 		var leftExpr, rightExpr ColumnExpr
// 		json.Unmarshal(expr.Left, &leftExpr)
// 		json.Unmarshal(expr.Right, &rightExpr)
// 		return Evaluate(leftExpr, row).(float64) < Evaluate(rightExpr, row).(float64)
// 	case "le":
// 		var leftExpr, rightExpr ColumnExpr
// 		json.Unmarshal(expr.Left, &leftExpr)
// 		json.Unmarshal(expr.Right, &rightExpr)
// 		return Evaluate(leftExpr, row).(float64) <= Evaluate(rightExpr, row).(float64)
// 	case "ge":
// 		var leftExpr, rightExpr ColumnExpr
// 		json.Unmarshal(expr.Left, &leftExpr)
// 		json.Unmarshal(expr.Right, &rightExpr)
// 		return Evaluate(leftExpr, row).(float64) >= Evaluate(rightExpr, row).(float64)
// 	case "eq":
// 		var leftExpr, rightExpr ColumnExpr
// 		json.Unmarshal(expr.Left, &leftExpr)
// 		json.Unmarshal(expr.Right, &rightExpr)
// 		return Evaluate(leftExpr, row).(float64) == Evaluate(rightExpr, row).(float64)
// 	case "ne":
// 		var leftExpr, rightExpr ColumnExpr
// 		json.Unmarshal(expr.Left, &leftExpr)
// 		json.Unmarshal(expr.Right, &rightExpr)
// 		return Evaluate(leftExpr, row).(float64) != Evaluate(rightExpr, row).(float64)
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

// 	default:
// 		return nil
// 	}
// }

// Col returns a Column for the specified column name.
func Col(name string) Column {
	return Column{
		Name: fmt.Sprintf("Col(%s)", name),
		Fn: func(row map[string]interface{}) interface{} {
			return row[name]
		},
	}
}

// Lit returns a Column that always returns the provided literal value.
func Lit(value interface{}) Column {
	return Column{
		Name: "lit",
		Fn: func(row map[string]interface{}) interface{} {
			return value
		},
	}
}

// CollectList returns a Column that is an array of the given column's values.
func CollectList(name string) Column {
	return Column{
		Name: name,
		Fn: func(row map[string]interface{}) interface{} {
			src := row[name].([]interface{})
			out := make([]interface{}, len(src))
			copy(out, src)
			return out		
		},
	}
}

// CollectSet returns a Column that is a set of unique values from the given column.
func CollectSet(name string) Column {
	return Column{
		Name: fmt.Sprintf("CollectSet(%s)", name),
		Fn: func(row map[string]interface{}) interface{} {
			src := row[name].([]interface{})
			seen := make(map[interface{}]struct{}, len(src))
			out := make([]interface{}, 0, len(src))
			for _, v := range src {
				if _, ok := seen[v]; ok {
					continue
				}
				seen[v] = struct{}{}
				out = append(out, v) // preserves first-seen order
			}
			return out
		},
	}
}

// datetime

// epoch

// SHA256 returns a Column that concatenates the values of the specified columns,
// computes the SHA-256 checksum of the concatenated string, and returns it as a string.
func SHA256(cols ...Column) Column {
	return Column{
		Name: "SHA256",
		Fn: func(row map[string]interface{}) interface{} {
			var b strings.Builder
			// heuristic reserve
			b.Grow(16 * len(cols))
			for _, col := range cols {
				b.WriteString(fastToString(col.Fn(row)))
			}
			hash := sha256.Sum256([]byte(b.String()))			
			return hex.EncodeToString(hash[:])
		},
	}
}

// SHA512 returns a Column that concatenates the values of the specified columns,
// computes the SHA-512 checksum of the concatenated string, and returns it as a string.
func SHA512(cols ...Column) Column {
	return Column{
		Name: "SHA512",
		Fn: func(row map[string]interface{}) interface{} {
			var b strings.Builder
			b.Grow(16 * len(cols))
			for _, col := range cols {
				b.WriteString(fastToString(col.Fn(row)))
			}
			hash := sha512.Sum512([]byte(b.String()))
			return hex.EncodeToString(hash[:])
		},
	}
}

// from_json ? *

// // Split returns a Column that splits the string value of the specified column by the given delimiter.
// func Split(name string, delimiter string) Column {
// 	return Column{
// 		Name: fmt.Sprintf("Split(%s, %s)", name, delimiter),
// 		Fn: func(row map[string]interface{}) interface{} {
// 			switch v := row[name].(type) {
// 			case string:
// 				return strings.Split(v, delimiter)
// 			default:
// 				s := fastToString(v)
// 				if s == "" {
// 					return []string{}
// 				}
// 				return strings.Split(s, delimiter)
// 			}
// 		},
// 	}
// }

// Split splits a column or expression result by delimiter.
// Accepts either a column name (string) or a Column expression.
func Split(col interface{}, delimiter string) Column {
    var eval func(row map[string]interface{}) interface{}
    var label string

    switch v := col.(type) {
    case string:
        label = v
        eval = func(row map[string]interface{}) interface{} { return row[v] }
    case Column:
        label = v.Name
        eval = v.Fn
    default:
        label = fmt.Sprintf("%v", v)
        eval = func(row map[string]interface{}) interface{} { return row[label] }
    }

    return Column{
        Name: fmt.Sprintf("Split(%s, %s)", label, delimiter),
        Fn: func(row map[string]interface{}) interface{} {
            val := eval(row)
            s, err := toString(val)
            if err != nil {
                return []string{}
            }
            return strings.Split(s, delimiter)
        },
    }
}

// Keys returns a Column that extracts the keys from the nested map (top level only)
// found in the specified column.
func Keys(name string) Column {
	return Column{
		Name: fmt.Sprintf("Keys(%s)", name),
		Fn: func(row map[string]interface{}) interface{} {
			val := row[name]
			if val == nil {
				return []string{}
			}
			switch t := val.(type) {
			case map[string]interface{}:
				keys := make([]string, 0, len(t))
				for k := range t { keys = append(keys, k) }
				return keys
			case map[interface{}]interface{}:
				nested := convertMapKeysToString(t)
				keys := make([]string, 0, len(nested))
				for k := range nested { keys = append(keys, k) }
				return keys
			default:
				return []string{}
			}

		},
	}
}

// Lookup returns a Column that extracts the value from a nested map in the column nestCol
// using the string value produced by keyExpr (which can be either a column reference or a literal).
func Lookup(keyExpr Column, nestCol string) Column {
	return Column{
		Name: fmt.Sprintf("Lookup(%s, %s)", nestCol, keyExpr.Name),
		Fn: func(row map[string]interface{}) interface{} {
			// Evaluate the key expression.
			keyVal := fastToString(keyExpr.Fn(row))
			// Get the nested map from nestCol.
			nestedVal := row[nestCol]
			if nestedVal == nil {
				return nil
			}
			switch n := nestedVal.(type) {
			case map[string]interface{}:
				return n[keyVal]
			case map[interface{}]interface{}:
				m := convertMapKeysToString(n)
				return m[keyVal]
			default:
				return nil
			}
		},
	}
}

// pivot (row to column) *

// replace

// regexp_replace *

// starts with

// ends with

// contains *

// like - sql %%

// rlike - regex

// regexp

// sort_values()

// map()

// apply()

// query()

// rolling()

// isin()

// astype()

// ToDatetime() *

// DateFormat() ? *

// ToDate() *

// DateDiff() *

// ToEpoch() *

// FromEpoch()

// sql?

// Concat returns a Column that, when applied to a row,
// concatenates the string representations of the provided Columns using the specified delimiter.
// It converts each value to a string using toString. If conversion fails for a value, it uses an empty string.
func Concat(delim string, cols ...Column) Column {
	return Column{
		Name: "concat_ws",
		Fn: func(row map[string]interface{}) interface{} {
			var parts []string
			for _, col := range cols {
				val := col.Fn(row)
				str, err := toString(val)
				if err != nil {
					str = ""
				}
				parts = append(parts, str)
			}
			return strings.Join(parts, delim)
		},
	}
}

// Cast takes in an existing Column and a desired datatype ("int", "float", "string"),
// and returns a new Column that casts the value returned by the original Column to that datatype.
func Cast(col Column, datatype string) Column {
    return Column{
        Name: col.Name + "_cast",
        Fn: func(row map[string]interface{}) interface{} {
            val := col.Fn(row)
            switch datatype {
            case "int":
                // silent on nil
                if val == nil {
                    return 0
                }
                casted, err := toInt(val)
                if err != nil {
                    fmt.Printf("cast to int error: %v\n", err)
                    return nil
                }
                return casted
            case "float":
                // silent on nil
                if val == nil {
                    return 0.0
                }
                casted, err := toFloat64(val)
                if err != nil {
                    fmt.Printf("cast to float error: %v\n", err)
                    return nil
                }
                return casted
            case "string":
                // silent on nil
                if val == nil {
                    return ""
                }
                casted, err := toString(val)
                if err != nil {
                    fmt.Printf("cast to string error: %v\n", err)
                    return nil
                }
                return casted
            default:
                fmt.Printf("unsupported cast type: %s\n", datatype)
                return nil
            }
        },
    }
}
// toFloat64 attempts to convert an interface{} to a float64.
func toFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf("unsupported numeric type: %T", val)
	}
}

// toInt tries to convert the provided value to an int.
// It supports int, int32, int64, float32, float64, and string.
func toInt(val interface{}) (int, error) {
	switch v := val.(type) {
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case float32:
		return int(v), nil
	case float64:
		return int(v), nil
	case string:
		i, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string %q to int: %v", v, err)
		}
		return i, nil
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}


// toString attempts to convert an interface{} to a string.
// It supports string, int, int32, int64, float32, and float64.
func toString(val interface{}) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case int:
		return strconv.Itoa(v), nil
	case int32:
		return strconv.Itoa(int(v)), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	default:
		return "", fmt.Errorf("unsupported type %T", val)
	}
}

