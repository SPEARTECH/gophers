package gophers

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"strings"
	"strconv"
	"regexp"
	"unicode/utf8"
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

// func CollectList(name string) Column {
//     return Column{
//         Name: fmt.Sprintf("CollectList(%s)", name),
//         Fn: func(row map[string]interface{}) interface{} {
//             v := row[name]
//             switch t := v.(type) {
//             case nil:
//                 return []interface{}{}
//             case []interface{}:
//                 out := make([]interface{}, len(t))
//                 copy(out, t)
//                 return out
//             case []string:
//                 out := make([]interface{}, len(t))
//                 for i, s := range t { out[i] = s }
//                 return out
//             default:
//                 // source is a scalar (e.g., string). Wrap as a single-element list.
//                 return []interface{}{t}
//             }
//         },
//     }
// }

// func CollectSet(name string) Column {
//     return Column{
//         Name: fmt.Sprintf("CollectSet(%s)", name),
//         Fn: func(row map[string]interface{}) interface{} {
//             v := row[name]
//             switch t := v.(type) {
//             case nil:
//                 return []interface{}{}
//             case []interface{}:
//                 seen := make(map[interface{}]struct{}, len(t))
//                 out := make([]interface{}, 0, len(t))
//                 for _, x := range t {
//                     if _, ok := seen[x]; ok { continue }
//                     seen[x] = struct{}{}
//                     out = append(out, x)
//                 }
//                 return out
//             case []string:
//                 seen := make(map[string]struct{}, len(t))
//                 out := make([]interface{}, 0, len(t))
//                 for _, s := range t {
//                     if _, ok := seen[s]; ok { continue }
//                     seen[s] = struct{}{}
//                     out = append(out, s)
//                 }
//                 return out
//             default:
//                 // scalar -> single-element set
//                 return []interface{}{t}
//             }
//         },
//     }
// }
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
// Column method for chaining: Col("x").Split(delim)
func (c Column) Split(delimiter string) Column {
    return Column{
        Name: fmt.Sprintf("%s.Split(%q)", c.Name, delimiter),
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            s, err := toString(val)
            if err != nil { s = fastToString(val) }
            if strings.TrimSpace(s) == "" {
                return []string{}
            }
            return strings.Split(s, delimiter)
        },
    }
}
// func Split(col interface{}, delimiter string) Column {
//     var eval func(row map[string]interface{}) interface{}
//     var label string

//     switch v := col.(type) {
//     case string:
//         label = v
//         eval = func(row map[string]interface{}) interface{} { return row[v] }
//     case Column:
//         label = v.Name
//         eval = v.Fn
//     default:
//         label = fmt.Sprintf("%v", v)
//         eval = func(row map[string]interface{}) interface{} { return row[label] }
//     }

//     return Column{
//         Name: fmt.Sprintf("Split(%s, %s)", label, delimiter),
//         Fn: func(row map[string]interface{}) interface{} {
//             val := eval(row)
//             s, err := toString(val)
//             if err != nil {
//                 return []string{}
//             }
//             return strings.Split(s, delimiter)
//         },
//     }
// }



// Index picks the i-th element from an array column expression.
// Returns "" if out of range or not an array.
func (c Column) Index(i int) Column {
    return Column{
        Name: fmt.Sprintf("%s[%d]", c.Name, i),
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            switch v := val.(type) {
            case []string:
                if i >= 0 && i < len(v) { return v[i] }
                return ""
            case []interface{}:
                if i >= 0 && i < len(v) {
                    s, err := toString(v[i])
                    if err != nil { return fastToString(v[i]) }
                    return s
                }
                return ""
            default:
                return ""
            }
        },
    }
}

// Keys returns a Column that extracts the keys from the nested map (top level only)
// found in the specified column.
// Column method: Col("x").Keys()
func (c Column) Keys() Column {
    return Column{
        Name: fmt.Sprintf("%s.Keys()", c.Name),
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            if val == nil {
                return []string{}
            }
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
        },
    }
}
// func Keys(name string) Column {
// 	return Column{
// 		Name: fmt.Sprintf("Keys(%s)", name),
// 		Fn: func(row map[string]interface{}) interface{} {
// 			val := row[name]
// 			if val == nil {
// 				return []string{}
// 			}
// 			switch t := val.(type) {
// 			case map[string]interface{}:
// 				keys := make([]string, 0, len(t))
// 				for k := range t { keys = append(keys, k) }
// 				return keys
// 			case map[interface{}]interface{}:
// 				nested := convertMapKeysToString(t)
// 				keys := make([]string, 0, len(nested))
// 				for k := range nested { keys = append(keys, k) }
// 				return keys
// 			default:
// 				return []string{}
// 			}

// 		},
// 	}
// }

// Lookup returns a Column that extracts the value from a nested map in the column nestCol
// using the string value produced by keyExpr (which can be either a column reference or a literal).
// Column method: Col("nested").Lookup(Col("key"))
func (c Column) Lookup(keyExpr Column) Column {
    return Column{
        Name: fmt.Sprintf("%s.Lookup(%s)", c.Name, keyExpr.Name),
        Fn: func(row map[string]interface{}) interface{} {
            keyVal := fastToString(keyExpr.Fn(row))
            nestedVal := c.Fn(row)
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
// func Lookup(keyExpr Column, nestCol string) Column {
// 	return Column{
// 		Name: fmt.Sprintf("Lookup(%s, %s)", nestCol, keyExpr.Name),
// 		Fn: func(row map[string]interface{}) interface{} {
// 			// Evaluate the key expression.
// 			keyVal := fastToString(keyExpr.Fn(row))
// 			// Get the nested map from nestCol.
// 			nestedVal := row[nestCol]
// 			if nestedVal == nil {
// 				return nil
// 			}
// 			switch n := nestedVal.(type) {
// 			case map[string]interface{}:
// 				return n[keyVal]
// 			case map[interface{}]interface{}:
// 				m := convertMapKeysToString(n)
// 				return m[keyVal]
// 			default:
// 				return nil
// 			}
// 		},
// 	}
// }

// pivot (row to column) *

// Replace replaces up to n occurrences of old with new in the column's string value.
func (c Column) Replace(old, new string, n int) Column {
    return Column{
        Name: fmt.Sprintf("%s.Replace(%q,%q,%d)", c.Name, old, new, n),
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            s, err := toString(val)
            if err != nil {
                s = fastToString(val)
            }
            return strings.Replace(s, old, new, n)
        },
    }
}

// ReplaceAll replaces all occurrences of old with new in the column's string value.
func (c Column) ReplaceAll(old, new string) Column {
    return Column{
        Name: fmt.Sprintf("%s.ReplaceAll(%q,%q)", c.Name, old, new),
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            s, err := toString(val)
            if err != nil {
                s = fastToString(val)
            }
            return strings.ReplaceAll(s, old, new)
        },
    }
}

// RegexpReplace replaces all matches of pattern with replacement.
// Accepts either a column name (string) or a Column expression (use Lit(...) for literals).
// Column method variant
func (c Column) RegexpReplace(pattern, replacement string) Column {
    re := regexp.MustCompile(pattern)
    return Column{
        Name: fmt.Sprintf("%s.regexp_replace(%q,%q)", c.Name, pattern, replacement),
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            s, err := toString(val)
            if err != nil {
                s = fastToString(val)
            }
            return re.ReplaceAllString(s, replacement)
        },
    }
}
// func RegexpReplace(input interface{}, pattern, replacement string) Column {
//     re := regexp.MustCompile(pattern)

//     var eval func(row map[string]interface{}) interface{}
//     var label string
//     switch v := input.(type) {
//     case string:
//         label = v
//         eval = func(row map[string]interface{}) interface{} { return row[v] }
//     case Column:
//         label = v.Name
//         eval = v.Fn
//     default:
//         label = fmt.Sprintf("%v", v)
//         eval = func(row map[string]interface{}) interface{} { return row[label] }
//     }

//     return Column{
//         Name: fmt.Sprintf("regexp_replace(%s,%q,%q)", label, pattern, replacement),
//         Fn: func(row map[string]interface{}) interface{} {
//             val := eval(row)
//             s, err := toString(val)
//             if err != nil {
//                 s = fastToString(val)
//             }
//             return re.ReplaceAllString(s, replacement)
//         },
//     }
// }

// // Column method variant
// func (c Column) RegexpReplace(pattern, replacement string) Column {
//     re := regexp.MustCompile(pattern)
//     return Column{
//         Name: fmt.Sprintf("%s.regexp_replace(%q,%q)", c.Name, pattern, replacement),
//         Fn: func(row map[string]interface{}) interface{} {
//             val := c.Fn(row)
//             s, err := toString(val)
//             if err != nil {
//                 s = fastToString(val)
//             }
//             return re.ReplaceAllString(s, replacement)
//         },
//     }
// }


// Contains (case-sensitive) on Column
func (c Column) Contains(substr string) Column {
    return Column{
        Name: fmt.Sprintf("%s.Contains(%q)", c.Name, substr),
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            s, err := toString(val)
            if err != nil {
                s = fastToString(val)
            }
            return strings.Contains(s, substr)
        },
    }
}

// Case-insensitive variant
func (c Column) IContains(substr string) Column {
    needle := strings.ToLower(substr)
    return Column{
        Name: fmt.Sprintf("%s.IContains(%q)", c.Name, substr),
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            s, err := toString(val)
            if err != nil {
                s = fastToString(val)
            }
            return strings.Contains(strings.ToLower(s), needle)
        },
    }
}

func (c Column) NotContains(substr string) Column {
    return Column{
        Name: fmt.Sprintf("%s.NotContains(%q)", c.Name, substr),
        Fn: func(row map[string]interface{}) interface{} {
            s, err := toString(c.Fn(row)); if err != nil { s = fastToString(c.Fn(row)) }
            return !strings.Contains(s, substr)
        },
    }
}
func (c Column) INotContains(substr string) Column {
    needle := strings.ToLower(substr)
    return Column{
        Name: fmt.Sprintf("%s.INotContains(%q)", c.Name, substr),
        Fn: func(row map[string]interface{}) interface{} {
            s, err := toString(c.Fn(row)); if err != nil { s = fastToString(c.Fn(row)) }
            return !strings.Contains(strings.ToLower(s), needle)
        },
    }
}

// StartsWith returns a Column that checks if input starts with prefix.
func (c Column) StartsWith(prefix string) Column {
    return Column{
        Name: c.Name + ".startswith",
        Fn: func(row map[string]interface{}) interface{} {
            s, err := toString(c.Fn(row))
            if err != nil { s = fastToString(c.Fn(row)) }
            return strings.HasPrefix(s, prefix)
        },
    }
}

// EndsWith returns a Column that checks if input ends with suffix.
func (c Column) EndsWith(suffix string) Column {
    return Column{
        Name: c.Name + ".endswith",
        Fn: func(row map[string]interface{}) interface{} {
            s, err := toString(c.Fn(row))
            if err != nil { s = fastToString(c.Fn(row)) }
            return strings.HasSuffix(s, suffix)
        },
    }
}



// Like (SQL-style) with % (any) and _ (single) wildcards.
func (c Column) Like(pattern string) Column {
    re := regexp.MustCompile(sqlLikeToRegex(pattern))
    return Column{
        Name: fmt.Sprintf("%s.like", c.Name),
        Fn: func(row map[string]interface{}) interface{} {
            s, err := toString(c.Fn(row))
            if err != nil { s = fastToString(c.Fn(row)) }
            return re.MatchString(s)
        },
    }
}

func (c Column) NotLike(pattern string) Column {
    re := regexp.MustCompile(sqlLikeToRegex(pattern))
    return Column{
        Name: fmt.Sprintf("%s.notlike", c.Name),
        Fn: func(row map[string]interface{}) interface{} {
            s, err := toString(c.Fn(row))
            if err != nil { s = fastToString(c.Fn(row)) }
            return !re.MatchString(s)
        },
    }
}

// RLike (regex). Pass (?i) in pattern for case-insensitive.
func (c Column) RLike(pattern string) Column {
    re := regexp.MustCompile(pattern)
    return Column{
        Name: c.Name + ".rlike",
        Fn: func(row map[string]interface{}) interface{} {
            s, err := toString(c.Fn(row))
            if err != nil { s = fastToString(c.Fn(row)) }
            return re.MatchString(s)
        },
    }
}

func (c Column) NotRLike(pattern string) Column {
    re := regexp.MustCompile(pattern)
    return Column{
        Name: c.Name + ".notrlike",
        Fn: func(row map[string]interface{}) interface{} {
            s, err := toString(c.Fn(row))
            if err != nil { s = fastToString(c.Fn(row)) }
            return !re.MatchString(s)
        },
    }
}

// helper: convert SQL LIKE to anchored regex
func sqlLikeToRegex(pat string) string {
    var b strings.Builder
    b.WriteString("^")
    for _, r := range pat {
        switch r {
        case '%':
            b.WriteString(".*")
        case '_':
            b.WriteString(".")
        case '.', '+', '*', '?', '^', '$', '(', ')', '[', ']', '{', '}', '|', '\\':
            b.WriteByte('\\')
            b.WriteRune(r)
        default:
            b.WriteRune(r)
        }
    }
    b.WriteString("$")
    return b.String()
}

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

// Lower returns a Column that lowercases the input (column or expression).
func (c Column) Lower() Column {
    return Column{
        Name: c.Name + ".lower",
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            s, err := toString(val)
            if err != nil { s = fastToString(val) }
            return strings.ToLower(s)
        },
    }
}

// Upper returns a Column that uppercases the input (column or expression).
func (c Column) Upper() Column {
    return Column{
        Name: c.Name + ".upper",
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            s, err := toString(val)
            if err != nil { s = fastToString(val) }
            return strings.ToUpper(s)
        },
    }
}

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
func (c Column) Cast(datatype string) Column {
    return Column{
        Name: c.Name + "_cast",
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            switch datatype {
            case "int":
                if val == nil { return 0 }
                casted, err := toInt(val)
                if err != nil {
                    fmt.Printf("cast to int error: %v\n", err)
                    return nil
                }
                return casted
            case "float":
                if val == nil { return 0.0 }
                casted, err := toFloat64(val)
                if err != nil {
                    fmt.Printf("cast to float error: %v\n", err)
                    return nil
                }
                return casted
            case "string":
                if val == nil { return "" }
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

// Length returns the character count of a column/expression (Unicode-aware).
func (c Column) Length() Column {
    return Column{
        Name: c.Name + ".length",
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            s, err := toString(val)
            if err != nil { s = fastToString(val) }
            return utf8.RuneCountInString(s)
        },
    }
}

// ArrayJoin joins elements of an array column with a delimiter.
// If nullReplacement is provided, nulls are replaced with it; otherwise nulls are skipped.
func (c Column) ArrayJoin(delim string, nullReplacement ...string) Column {
    return Column{
        Name: fmt.Sprintf("%s.array_join(%q)", c.Name, delim),
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            switch arr := val.(type) {
            case []string:
                return strings.Join(arr, delim)
            case []interface{}:
                parts := make([]string, 0, len(arr))
                for _, x := range arr {
                    if x == nil {
                        if len(nullReplacement) > 0 {
                            parts = append(parts, nullReplacement[0])
                        }
                        // skip nil if no replacement
                        continue
                    }
                    s, err := toString(x)
                    if err != nil { s = fastToString(x) }
                    parts = append(parts, s)
                }
                return strings.Join(parts, delim)
            default:
                s, err := toString(val)
                if err != nil { s = fastToString(val) }
                return s
            }
        },
    }
}

// ExtractHTML parses the HTML in a column/expression and returns []string:
// - If field is a known output column, returns that column for all elements.
// - Otherwise, treats field as a tag name and returns inner_html_str for matching elements.
// Column method: parse HTML and extract values
func (c Column) ExtractHTML(field string) Column {
    known := map[string]struct{}{
        "outer_html_str": {}, "inner_html_str": {}, "text": {}, "tag": {},
        "href": {}, "src": {}, "href_abs": {}, "src_abs": {},
    }
    if field == "" {
        field = "inner_html_str"
    }

    return Column{
        Name: fmt.Sprintf("%s.ExtractHTML(%s)", c.Name, field),
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            htmlStr, err := toString(val)
            if err != nil || strings.TrimSpace(htmlStr) == "" {
                return []string{}
            }

            df := ReadHTML(htmlStr)

            // Direct column return if field is known
            if _, ok := known[field]; ok {
                col := df.Data[field]
                if col == nil {
                    return []string{}
                }
                out := make([]string, len(col))
                for i := range col {
                    s, err := toString(col[i])
                    if err != nil { s = fastToString(col[i]) }
                    out[i] = s
                }
                return out
            }

            // Otherwise, field is a tag filter; return inner_html_str of matching tag
            tags := df.Data["tag"]
            inn := df.Data["inner_html_str"]
            if tags == nil || inn == nil {
                return []string{}
            }
            out := make([]string, 0, df.Rows)
            for i := 0; i < df.Rows; i++ {
                ts, _ := toString(tags[i])
                if ts == field {
                    s, _ := toString(inn[i])
                    out = append(out, s)
                }
            }
            return out
        },
    }
}

// ExtractHTMLTop parses only top-level elements and returns []string:
// - If field is a known output column, returns that column for top-level elements.
// - Otherwise, treats field as a tag name and returns inner_html_str for matching top-level tag.
// Column method: top-level elements only
func (c Column) ExtractHTMLTop(field string) Column {
    known := map[string]struct{}{
        "outer_html_str": {}, "inner_html_str": {}, "text": {}, "tag": {},
        "href": {}, "src": {}, "href_abs": {}, "src_abs": {},
    }
    if field == "" {
        field = "inner_html_str"
    }

    return Column{
        Name: fmt.Sprintf("%s.ExtractHTMLTop(%s)", c.Name, field),
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            htmlStr, err := toString(val)
            if err != nil || strings.TrimSpace(htmlStr) == "" {
                return []string{}
            }

            df := ReadHTMLTop(htmlStr)

            if _, ok := known[field]; ok {
                col := df.Data[field]
                if col == nil {
                    return []string{}
                }
                out := make([]string, len(col))
                for i := range col {
                    s, err := toString(col[i])
                    if err != nil { s = fastToString(col[i]) }
                    out[i] = s
                }
                return out
            }

            tags := df.Data["tag"]
            inn := df.Data["inner_html_str"]
            if tags == nil || inn == nil {
                return []string{}
            }
            out := make([]string, 0, df.Rows)
            for i := 0; i < df.Rows; i++ {
                ts, _ := toString(tags[i])
                if ts == field {
                    s, _ := toString(inn[i])
                    out = append(out, s)
                }
            }
            return out
        },
    }
}
// RegexpExtract extracts the specified capture group from the first regex match.
// group = 0 returns the entire match; >0 returns that capture group.
// If no match or group out of range, returns "".
func (c Column) RegexpExtract(pattern string, group int) Column {
    re := regexp.MustCompile(pattern)
    return Column{
        Name: fmt.Sprintf("%s.regexp_extract(%q,%d)", c.Name, pattern, group),
        Fn: func(row map[string]interface{}) interface{} {
            val := c.Fn(row)
            s, err := toString(val)
            if err != nil { s = fastToString(val) }
            m := re.FindStringSubmatch(s)
            if len(m) == 0 { return "" }
            if group < 0 || group >= len(m) { return "" }
            return m[group]
        },
    }
}