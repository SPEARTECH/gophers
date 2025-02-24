package gophers

import (
	"fmt"
	"sort"
)

// AggregatorFn defines a function that aggregates a slice of values.
type AggregatorFn func([]interface{}) interface{}

// Aggregation holds a target column name and the aggregation function to apply.
type Aggregation struct {
	ColumnName string
	Fn         AggregatorFn
}

// Agg converts multiple Column functions to a slice of Aggregation structs for use in aggregation.
func Agg(cols ...Column) []Aggregation {
	aggs := []Aggregation{}
	for _, col := range cols {
		agg := Aggregation{
			ColumnName: col.Name,
			Fn: func(vals []interface{}) interface{} {
				row := make(map[string]interface{})
				row[col.Name] = vals
				return col.Fn(row)
			},
		}
		aggs = append(aggs, agg)
	}
	return aggs
}

// Sum returns a Column that sums numeric values from the specified column.
func Sum(name string) Column {
	return Column{
		Name: name,
		Fn: func(row map[string]interface{}) interface{} {
			val, ok := row[name]
			if !ok || val == nil {
				return 0.0
			}
			sum := 0.0
			for _, v := range val.([]interface{}) {
				fVal, err := toFloat64(v)
				if err != nil {
					fmt.Printf("sum conversion error: %v\n", err)
					continue
				}
				sum += fVal
			}
			return sum
		},
	}
}

// Max returns a Column that finds the maximum numeric value from the specified column.
func Max(name string) Column {
	return Column{
		Name: name,
		Fn: func(row map[string]interface{}) interface{} {
			val, ok := row[name]
			if !ok || val == nil {
				return nil
			}
			maxSet := false
			var max float64
			for _, v := range val.([]interface{}) {
				fVal, err := toFloat64(v)
				if err != nil {
					fmt.Printf("max conversion error: %v\n", err)
					continue
				}
				if !maxSet || fVal > max {
					max = fVal
					maxSet = true
				}
			}
			if !maxSet {
				return nil
			}
			return max
		},
	}
}

// Min returns a Column that finds the minimum numeric value from the specified column.
func Min(name string) Column {
	return Column{
		Name: name,
		Fn: func(row map[string]interface{}) interface{} {
			val, ok := row[name]
			if !ok || val == nil {
				return nil
			}
			minSet := false
			var min float64
			for _, v := range val.([]interface{}) {
				fVal, err := toFloat64(v)
				if err != nil {
					fmt.Printf("min conversion error: %v\n", err)
					continue
				}
				if !minSet || fVal < min {
					min = fVal
					minSet = true
				}
			}
			if !minSet {
				return nil
			}
			return min
		},
	}
}

// Median returns a Column that finds the median numeric value from the specified column.
func Median(name string) Column {
	return Column{
		Name: name,
		Fn: func(row map[string]interface{}) interface{} {
			val, ok := row[name]
			if !ok || val == nil {
				return nil
			}
			var nums []float64
			for _, v := range val.([]interface{}) {
				fVal, err := toFloat64(v)
				if err != nil {
					fmt.Printf("median conversion error: %v\n", err)
					continue
				}
				nums = append(nums, fVal)
			}

			n := len(nums)
			if n == 0 {
				return nil
			}

			// Sort the numbers.
			sort.Float64s(nums)

			if n%2 == 1 {
				// Odd count; return middle element.
				return nums[n/2]
			}
			// Even count; return average of two middle elements.
			median := (nums[n/2-1] + nums[n/2]) / 2.0
			return median
		},
	}
}

// Mean returns a Column that calculates the mean (average) of numeric values from the specified column.
func Mean(name string) Column {
	return Column{
		Name: name,
		Fn: func(row map[string]interface{}) interface{} {
			val, ok := row[name]
			if !ok || val == nil {
				return nil
			}
			sum := 0.0
			count := 0
			for _, v := range val.([]interface{}) {
				fVal, err := toFloat64(v)
				if err != nil {
					fmt.Printf("mean conversion error: %v\n", err)
					continue
				}
				sum += fVal
				count++
			}
			if count == 0 {
				return nil
			}
			return sum / float64(count)
		},
	}
}

// Mode returns a Column that finds the mode (most frequent value) among the numeric values from the specified column.
func Mode(name string) Column {
	return Column{
		Name: name,
		Fn: func(row map[string]interface{}) interface{} {
			val, ok := row[name]
			if !ok || val == nil {
				return nil
			}
			// Use a map to count frequencies.
			freq := make(map[float64]int)
			var mode float64
			maxCount := 0

			for _, v := range val.([]interface{}) {
				fVal, err := toFloat64(v)
				if err != nil {
					fmt.Printf("mode conversion error: %v\n", err)
					continue
				}
				freq[fVal]++
				if freq[fVal] > maxCount {
					maxCount = freq[fVal]
					mode = fVal
				}
			}
			// If no valid values, return nil.
			if maxCount == 0 {
				return nil
			}
			return mode
		},
	}
}

// Unique returns a Column that counts the number of unique values from the specified column.
func Unique(name string) Column {
	return Column{
		Name: name,
		Fn: func(row map[string]interface{}) interface{} {
			val, ok := row[name]
			if !ok || val == nil {
				return 0
			}
			uniqueSet := make(map[interface{}]bool)
			for _, v := range val.([]interface{}) {
				uniqueSet[v] = true
			}
			return len(uniqueSet)
		},
	}
}

// First returns a Column that gets the first value from the specified column.
func First(name string) Column {
	return Column{
		Name: name,
		Fn: func(row map[string]interface{}) interface{} {
			val, ok := row[name]
			if !ok || val == nil {
				return nil
			}
			if len(val.([]interface{})) == 0 {
				return nil
			}
			return val.([]interface{})[0]
		},
	}
}
