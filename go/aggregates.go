package gophers

import (
	"fmt"
	"sort"
)

// Agg converts multiple Column functions to a slice of Aggregation structs for use in aggregation.
func Agg(cols ...Column) []Aggregation {
	aggs := []Aggregation{}
	for _, col := range cols {
		colName := col.Name
		agg := Aggregation{
			ColumnName: colName,
			Fn: func(vals []interface{}) interface{} {
				// Create a map with the column name as key and the first value
				// We're using a dummy map just to match the expected type
				dummyRow := make(map[string]interface{})
				// Put all values in the map under the column name
				dummyRow[colName] = vals[0] // Use just the first value for simplicity

				// Call the Column's function with this map
				return col.Fn(dummyRow)
			},
		}
		aggs = append(aggs, agg)
	}
	return aggs
}

// Sum returns an Aggregation that sums numeric values from the specified column.
func Sum(name string) Aggregation {
	return Aggregation{
		ColumnName: name,
		Fn: func(vals []interface{}) interface{} {
			sum := 0.0
			for _, v := range vals {
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

// Max returns an Aggregation that finds the maximum numeric value from the specified column.
func Max(name string) Aggregation {
	return Aggregation{
		ColumnName: name,
		Fn: func(vals []interface{}) interface{} {
			maxSet := false
			var max float64
			for _, v := range vals {
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

// Min returns an Aggregation that finds the minimum numeric value from the specified column.
func Min(name string) Aggregation {
	return Aggregation{
		ColumnName: name,
		Fn: func(vals []interface{}) interface{} {
			minSet := false
			var min float64
			for _, v := range vals {
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

// Median returns an Aggregation that finds the median numeric value from the specified column.
func Median(name string) Aggregation {
	return Aggregation{
		ColumnName: name,
		Fn: func(vals []interface{}) interface{} {
			var nums []float64
			for _, v := range vals {
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

// Mean returns an Aggregation that calculates the mean (average) of numeric values from the specified column.
func Mean(name string) Aggregation {
	return Aggregation{
		ColumnName: name,
		Fn: func(vals []interface{}) interface{} {
			sum := 0.0
			count := 0
			for _, v := range vals {
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

// Mode returns an Aggregation that finds the mode (most frequent value) among the numeric values from the specified column.
func Mode(name string) Aggregation {
	return Aggregation{
		ColumnName: name,
		Fn: func(vals []interface{}) interface{} {
			// Use a map to count frequencies.
			freq := make(map[float64]int)
			var mode float64
			maxCount := 0

			for _, v := range vals {
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

// Unique returns an Aggregation that counts the number of unique values from the specified column.
func Unique(name string) Aggregation {
	return Aggregation{
		ColumnName: name,
		Fn: func(vals []interface{}) interface{} {
			uniqueSet := make(map[interface{}]bool)
			for _, v := range vals {
				uniqueSet[v] = true
			}
			return len(uniqueSet)
		},
	}
}

// First returns an Aggregation that gets the first value from the specified column.
func First(name string) Aggregation {
	return Aggregation{
		ColumnName: name,
		Fn: func(vals []interface{}) interface{} {
			if len(vals) == 0 {
				return nil
			}
			return vals[0]
		},
	}
}
