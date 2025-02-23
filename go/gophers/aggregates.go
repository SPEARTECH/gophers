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

// Sum returns an Aggregation for summing numeric values from the specified column.
func Sum(col string) Aggregation {
	return Aggregation{
		ColumnName: col,
		Fn:         SumAggregator,
	}
}

// Sum returns an Aggregation for summing numeric values from the specified column.
func Max(col string) Aggregation {
	return Aggregation{
		ColumnName: col,
		Fn:         MaxAggregator,
	}
}

// Min returns an Aggregation for smallest numeric values from the specified column.
func Min(col string) Aggregation {
	return Aggregation{
		ColumnName: col,
		Fn:         MinAggregator,
	}
}

// Median returns an Aggregation for middle numeric value from the specified column.
func Median(col string) Aggregation {
	return Aggregation{
		ColumnName: col,
		Fn:         MedianAggregator,
	}
}

// Avg returns an Aggregation for average numeric values from the specified column.
func Avg(col string) Aggregation {
	return Aggregation{
		ColumnName: col,
		Fn:         MeanAggregator,
	}
}

// Mean returns an Aggregation for average numeric values from the specified column.
func Mean(col string) Aggregation {
	return Aggregation{
		ColumnName: col,
		Fn:         MeanAggregator,
	}
}

// Mode returns an Aggregation for most occured numeric values from the specified column.
func Mode(col string) Aggregation {
	return Aggregation{
		ColumnName: col,
		Fn:         ModeAggregator,
	}
}

// Unique returns an Aggregation for counting unique values from the specified column.
func Unique(col string) Aggregation {
	return Aggregation{
		ColumnName: col,
		Fn:         UniqueAggregator,
	}
}

// First returns an Aggregation for getting the first value from the specified column.
func First(col string) Aggregation {
	return Aggregation{
		ColumnName: col,
		Fn:         FirstAggregator,
	}
}

// UniqueAggregator counts the number of unique values in the column.
func UniqueAggregator(vals []interface{}) interface{} {
	uniqueSet := make(map[interface{}]bool)
	for _, val := range vals {
		uniqueSet[val] = true
	}
	return len(uniqueSet)
}

// SumAggregator converts each value to float64 and returns their sum.
func SumAggregator(vals []interface{}) interface{} {
	sum := 0.0
	for _, val := range vals {
		fVal, err := toFloat64(val)
		if err != nil {
			fmt.Printf("sum conversion error: %v\n", err)
			continue
		}
		sum += fVal
	}
	return sum
}

// MinAggregator converts each value to float64 and returns the minimum.
func MinAggregator(vals []interface{}) interface{} {
	minSet := false
	var min float64
	for _, val := range vals {
		fVal, err := toFloat64(val)
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
}

// MeanAggregator calculates the mean (average) of values.
func MeanAggregator(vals []interface{}) interface{} {
	sum := 0.0
	count := 0
	for _, val := range vals {
		fVal, err := toFloat64(val)
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
}

// MedianAggregator calculates the median value.
func MedianAggregator(vals []interface{}) interface{} {
	var nums []float64
	for _, val := range vals {
		fVal, err := toFloat64(val)
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
}

// ModeAggregator finds the mode (most frequent value) among the values.
// If there are ties, it returns one of the most frequent values.
func ModeAggregator(vals []interface{}) interface{} {
	// Use a map to count frequencies.
	freq := make(map[float64]int)
	var mode float64
	maxCount := 0

	for _, val := range vals {
		fVal, err := toFloat64(val)
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
}

// MaxAggregator converts each value to float64 and returns the maximum.
func MaxAggregator(vals []interface{}) interface{} {
	maxSet := false
	var max float64
	for _, val := range vals {
		fVal, err := toFloat64(val)
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
}

// FirstAggregator returns the first value in the column.
func FirstAggregator(vals []interface{}) interface{} {
	if len(vals) == 0 {
		return nil
	}
	return vals[0]
}
