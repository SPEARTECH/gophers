package gophers

import (
	"fmt"
	"strconv"
)

// Print displays the DataFrame in a simple tabular format.
func (df *DataFrame) Show() {
	// Print header.
	for _, col := range df.Cols {
		fmt.Printf("%-15s", col)
	}
	fmt.Println()

	// Print each row.
	for i := 0; i < df.Rows; i++ {
		for _, col := range df.Cols {
			fmt.Printf("%-15v", df.Data[col][i])
		}
		fmt.Println()
	}
}

func (df *DataFrame) Head(){
	// Print header.
	for _, col := range df.Cols {
		fmt.Printf("%-15s", col)
	}
	fmt.Println()

	// Print each row.
	for i := 0; i < 5 && i < df.Rows; i++ {
		for _, col := range df.Cols {
			fmt.Printf("%-15v", df.Data[col][i])
		}
		fmt.Println()
	}
}

func (df *DataFrame) Tail(){
	// Print header.
	for _, col := range df.Cols {
		fmt.Printf("%-15s", col)
	}
	fmt.Println()

	// Print each row.
	for i := 0; i < df.Rows && i > df.Rows - 5 ; i++ {
		for _, col := range df.Cols {
			fmt.Printf("%-15v", df.Data[col][i])
		}
		fmt.Println()
	}
}


func (df *DataFrame) columns() []string {
	return df.Cols
}

func (df *DataFrame) Display(vertical bool){
	v := true
	if vertical != true || vertical != false{
		v = vertical
	}
	if v == true{
		for _, row := range df.Data{
			for i, item := range df.Columns{
				fmt.Println(item, row[i])
			}
		}
	} else {
		df.show()
	}
}

func (df *DataFrame) Vertical(chars int){
	if chars <= 0{
		chars = 25
	}
		fmt.Println(df)
		count := 0
		max_len := 0
		for count < df.Rows{
			for _, col := range df.Cols{
				if len(col) > max_len{
					max_len = len(col)
				}
				values, exists := df.Data[col]
				if !exists{
					fmt.Println("Column not found:",col)
					continue
				}
				
				if count < len(values){
					var item1 string
					if chars >= len(col){
						item1 = col
					} else {
						item1 = fmt.Sprint(col[:chars-3],"...")
					}
					var item2 string
					switch v := values[count].(type){
					case int: 
						item2 = strconv.Itoa(v)
					case float64:
						item2 = strconv.FormatFloat(v, 'f', 2, 64)
					case bool:
						item2 = strconv.FormatBool(v)
					case string:
						item2 = v
					default:
						item2 = fmt.Sprintf("%v", v)
					}
					if chars < len(item2){
						item2 = item2[:chars]
					}	
					space := "\t"
					var num int
					num = len(item1)/7
					if num > 0{
						for i := 0; i < num; i++{ //fix math
							// if 
							space += "\t"
						}
					}
					fmt.Println(item1, space, item2)
				}
			}
			count++
		}
}

// displayHTML()?