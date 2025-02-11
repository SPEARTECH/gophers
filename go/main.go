
package main

import (
    "gophers/gophers/source"
    "fmt"
)

type Data struct{
    Name string
    Age int
    City string
}

func main() {
    // // Define the struct within the main function
    // var people []struct {
    //     Name string
    //     Age  int
    //     City string
    // }
    var data []gophers.Data

    // Add rows to the slice
    data = append(data, gophers.Data {
        Name: "Alice",
        Age:  30,
        City: "New York",
    })

    data = append(data, gophers.Data {
        Name: "Bob",
        Age:  25,
        City: "San Francisco",
    })

    data = append(data, gophers.Data{
        Name: "Charlie",
        Age:  35,
        City: "Los Angeles",
    })

    df := gophers.DATAFRAME(data)
    fmt.Println(df)

    // // Print the data
    // fmt.Print("name\tage\tcity")
    // for _, person := range people {
    //     fmt.Println(person.Name, "\t", person.Age, "\t", person.City)
    // }
}    

// create go version of pandas functions (most need to return a dataframe)
