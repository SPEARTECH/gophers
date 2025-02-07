
package main

import (
    "C"
)

//export go_module
func go_module() *C.char {
    response := "Welcome to Gophers!"

    return C.CString(response)
}

func main() {
    // c_module()
}    

// create go version of pandas functions (most need to return a dataframe)
