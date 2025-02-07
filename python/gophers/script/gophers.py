#read from go module
from ctypes import cdll, c_char_p
import os

# list out functions to use and replace each pandas function (these will take in the go version of the function)


def main():
    path = os.path.dirname(os.path.realpath(__file__))

    # Example for calling from Go package and returning results
    # Load the shared library
    try:
        gophers = cdll.LoadLibrary(path+'/go_module/gophers.so')
    except Exception as e:
        print(str(e))
        return

    # Define the return type of the function
    gophers.go_module.restype = c_char_p
    
    go_message = gophers.go_module().decode('utf-8')
    print(go_message)

    return go_message


if __name__ == '__main__':
    main()
    
