#read from go module
from ctypes import cdll, c_char_p
import os



def main():
    # Example for calling from Go package and returning results
    path = os.path.dirname(os.path.realpath(__file__))

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
    
