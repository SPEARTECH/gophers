from ctypes import cdll, c_char_p, c_int
import os
import json

# Helper for extraction (already available in Go as Col)
def Col(source):
    return FuncColumn("Col",source)

# Helper for extraction (already available in Go as Col)
def Lit(source):
    return FuncColumn("Lit",source)

class FuncColumn:
    """Helper for function-based column operations.
       func_name is a string like "SHA256" and cols is a list of column names.
    """
    def __init__(self, func_name, cols):
        self.func_name = func_name
        self.cols = cols

class SplitColumn:
    """Helper for function-based column operations.
       func_name is a string like "SHA256" and cols is a list of column names.
    """
    def __init__(self, func_name, cols, delim):
        self.func_name = func_name
        self.cols = cols
        self.delim = delim

# Helper functions for common operations
def SHA256(*cols):
    return FuncColumn("SHA256", list(cols))

def SHA512(*cols):
    return FuncColumn("SHA512", list(cols))

# In this design, CollectList, CollectSet, and Split will be handled
# by their own exported Go wrappers.
def CollectList(col_name):
    return FuncColumn("CollectList",col_name)  # This is a marker value; see DataFrame.Column below.

def CollectSet(col_name):
    return FuncColumn("CollectSet", col_name)

def Split(col_name, delimiter):
    return SplitColumn(col_name, delimiter)

class DataFrame:
    def __init__(self):
        path = os.path.dirname(os.path.realpath(__file__))
        self.gophers = cdll.LoadLibrary(path + '/go_module/gophers.so')
        self.gophers.ReadJSON.restype = c_char_p
        self.gophers.Show.restype = c_char_p
        self.gophers.Head.restype = c_char_p
        self.gophers.Tail.restype = c_char_p
        self.gophers.Vertical.restype = c_char_p
        self.gophers.ColumnOp.restype = c_char_p
        self.gophers.ColumnCollectList.restype = c_char_p
        self.gophers.ColumnCollectSet.restype = c_char_p
        self.gophers.ColumnSplit.restype = c_char_p
        self.gophers.DFColumns.restype = c_char_p
        self.gophers.DFCount.restype = c_int
        self.gophers.DFCountDuplicates.restype = c_int
        self.gophers.DFCountDistinct.restype = c_int
        self.gophers.DFCollect.restype = c_char_p
        self.gophers.DisplayBrowserWrapper.restype = c_char_p
        
    def ReadJSON(self, json_data):
        # Store the JSON representation of DataFrame from Go.
        self.df_json = self.gophers.ReadJSON(json_data.encode('utf-8')).decode('utf-8')

    def Show(self, chars, record_count=100):
        result = self.gophers.Show(self.df_json.encode('utf-8'), c_int(chars), c_int(record_count)).decode('utf-8')
        print(result)

    def Columns(self):
        cols_json = self.gophers.DFColumns(self.df_json.encode('utf-8')).decode('utf-8')
        return json.loads(cols_json)

    def Count(self):
        return self.gophers.DFCount(self.df_json.encode('utf-8'))

    def CountDuplicates(self, cols=None):
        if cols is None:
            cols_json = json.dumps([])
        else:
            cols_json = json.dumps(cols)
        return self.gophers.DFCountDuplicates(self.df_json.encode('utf-8'),
                                              cols_json.encode('utf-8'))

    def CountDistinct(self, cols=None):
        if cols is None:
            cols_json = json.dumps([])
        else:
            cols_json = json.dumps(cols)
        return self.gophers.DFCountDistinct(self.df_json.encode('utf-8'),
                                            cols_json.encode('utf-8'))

    def Collect(self, col_name):
        collected = self.gophers.DFCollect(self.df_json.encode('utf-8'),
                                           col_name.encode('utf-8')).decode('utf-8')
        return json.loads(collected)
    def Head(self, chars):
        result = self.gophers.Head(self.df_json.encode('utf-8'), c_int(chars)).decode('utf-8')
        print(result)

    def Tail(self, chars):
        result = self.gophers.Tail(self.df_json.encode('utf-8'), c_int(chars)).decode('utf-8')
        print(result)

    def Vertical(self, chars, record_count=100):
        result = self.gophers.Vertical(self.df_json.encode('utf-8'), c_int(chars), c_int(record_count)).decode('utf-8')
        print(result)

    def DisplayBrowser(self):
        err = self.gophers.DisplayBrowserWrapper(self.df_json.encode('utf-8')).decode('utf-8')
        if err:
            print("Error displaying in browser:", err)
        return self

    def Column(self, col_name, col_spec):
        # If col_spec is an instance of ColumnExpr, use ColumnFrom.
        if isinstance(col_spec, FuncColumn):
            cols_json = json.dumps(col_spec.cols)
            self.df_json = self.gophers.ColumnOp(
                self.df_json.encode('utf-8'),
                col_name.encode('utf-8'),
                col_spec.func_name.encode('utf-8'),
                cols_json.encode('utf-8')
            ).decode('utf-8')
        # Check for CollectList marker (a string) and call ColumnCollectList.
        elif isinstance(col_spec, str) and col_spec.startswith("CollectList:"):
            # col_spec is in the form "CollectList:colname"
            src = col_spec.split(":", 1)[1]
            self.df_json = self.gophers.ColumnCollectList(
                self.df_json.encode('utf-8'),
                col_name.encode('utf-8'),
                src.encode('utf-8')
            ).decode('utf-8')
        # Similarly for CollectSet.
        elif isinstance(col_spec, str) and col_spec.startswith("CollectSet:"):
            src = col_spec.split(":", 1)[1]
            self.df_json = self.gophers.ColumnCollectSet(
                self.df_json.encode('utf-8'),
                col_name.encode('utf-8'),
                src.encode('utf-8')
            ).decode('utf-8')
        # For Split, expect a tuple: (source, delimiter)
        elif isinstance(col_spec, SplitColumn):
            src, delim = col_spec
            self.df_json = self.gophers.ColumnSplit(
                self.df_json.encode('utf-8'),
                col_name.encode('utf-8'),
                src.encode('utf-8'),
                delim.encode('utf-8')
            ).decode('utf-8')
        # Otherwise, treat col_spec as a literal.        
        else:
            print("Error running code, no valid input.")
        return self

# Example usage:
def main():
    json_data = '[{"col1": "value1", "col2": "value2", "col3": "value3"}, {"col1": "value4", "col2": "value5", "col3": "value6"}, {"col1": "value7", "col2": "value8", "col3": "value9"}]'
    df = DataFrame()
    df.ReadJSON(json_data)
    print("Head:")
    df.Head(25)
    print("Tail:")
    df.Tail(25)
    print("Vertical:")
    df.Vertical(25, record_count=3)
    print("Columns:")
    print(df.Columns())
    df.DisplayBrowser()

if __name__ == '__main__':
    main()