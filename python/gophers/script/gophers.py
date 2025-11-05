from ctypes import cdll, c_char_p, c_int #use cffi instead of ctypes? needed for concurrency, ctypes does not release the GIL!
import os
import json
from IPython.display import HTML, display

path = os.path.dirname(os.path.realpath(__file__))
gophers = cdll.LoadLibrary(path + '/go_module/gophers.so')
# Set restype for functions at module load time
gophers.ReadJSON.restype = c_char_p
gophers.ReadNDJSON.restype = c_char_p
gophers.ReadCSV.restype = c_char_p
gophers.ReadYAML.restype = c_char_p
gophers.GetAPIJSON.restype = c_char_p
gophers.Show.restype = c_char_p
gophers.Head.restype = c_char_p
gophers.Tail.restype = c_char_p
gophers.Vertical.restype = c_char_p
gophers.ColumnWrapper.restype = c_char_p
gophers.ColumnsWrapper.restype = c_char_p
gophers.CountWrapper.restype = c_int
gophers.CountDuplicatesWrapper.restype = c_int
gophers.CountDistinctWrapper.restype = c_int
gophers.CollectWrapper.restype = c_char_p
gophers.DisplayBrowserWrapper.restype = c_char_p
gophers.DisplayWrapper.restype = c_char_p
gophers.DisplayToFileWrapper.restype = c_char_p
gophers.DisplayChartWrapper.restype = c_char_p
gophers.BarChartWrapper.restype = c_char_p
gophers.ColumnChartWrapper.restype = c_char_p
gophers.StackedBarChartWrapper.restype = c_char_p
gophers.StackedPercentChartWrapper.restype = c_char_p
gophers.GroupByWrapper.restype = c_char_p
gophers.ExplodeWrapper.restype = c_char_p
gophers.FilterWrapper.restype = c_char_p
gophers.SelectWrapper.restype = c_char_p
gophers.UnionWrapper.restype = c_char_p
gophers.JoinWrapper.restype = c_char_p
gophers.SortWrapper.restype = c_char_p
gophers.FilterWrapper.restype = c_char_p
gophers.OrderByWrapper.restype = c_char_p
gophers.DropWrapper.restype = c_char_p
gophers.DropDuplicatesWrapper.restype = c_char_p
gophers.DropNAWrapper.restype = c_char_p
gophers.FillNAWrapper.restype = c_char_p
gophers.RenameWrapper.restype = c_char_p
gophers.GroupByWrapper.restype = c_char_p
gophers.AggWrapper.restype = c_char_p
gophers.SumWrapper.restype = c_char_p
gophers.MaxWrapper.restype = c_char_p
gophers.MinWrapper.restype = c_char_p
gophers.MedianWrapper.restype = c_char_p
gophers.MeanWrapper.restype = c_char_p
gophers.ModeWrapper.restype = c_char_p
gophers.UniqueWrapper.restype = c_char_p
gophers.FirstWrapper.restype = c_char_p
gophers.CreateReportWrapper.restype = c_char_p
gophers.OpenReportWrapper.restype = c_char_p
gophers.SaveReportWrapper.restype = c_char_p
gophers.AddPageWrapper.restype = c_char_p
gophers.AddHTMLWrapper.restype = c_char_p
gophers.AddDataframeWrapper.restype = c_char_p
gophers.AddChartWrapper.restype = c_char_p
gophers.AddHeadingWrapper.restype = c_char_p
gophers.AddTextWrapper.restype = c_char_p
gophers.AddSubTextWrapper.restype = c_char_p
gophers.AddBulletsWrapper.restype = c_char_p
gophers.ToCSVFileWrapper.restype = c_char_p
gophers.FlattenWrapper.restype = c_char_p
gophers.StringArrayConvertWrapper.restype = c_char_p
gophers.KeysToColsWrapper.restype = c_char_p
gophers.ReadSqlite.restype = c_char_p
gophers.WriteSqlite.restype = c_char_p
gophers.GetSqliteTables.restype = c_char_p
gophers.GetSqliteSchema.restype = c_char_p
gophers.CloneWrapper.restype = c_char_p

class ColumnExpr:
    def __init__(self, expr):
        self.expr = expr

    def to_json(self):
        return json.dumps(self.expr)

    def Help(self):
        print("""Column Help:
    Contains(substr)
    EndsWith(suffix)
    Eq(other)
    Ge(other)
    Gt(other)
    IsBetween(lower, upper)
    IsIn(values)
    IsNotNull()
    IsNull()
    Le(other)
    Like(pattern)
    Lower()
    Lt(other)
    LTrim()
    Ne(other)
    NotContains(substr)
    NotLike(pattern)
    Replace(old, new)
    RTrim()
    StartsWith(prefix)
    Substr(start, length)
    Title()
    Trim()
    Upper()""")
        
    def __repr__(self):
        return f"ColumnExpr({self.expr})"

    def IsNull(self):
        return ColumnExpr({ "type": "isnull", "expr": self.expr })
    
    def IsNotNull(self):
        return ColumnExpr({ "type": "isnotnull", "expr": self.expr })
    
    def IsIn(self, values):
        return ColumnExpr({ "type": "isin", "expr": self.expr, "values": values })
    
    def IsBetween(self, lower, upper):
        return ColumnExpr({ "type": "isbetween", "expr": self.expr, "lower": lower, "upper": upper })
    
    def Like(self, pattern):
        return ColumnExpr({ "type": "like", "expr": self.expr, "pattern": pattern })
    
    def NotLike(self, pattern):
        return ColumnExpr({ "type": "notlike", "expr": self.expr, "pattern": pattern })
    
    def StartsWith(self, prefix):
        return ColumnExpr({ "type": "startswith", "expr": self.expr, "prefix": prefix })
    
    def EndsWith(self, suffix):
        return ColumnExpr({ "type": "endswith", "expr": self.expr, "suffix": suffix })
    
    def Contains(self, substr):
        return ColumnExpr({ "type": "contains", "expr": self.expr, "substr": substr })
    
    def NotContains(self, substr):
        return ColumnExpr({ "type": "notcontains", "expr": self.expr, "substr": substr })
    
    def Replace(self, old, new):
        return ColumnExpr({ "type": "replace", "expr": self.expr, "old": old, "new": new })
    
    def Trim(self):
        return ColumnExpr({ "type": "trim", "expr": self.expr })
    
    def LTrim(self):
        return ColumnExpr({ "type": "ltrim", "expr": self.expr })
    
    def RTrim(self):
        return ColumnExpr({ "type": "rtrim", "expr": self.expr })
    
    def Lower(self):
        return ColumnExpr({ "type": "lower", "expr": self.expr })
    
    def Upper(self):
        return ColumnExpr({ "type": "upper", "expr": self.expr })
    
    # def Title(self):
    #     return ColumnExpr({ "type": "title", "expr": self.expr })
    
    # def Substr(self, start, length):
    #     return ColumnExpr({ "type": "substr", "expr": self.expr, "start": start, "length": length })
    
    def Gt(self, other):
        return ColumnExpr({ "type": "gt", "left": self.expr, "right": other })
    
    def Lt(self, other):
        return ColumnExpr({ "type": "lt", "left": self.expr, "right": other })
    
    def Ge(self, other):
        return ColumnExpr({ "type": "ge", "left": self.expr, "right": other })
    
    def Le(self, other):
        return ColumnExpr({ "type": "le", "left": self.expr, "right": other })
    
    def Eq(self, other):
        return ColumnExpr({ "type": "eq", "left": self.expr, "right": other })
    
    def Ne(self, other):
        return ColumnExpr({ "type": "ne", "left": self.expr, "right": other })

# class SplitColumn:
#     """Helper for function-based column operations.
#        func_name is a string like "SHA256" and cols is a list of column names.
#     """
#     def __init__(self, func_name, cols, delim):
#         self.func_name = func_name
#         self.cols = cols
#         self.delim = delim

# Chart obj
class Chart:
    def __init__(self, html):
        self.html = html

# Report + Methods
class Report:
    def __init__(self, report_json):
        self.report_json = report_json

    def Help(self):
        print("""Report Help:
    AddBullets(page, bullets)
    AddChart(page, chart)
    AddDataframe(page, df)
    AddHeading(page, text, size)
    AddHTML(page, text)
    AddPage(name)
    AddSubText(page, text)
    AddText(page, text)
    Open()
    Save(filename)""")
        
    def Open(self):
        # print("")
        # print("printing open report:"+self.report_json)

        err = gophers.OpenReportWrapper(self.report_json.encode('utf-8')).decode('utf-8')
        if err != "success":
            print("Error opening report:", err)
        return self

    def Save(self, filename):
        err = gophers.SaveReportWrapper(self.report_json.encode('utf-8'), filename.encode('utf-8')).decode('utf-8')
        if err:
            print("Error saving report:", err)
        return self

    def AddPage(self, name):
        result = gophers.AddPageWrapper(self.report_json.encode('utf-8'), name.encode('utf-8')).decode('utf-8')
        if result:
            self.report_json = result
            # print("AddPage: Updated report JSON:", self.report_json)
        else:
            print("Error adding page:", result)
        return self

    def AddHTML(self, page, text):
        result = gophers.AddHTMLWrapper(self.report_json.encode('utf-8'), page.encode('utf-8'), text.encode('utf-8')).decode('utf-8')
        if result:
            self.report_json = result
        else:
            print("Error adding HTML:", result)
        return self

    def AddDataframe(self, page, df):
        result = gophers.AddDataframeWrapper(self.report_json.encode('utf-8'), page.encode('utf-8'), df.df_json.encode('utf-8')).decode('utf-8')
        if result:
            self.report_json = result
        else:
            print("Error adding dataframe:", result)
        return self

    def AddChart(self, page, chart):
        chart_json = chart.html
        # print(f"Chart JSON: {chart_json}")

        result = gophers.AddChartWrapper(
            self.report_json.encode('utf-8'),
            page.encode('utf-8'),
            chart_json.encode('utf-8')
        ).decode('utf-8')

        if result:
            # print(f"Chart added successfully, result: {result[:100]}...")
            self.report_json = result
        else:
            print(f"Error adding chart, empty result")
        return self
    def AddHeading(self, page, text, size):
        result = gophers.AddHeadingWrapper(self.report_json.encode('utf-8'), page.encode('utf-8'), text.encode('utf-8'), size).decode('utf-8')
        if result:
            self.report_json = result
        else:
            print("Error adding heading:", result)
        return self

    def AddText(self, page, text):
        result = gophers.AddTextWrapper(self.report_json.encode('utf-8'), page.encode('utf-8'), text.encode('utf-8')).decode('utf-8')
        if result:
            self.report_json = result
        else:
            print("Error adding text:", result)
        return self

    def AddSubText(self, page, text):
        result = gophers.AddSubTextWrapper(self.report_json.encode('utf-8'), page.encode('utf-8'), text.encode('utf-8')).decode('utf-8')
        if result:
            self.report_json = result
        else:
            print("Error adding subtext:", result)
        return self

    def AddBullets(self, page, bullets):
        bullets_json = json.dumps(bullets)
        result = gophers.AddBulletsWrapper(self.report_json.encode('utf-8'), page.encode('utf-8'), bullets_json.encode('utf-8')).decode('utf-8')
        if result:
            self.report_json = result
        else:
            print("Error adding bullets:", result)
        return self

def Help():
    print("""Functions Help:
    Agg(*aggregations)
    And(left, right)
    ArraysZip(*cols)
    Col(name)
    CollectList(col_name)
    CollectSet(col_name)
    Concat(delimiter, *cols)
    DisplayChart(chart)
    DisplayHTML(html)
    GetAPIJSON(endpoint, headers, query_params)
    GetSqliteSchema(db_path, table),
    GetSqliteTables(db_path),
    If(condition, trueExpr, falseExpr)
    Lit(value)
    Or(left, right)
    ReadCSV(csv_data)
    ReadJSON(json_data)
    ReadNDJSON(json_data)
    ReadSqlite(db_path, table, query)
    ReadYAML(yaml_data)
    SHA256(*cols)
    SHA512(*cols)
    Split(col_name, delimiter)
    Sum(column_name)""")

    
# Aggregate functions
def Sum(column_name):
    # Call the Go SumWrapper function with only the column name
    sum_agg_json = gophers.SumWrapper(column_name.encode('utf-8')).decode('utf-8')
    # Parse the JSON string into a Python dict before returning it
    return json.loads(sum_agg_json)
def Max(column_name):
    # Call the Go SumWrapper function with only the column name
    sum_agg_json = gophers.MaxWrapper(column_name.encode('utf-8')).decode('utf-8')
    # Parse the JSON string into a Python dict before returning it
    return json.loads(sum_agg_json)
def Min(column_name):
    # Call the Go SumWrapper function with only the column name
    sum_agg_json = gophers.MinWrapper(column_name.encode('utf-8')).decode('utf-8')
    # Parse the JSON string into a Python dict before returning it
    return json.loads(sum_agg_json)
def Median(column_name):
    # Call the Go SumWrapper function with only the column name
    sum_agg_json = gophers.MedianWrapper(column_name.encode('utf-8')).decode('utf-8')
    # Parse the JSON string into a Python dict before returning it
    return json.loads(sum_agg_json)
def Mean(column_name):
    # Call the Go SumWrapper function with only the column name
    sum_agg_json = gophers.MeanWrapper(column_name.encode('utf-8')).decode('utf-8')
    # Parse the JSON string into a Python dict before returning it
    return json.loads(sum_agg_json)
def Mode(column_name):
    # Call the Go SumWrapper function with only the column name
    sum_agg_json = gophers.ModeWrapper(column_name.encode('utf-8')).decode('utf-8')
    # Parse the JSON string into a Python dict before returning it
    return json.loads(sum_agg_json)
def First(column_name):
    # Call the Go SumWrapper function with only the column name
    sum_agg_json = gophers.FirstWrapper(column_name.encode('utf-8')).decode('utf-8')
    # Parse the JSON string into a Python dict before returning it
    return json.loads(sum_agg_json)
def Unique(column_name):
    # Call the Go SumWrapper function with only the column name
    sum_agg_json = gophers.UniqueWrapper(column_name.encode('utf-8')).decode('utf-8')
    # Parse the JSON string into a Python dict before returning it
    return json.loads(sum_agg_json)

def Agg(*aggregations):
    # Simply return the list of aggregations
    return list(aggregations)

# Column functions
def Col(name):
    return ColumnExpr({ "type": "col", "name": name })

def Lit(value):
    return ColumnExpr({ "type": "lit", "value": value })

def Cast(col, datatype):
    """
    Returns a ColumnExpr that casts the value of 'col'
    to the specified datatype ("int", "float", or "string").
    """
    return ColumnExpr({
        "type": "cast",
        "col": json.loads(col.to_json()),
        "datatype": datatype
    })

# Logic functions
def Or(left, right):
    return ColumnExpr({ "type": "or", "left": json.loads(left.to_json()), "right": json.loads(right.to_json()) })

def And(left, right):
    return ColumnExpr({ "type": "and", "left": json.loads(left.to_json()), "right": json.loads(right.to_json()) })

def If(condition, trueExpr, falseExpr):
    return ColumnExpr({ "type": "if", "cond": json.loads(condition.to_json()), "true": json.loads(trueExpr.to_json()), "false": json.loads(falseExpr.to_json()) })

# List functions
def SHA256(*cols):
    return ColumnExpr({ "type": "sha256", "cols": [json.loads(col.to_json()) for col in cols] })

def SHA512(*cols):
    return ColumnExpr({ "type": "sha512", "cols": [json.loads(col.to_json()) for col in cols] })

def CollectList(col_name):
    return ColumnExpr({ "type": "collectlist", "col": col_name })

def CollectSet(col_name):
    return ColumnExpr({ "type": "collectset", "col": col_name })

def Split(col_name, delimiter):
    return ColumnExpr({ "type": "split", "col": col_name, "delimiter": delimiter })

def Concat(delimiter, *cols):
    """
    Returns a ColumnExpr that concatenates the string representations
    of the given column expressions using the specified delimiter.
    """
    return ColumnExpr({
        "type": "concat_ws",
        "delimiter": delimiter,
        "cols": [json.loads(col.to_json()) for col in cols]
    })

def ArraysZip(*cols):
    """
    Returns a ColumnExpr that zips the given column expressions
    into an array of structs.
    """
    return ColumnExpr({
        "type": "arrays_zip",
        "cols": [json.loads(col.to_json()) for col in cols]
    })

def Keys(col_name):
    return ColumnExpr({ "type": "keys", "col": col_name })

def Lookup(key_expr, nested_col):
    """
    Creates a ColumnExpr for lookup.
    
    Parameters:
      nested_col: the name of the nested column (will be wrapped with Col())
      key_expr: a ColumnExpr representing the lookup key (e.g. Col('key') or Lit("some constant"))
    
    Returns:
      A ColumnExpr with type "lookup".
    """
    # If key_expr is not already a ColumnExpr, wrap it.
    if not isinstance(key_expr, ColumnExpr):
        key_expr = Lit(key_expr)
    return ColumnExpr({
        "type": "lookup",
        "left": json.loads(key_expr.to_json()),
        "right": json.loads(Col(nested_col).to_json())
    })

# Source functions
def ReadJSON(json_data):
    # Store the JSON representation of DataFrame from Go.
    df_json = gophers.ReadJSON(json_data.encode('utf-8')).decode('utf-8')
    return DataFrame(df_json)

def ReadNDJSON(json_data):
    # Store the JSON representation of DataFrame from Go.
    df_json = gophers.ReadNDJSON(json_data.encode('utf-8')).decode('utf-8')
    return DataFrame(df_json)

def ReadCSV(json_data):
    # Store the JSON representation of DataFrame from Go.
    df_json = gophers.ReadCSV(json_data.encode('utf-8')).decode('utf-8')
    return DataFrame(df_json)

def ReadYAML(yaml_data):
    # Store the JSON representation of DataFrame from Go.
    df_json = gophers.ReadYAML(yaml_data.encode('utf-8')).decode('utf-8')
    return DataFrame(df_json)

def GetAPIJSON(endpoint, headers, query_params):
    # Store the JSON representation of DataFrame from Go.
    df_json = gophers.GetAPIJSON(endpoint.encode('utf-8'), headers.encode('utf-8'), query_params.encode('utf-8')).decode('utf-8')
    return DataFrame(df_json)

def ReadSqlite(db_path, table=None, query=None):
    """
    Read from a SQLite database.
    - If query is provided, it runs that SQL and returns a DataFrame.
    - Else if table is provided, returns SELECT * FROM table.
    - Else reads all user tables and merges rows (adds _table column).
    """
    t = "" if table is None else table
    q = "" if query is None else query
    df_json = gophers.ReadSqlite(db_path.encode('utf-8'), t.encode('utf-8'), q.encode('utf-8')).decode('utf-8')
    return DataFrame(df_json)

def GetSqliteTables(db_path: str):
    """
    Return a list of table names in the SQLite database.
    Raises RuntimeError on error.
    """
    raw = gophers.GetSqliteTables(db_path.encode("utf-8")).decode("utf-8")
    try:
        obj = json.loads(raw)
    except Exception:
        raise RuntimeError(raw)
    if isinstance(obj, dict) and obj.get("error"):
        raise RuntimeError(obj["error"])
    return obj.get("tables", [])

def GetSqliteSchema(db_path: str, table: str):
    """
    Return schema info for a table:
      { table, columns:[{cid,name,type,notnull,default,primaryKey}], foreign_keys:[...], indexes:[...] }
    Raises RuntimeError on error.
    """
    raw = gophers.GetSqliteSchema(db_path.encode("utf-8"), table.encode("utf-8")).decode("utf-8")
    try:
        obj = json.loads(raw)
    except Exception:
        raise RuntimeError(raw)
    if isinstance(obj, dict) and obj.get("error"):
        raise RuntimeError(obj["error"])
    return obj

# Display functions
def DisplayHTML(html):
    display(HTML(html))

def DisplayChart(chart):
    html = gophers.DisplayChartWrapper(chart.html.encode('utf-8'))
    display(HTML(html))

# Report methods
def CreateReport(title):
    report_json = gophers.CreateReportWrapper(title.encode('utf-8')).decode('utf-8')
    # print("CreateReport: Created report JSON:", report_json)
    return Report(report_json)

# PANDAS FUNCTIONS
# loc
# iloc

# Dataframe + Methods
class DataFrame:
    def __init__(self, df_json=None):
        self.df_json = df_json

    def Help(self):
        print("""DataFrame Help:
    BarChart(title, subtitle, groupcol, aggs)
    Clone()
    Column(col_name, col_spec)
    ColumnChart(title, subtitle, groupcol, aggs)
    Columns()
    Collect(col_name)
    Count()
    CountDistinct(cols)
    CountDuplicates(cols)
    CreateReport(title)
    Display()
    DisplayBrowser()
    DisplayToFile(file_path)
    Drop(*cols)
    DropDuplicates(cols)
    DropNA(cols)
    FillNA(value)
    Filter(condition)
    Flatten(*cols)
    GroupBy(groupCol, aggs)
    Head(chars)
    Join(df2, col1, col2, how)
    OrderBy(col, asc)
    Select(*cols)
    Show(chars, record_count)
    Sort(*cols)
    StackedBarChart(title, subtitle, groupcol, aggs)
    StackedPercentChart(title, subtitle, groupcol, aggs)
    StringArrayConvert(col_name)
    Tail(chars)
    ToCSVFile(filename)
    Union(df2)
    Vertical(chars, record_count)
    WriteSqlite(db_path, table_name, mode, key_cols)""")
        
    # Display functions
    def Show(self, chars, record_count=100):
        result = gophers.Show(self.df_json.encode('utf-8'), c_int(chars), c_int(record_count)).decode('utf-8')
        print(result)

    def Columns(self):
        cols_json = gophers.ColumnsWrapper(self.df_json.encode('utf-8')).decode('utf-8')
        return json.loads(cols_json)

    def Count(self):
        return gophers.CountWrapper(self.df_json.encode('utf-8'))

    def CountDuplicates(self, cols=None):
        if cols is None:
            cols_json = json.dumps([])
        else:
            cols_json = json.dumps(cols)
        return gophers.CountDuplicatesWrapper(self.df_json.encode('utf-8'),
                                              cols_json.encode('utf-8'))

    def CountDistinct(self, cols=None):
        if cols is None:
            cols_json = json.dumps([])
        else:
            cols_json = json.dumps(cols)
        return gophers.CountDistinctWrapper(self.df_json.encode('utf-8'),
                                            cols_json.encode('utf-8'))

    def Collect(self, col_name):
        collected = gophers.CollectWrapper(self.df_json.encode('utf-8'),
                                           col_name.encode('utf-8')).decode('utf-8')
        return json.loads(collected)
    
    def Head(self, chars):
        result = gophers.Head(self.df_json.encode('utf-8'), c_int(chars)).decode('utf-8')
        print(result)

    def Tail(self, chars):
        result = gophers.Tail(self.df_json.encode('utf-8'), c_int(chars)).decode('utf-8')
        print(result)

    def Vertical(self, chars, record_count=100):
        result = gophers.Vertical(self.df_json.encode('utf-8'), c_int(chars), c_int(record_count)).decode('utf-8')
        print(result)

    def DisplayBrowser(self):
        err = gophers.DisplayBrowserWrapper(self.df_json.encode('utf-8')).decode('utf-8')
        if err:
            print("Error displaying in browser:", err)
        return self
    
    def Display(self):
        html = gophers.DisplayWrapper(self.df_json.encode('utf-8')).decode('utf-8')
        print(html)
        display(HTML(html))
        # return self
    
    def DisplayToFile(self, file_path):
        err = gophers.DisplayToFileWrapper(self.df_json.encode('utf-8'), file_path.encode('utf-8')).decode('utf-8')
        if err:
            print("Error writing to file:", err)
        return self
        
    # Chart methods
    def BarChart(self, title, subtitle, groupcol, aggs):
        # Make sure aggs is a list
        if not isinstance(aggs, list):
            aggs = [aggs]
        
        aggs_json = json.dumps(aggs)
        html = gophers.BarChartWrapper(
            self.df_json.encode('utf-8'), 
            title.encode('utf-8'), 
            subtitle.encode('utf-8'), 
            groupcol.encode('utf-8'), 
            aggs_json.encode('utf-8')
        ).decode('utf-8')
        
        # Create a Chart object
        chart = Chart(html)
        # print(html)
        
        # Display the chart
        # display(HTML(html))
        
        # Return the Chart object
        return chart
    
    def ColumnChart(self, title, subtitle, groupcol, aggs):
        # Make sure aggs is a list
        if not isinstance(aggs, list):
            aggs = [aggs]
        
        aggs_json = json.dumps(aggs)
        html = gophers.ColumnChartWrapper(
            self.df_json.encode('utf-8'), 
            title.encode('utf-8'), 
            subtitle.encode('utf-8'), 
            groupcol.encode('utf-8'), 
            aggs_json.encode('utf-8')
        ).decode('utf-8')
        
        # Create a Chart object
        chart = Chart(html)
        
        # Display the chart
        # display(HTML(html))
        
        # Return the Chart object
        return chart
    
    def StackedBarChart(self, title, subtitle, groupcol, aggs):
        aggs_json = json.dumps([agg.__dict__ for agg in aggs])
        html = gophers.StackedBarChartWrapper(self.df_json.encode('utf-8'), title.encode('utf-8'), subtitle.encode('utf-8'), groupcol.encode('utf-8'), aggs_json.encode('utf-8')).decode('utf-8')
        display(HTML(html))
        return self
    
    def StackedPercentChart(self, title, subtitle, groupcol, aggs):
        aggs_json = json.dumps([agg.__dict__ for agg in aggs])
        html = gophers.StackedPercentChartWrapper(self.df_json.encode('utf-8'), title.encode('utf-8'), subtitle.encode('utf-8'), groupcol.encode('utf-8'), aggs_json.encode('utf-8')).decode('utf-8')
        display(HTML(html))
        return self
    
    # Transform functions
    def Column(self, col_name, col_spec):
        if isinstance(col_spec, ColumnExpr):
            self.df_json = gophers.ColumnWrapper(
                self.df_json.encode('utf-8'),
                col_name.encode('utf-8'),
                col_spec.to_json().encode('utf-8')
            ).decode('utf-8')
        # Otherwise, treat col_spec as a literal.        
        else:
            print(f"Error running code, cannot run {col_name} within Column function.")
        return self 
    def GroupBy(self, groupCol, aggs):
        # aggs should be a list of JSON objects returned by Sum
        self.df_json = gophers.GroupByWrapper(
            self.df_json.encode('utf-8'),
            groupCol.encode('utf-8'),
            json.dumps(aggs).encode('utf-8')
        ).decode('utf-8')
        return self    
    def Select(self, *cols):
        # cols should be a list of column names
        self.df_json = gophers.SelectWrapper(
            self.df_json.encode('utf-8'),
            json.dumps([col for col in cols]).encode('utf-8')
        ).decode('utf-8')
        return self
    def Union(self, df2):
        self.df_json = gophers.UnionWrapper(
            self.df_json.encode('utf-8'),
            df2.df_json.encode('utf-8')
        ).decode('utf-8')
        return self
    def Join(self, df2, col1, col2, how):
        self.df_json = gophers.JoinWrapper(
            self.df_json.encode('utf-8'),
            df2.df_json.encode('utf-8'),
            col1.encode('utf-8'),
            col2.encode('utf-8'),
            how.encode('utf-8')
        ).decode('utf-8')
        return self
    def Sort(self, *cols):
        self.df_json = gophers.SortWrapper(
            self.df_json.encode('utf-8'),
            json.dumps([col for col in cols]).encode('utf-8')
        ).decode('utf-8')   
        return self
    def Filter(self, condition):
        colspec = ColumnExpr(json.loads(condition.to_json()))
        if isinstance(colspec, ColumnExpr):
            self.df_json = gophers.FilterWrapper(
                self.df_json.encode('utf-8'),
                colspec.to_json().encode('utf-8')
            ).decode('utf-8')
        else:
            print(f"Error: condition must be a ColumnExpr, got {type(condition)}")
        return self
    def OrderBy(self, col, asc):
        self.df_json = gophers.OrderByWrapper(
            self.df_json.encode('utf-8'),
            col.encode('utf-8'),
            asc
        ).decode('utf-8')
        return self
    def Drop(self, *cols):
        self.df_json = gophers.DropWrapper(
            self.df_json.encode('utf-8'),
            json.dumps([col for col in cols]).encode('utf-8')
        ).decode('utf-8')       
        return self
    def DropDuplicates(self, cols=None):
        if cols is None:
            cols_json = json.dumps([])
        else:
            cols_json = json.dumps(cols)
        self.df_json = gophers.DropDuplicatesWrapper(
            self.df_json.encode('utf-8'),
            cols_json.encode('utf-8')
        ).decode('utf-8')
        return self
    def DropNA(self, cols=None):
        if cols is None:
            cols_json = json.dumps([])
        else:
            cols_json = json.dumps(cols)
        self.df_json = gophers.DropNAWrapper(
            self.df_json.encode('utf-8'),
            cols_json.encode('utf-8')
        ).decode('utf-8')
        return self
    def FillNA(self, value):
        self.df_json = gophers.FillNAWrapper(
            self.df_json.encode('utf-8'),
            value.encode('utf-8')
        ).decode('utf-8')
        return self
    def Rename(self, old_name, new_name):
        self.df_json = gophers.RenameWrapper(
            self.df_json.encode('utf-8'),
            old_name.encode('utf-8'),
            new_name.encode('utf-8')
        ).decode('utf-8')
        return self
    def Explode(self, *cols):
        self.df_json = gophers.ExplodeWrapper(
            self.df_json.encode('utf-8'),
            json.dumps([col for col in cols]).encode('utf-8')
        ).decode('utf-8')
        return self
    # def Filter(self, condition):
    #     self.df_json = gophers.FilterWrapper(
    #         self.df_json.encode('utf-8'),
    #         condition.to_json().encode('utf-8')
    #     ).decode('utf-8')
    #     return self
    def Flatten(self, *cols):
        self.df_json = gophers.FlattenWrapper(
            self.df_json.encode('utf-8'),
            json.dumps([col for col in cols]).encode('utf-8')
        ).decode('utf-8')
        return self
    
    def KeysToCols(self, col):
        self.df_json = gophers.KeysToColsWrapper(
            self.df_json.encode('utf-8'),
            col.encode('utf-8')
        ).decode('utf-8')
        return self
    
    def StringArrayConvert(self, col_name):
        self.df_json = gophers.StringArrayConvertWrapper(
            self.df_json.encode('utf-8'),
            col_name.encode('utf-8')
        ).decode('utf-8')
        return self
    
    def Clone(self):
        """Return a new DataFrame copied from this one (deep copy)."""
        new_json = gophers.CloneWrapper(self.df_json.encode('utf-8')).decode('utf-8')
        return DataFrame(new_json)
    
    # Sink Functions
    def ToCSVFile(self, filename):
        gophers.ToCSVFileWrapper(self.df_json.encode('utf-8'), filename.encode('utf-8'))
        # add output giving file name/location
        return self
    
    def WriteSqlite(self, db_path: str, table: str, mode: str = "upsert", key_cols=None, create_index: bool = True):
        """
        Standard write to SQLite for this DataFrame.
        - mode: "overwrite" or "upsert"
        - key_cols: required for upsert; list/tuple of column names
        - create_index: create UNIQUE index on key_cols for upsert
        """
        keys_json = json.dumps(list(key_cols or []))
        res = gophers.WriteSqlite(
            db_path.encode("utf-8"),
            table.encode("utf-8"),
            self.df_json.encode("utf-8"),
            mode.encode("utf-8"),
            keys_json.encode("utf-8"),
            c_int(1 if create_index else 0),
        ).decode("utf-8")
        if res != "success":
            raise RuntimeError(res)
        return self    
# Example usage:
def main():
    data = '''
[
        {
            "name": "Julie Skels",
            "alignment": "Melee Maniac",
            "health": 90,
            "max_health": 100,
            "gold": 100,
            "background_history": "Works as a mercenary for the Red Company.",
            "body_appearance": "White woman with several scars etched on her body.",
            "inventory": [
                {
                    "name": "Custom-blade Knife",
                    "item_id": 2,
                    "equipped": true,
                    "description": "A sharp blade",
                    "estimated_value": 5,
                    "weight": 3,
                    "history": [
                        {
                            "date": "1st of September, 1069",
                            "event": "Received from the blacksmith in Anton."
                        }
                    ]
                }
            ],
            "stats": {
                "Strength": 15,
                "Intelligence": 10,
                "Dexterity": 12,
                "Charisma": 8,
                "Constitution": 14,
                "Wisdom": 11
            },
            "character_id": 0,
            "player_id": 0,
            "is_npc": false,
            "image_filename": "julie_skels.png",
            "in_party": true,
            "location_id": 2,
            "history": [
                {
                    "date": "1st of September, 1069",
                    "event": "Arrived at the marketplace for the festival."
                }
            ]
        },
        {
            "name": "Bob",
            "alignment": "Good",
            "health": 80,
            "max_health": 100,
            "gold": 30,
            "background_history": "Is the mayor of Anton.",
            "body_appearance": "Burly white man with a sword-bow.",
            "inventory": [
                {
                    "name": "Bow",
                    "item_id": 3,
                    "equipped": true,
                    "description": "A long bow",
                    "estimated_value": 3,
                    "weight": 2,
                    "history": []
                }
            ],
            "stats": {
                "Strength": 7,
                "Intelligence": 12,
                "Constitution": 10,
                "Dexterity": 15,
                "Wisdom": 8,
                "Charisma": 9
            },
            "character_id": 1,
            "player_id": -1,
            "is_npc": true,
            "image_filename": "bob.png",
            "in_party": true,
            "location_id": 2,
            "history": [
                {
                    "date": "1st of September, 1069",
                    "event": "Noticed goblin activity near Anton."
                }
            ]
        },
        {
            "name": "Alice",
            "alignment": "Neutral",
            "health": 100,
            "max_health": 100,
            "gold": 50,
            "background_history": "Daughter of Wolves.",
            "body_appearance": "Slim tall body, free of marks or scars.",
            "inventory": [
                {
                    "name": "Sword",
                    "item_id": 4,
                    "equipped": true,
                    "description": "A sharp sword",
                    "estimated_value": 12,
                    "weight": 5,
                    "history": []
                }
            ],
            "stats": {
                "Strength": 10,
                "Intelligence": 8,
                "Constitution": 12,
                "Dexterity": 13,
                "Wisdom": 9,
                "Charisma": 11
            },
            "character_id": 2,
            "player_id": -1,
            "is_npc": true,
            "image_filename": "alice.png",
            "in_party": false,
            "location_id": 1,
            "history": []
        }
    ]
'''
    df = ReadJSON(data)
    df = df.Explode("inventory")
    df = df.Flatten("inventory")
    df = df.Flatten("stats")
    # df.Vertical(100, 10)
    # report = CreateReport("Test Report")
    # report.AddPage("Main Page")
    # report.AddHeading("Main Page", "This is the main page of the report", 1)
    # report.AddDataframe("Main Page", df)
    # report.Open()
    # df = GetAPIJSON("https://poetrydb.org/title/Ozymandias/lines.json","","")
    # df = df.Explode("lines")
    # print(str(GetSqliteSchema("db.sqlite3", "SCOPS2_Child_App_email")).replace("'", '"').replace(": None,", ': "null",'))
    # df = ReadJSON(str(GetSqliteSchema("db.sqlite3", "SCOPS2_Child_App_email")).replace("'", '"').replace(": None,", ': "null",'))
    # df = df.Select("columns")\
    #     .Explode("columns")\
    #     .Flatten("columns")
    # df.Vertical(100, 10)
    # # df.DisplayBrowser()
    # report = CreateReport("SQLite Schema Report")
    # report.AddPage("Schema Page")
    # report.AddHeading("Schema Page", "Schema of SCOPS2_Child_App_email Table", 1)
    # report.AddDataframe("Schema Page", df)
    # report.Save("sqlite_schema_report.html").Open()
    df.DisplayBrowser()
    # df.Vertical(100, 10)
    # pass

if __name__ == '__main__':
    main()