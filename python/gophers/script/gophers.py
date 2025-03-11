from ctypes import cdll, c_char_p, c_int
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
gophers.DisplayHTMLWrapper.restype = c_char_p
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


class ColumnExpr:
    def __init__(self, expr):
        self.expr = expr

    def to_json(self):
        return json.dumps(self.expr)

    def Help(self):
        print("Column Help:")
        print("\tIsNull()")
        print("\tIsNotNull()")
        print("\tIsIn(values)")
        print("\tIsBetween(lower, upper)")
        print("\tLike(pattern)")
        print("\tNotLike(pattern)")
        print("\tStartsWith(prefix)")
        print("\tEndsWith(suffix)")
        print("\tContains(substr)")
        print("\tNotContains(substr)")
        print("\tReplace(old, new)")
        print("\tTrim()")
        print("\tLTrim()")
        print("\tRTrim()")
        print("\tLower()")
        print("\tUpper()")
        print("\tTitle()")
        print("\tSubstr(start, length)")
        print("\tGt(other)")
        print("\tLt(other)")
        print("\tGe(other)")
        print("\tLe(other)")
        print("\tEq(other)")
        print("\tNe(other)")
        
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
        print("Report Help:")
        print("\tOpen()")
        print("\tSave(filename)")
        print("\tAddPage(name)")
        print("\tAddHTML(page, text)")
        print("\tAddDataframe(page, df)")
        print("\tAddChart(page, chart)")
        print("\tAddHeading(page, text, size)")
        print("\tAddText(page, text)")
        print("\tAddSubText(page, text)")
        print("\tAddBullets(page, bullets)")

    def Open(self):
        # print("")
        print("printing open report:"+self.report_json)

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
    print("Functions Help:")
    print("\tReadJSON(json_data)")
    print("\tReadNDJSON(json_data)")
    print("\tReadCSV(csv_data)")
    print("\tReadYAML(yaml_data)")
    print("\tGetAPIJSON(endpoint, headers, query_params)")
    print("\tSum(column_name)")
    print("\tAgg(*aggregations)")
    print("\tCol(name)")
    print("\tLit(value)")
    print("\tOr(left, right)")
    print("\tAnd(left, right)")
    print("\tIf(condition, trueExpr, falseExpr)")
    print("\tSHA256(*cols)")
    print("\tSHA512(*cols)")
    print("\tCollectList(col_name)")
    print("\tCollectSet(col_name)")
    print("\tSplit(col_name, delimiter)")

    
# Aggregate functions
def Sum(column_name):
    # Call the Go SumWrapper function with only the column name
    sum_agg_json = gophers.SumWrapper(column_name.encode('utf-8')).decode('utf-8')
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

def Concat(*cols):
    """
    Returns a ColumnExpr that concatenates the string representations
    of the given column expressions.
    """
    return ColumnExpr({
        "type": "concat",
        "cols": [json.loads(col.to_json()) for col in cols]
    })

def Concat_WS(delimiter, *cols):
    """
    Returns a ColumnExpr that concatenates the string representations
    of the given column expressions using the specified delimiter.
    """
    return ColumnExpr({
        "type": "concat_ws",
        "delimiter": delimiter,
        "cols": [json.loads(col.to_json()) for col in cols]
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

# PANDAS FUNCTIONS
# loc
# iloc

# Dataframe + Methods
class DataFrame:
    def __init__(self, df_json=None):
        self.df_json = df_json

    def Help(self):
        print("DataFrame Help:")
        print("\tShow(chars, record_count)")   
        print("\tColumns()")
        print("\tCount()")
        print("\tCountDuplicates(cols)")
        print("\tCountDistinct(cols)")
        print("\tCollect(col_name)")
        print("\tHead(chars)")
        print("\tTail(chars)")
        print("\tVertical(chars, record_count)")
        print("\tDisplayBrowser()")
        print("\tDisplay()")
        print("\tDisplayToFile(file_path)")
        print("\tBarChart(title, subtitle, groupcol, aggs)")
        print("\tColumnChart(title, subtitle, groupcol, aggs)")
        print("\tStackedBarChart(title, subtitle, groupcol, aggs)")
        print("\tStackedPercentChart(title, subtitle, groupcol, aggs)")
        print("\tColumn(col_name, col_spec)")
        print("\tGroupBy(groupCol, aggs)")
        print("\tSelect(*cols)")
        print("\tUnion(df2)")
        print("\tJoin(df2, col1, col2, how)")
        print("\tSort(*cols)")
        print("\tFilter(condition)")
        print("\tOrderBy(col, asc)")
        print("\tDrop(*cols)")
        print("\tDropDuplicates(cols)")
        print("\tDropNA(cols)")
        print("\tFillNA(value)")
        print("\tRenameWrapper(old_name, new_name)")
        print("\tCreateReport(title)")
        print("\tToCSVFile(filename)")
        
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
        display(HTML(html))
        return self
    
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
        self.df_json = gophers.FilterWrapper(
            self.df_json.encode('utf-8'),
            condition.to_json().encode('utf-8')
        ).decode('utf-8')
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
    def Filter(self, condition):
        self.df_json = gophers.FilterWrapper(
            self.df_json.encode('utf-8'),
            condition.to_json().encode('utf-8')
        ).decode('utf-8')
        return self
    
    # Report methods
    def CreateReport(self, title):
        report_json = gophers.CreateReportWrapper(self.df_json.encode('utf-8'), title.encode('utf-8')).decode('utf-8')
        # print("CreateReport: Created report JSON:", report_json)
        return Report(report_json)
    
    # Sink Functions
    def ToCSVFile(self, filename):
        gophers.ToCSVFileWrapper(self.df_json.encode('utf-8'), filename.encode('utf-8'))
        # add output giving file name/location
        return self
    
# Example usage:
def main():
    pass

if __name__ == '__main__':
    main()