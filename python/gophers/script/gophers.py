from ctypes import cdll, c_char_p, c_int
import os
import json
from IPython.display import HTML, display

path = os.path.dirname(os.path.realpath(__file__))
gophers = cdll.LoadLibrary(path + '/go_module/gophers.so')
# Set restype for functions at module load time
gophers.ReadJSON.restype = c_char_p
gophers.Show.restype = c_char_p
gophers.Head.restype = c_char_p
gophers.Tail.restype = c_char_p
gophers.Vertical.restype = c_char_p
gophers.ColumnOp.restype = c_char_p
gophers.ColumnCollectList.restype = c_char_p
gophers.ColumnCollectSet.restype = c_char_p
gophers.ColumnSplit.restype = c_char_p
gophers.DFColumns.restype = c_char_p
gophers.DFCount.restype = c_int
gophers.DFCountDuplicates.restype = c_int
gophers.DFCountDistinct.restype = c_int
gophers.DFCollect.restype = c_char_p
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
gophers.AggWrapper.restype = c_char_p
gophers.SumWrapper.restype = c_char_p
gophers.MaxWrapper.restype = c_char_p
gophers.MinWrapper.restype = c_char_p
gophers.MedianWrapper.restype = c_char_p
gophers.MeanWrapper.restype = c_char_p
gophers.ModeWrapper.restype = c_char_p
gophers.UniqueWrapper.restype = c_char_p
gophers.FirstWrapper.restype = c_char_p
gophers.CreateDashboardWrapper.restype = c_char_p
gophers.OpenDashboardWrapper.restype = c_char_p
gophers.SaveDashboardWrapper.restype = c_char_p
gophers.AddPageWrapper.restype = c_char_p
gophers.AddHTMLWrapper.restype = c_char_p
gophers.AddDataframeWrapper.restype = c_char_p
gophers.AddChartWrapper.restype = c_char_p
gophers.AddHeadingWrapper.restype = c_char_p
gophers.AddTextWrapper.restype = c_char_p
gophers.AddSubTextWrapper.restype = c_char_p
gophers.AddBulletsWrapper.restype = c_char_p
gophers.ToCSVFileWrapper.restype = c_char_p

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

class Chart:
    def __init__(self, html):
        self.html = html

class Dashboard:
    def __init__(self, dashboard_json):
        self.dashboard_json = dashboard_json

    def Open(self):
        # print("")
        print("printing open dashboard:"+self.dashboard_json)

        err = gophers.OpenDashboardWrapper(self.dashboard_json.encode('utf-8')).decode('utf-8')
        if err != "success":
            print("Error opening dashboard:", err)
        return self

    def Save(self, filename):
        err = gophers.SaveDashboardWrapper(self.dashboard_json.encode('utf-8'), filename.encode('utf-8')).decode('utf-8')
        if err:
            print("Error saving dashboard:", err)
        return self

    def AddPage(self, name):
        result = gophers.AddPageWrapper(self.dashboard_json.encode('utf-8'), name.encode('utf-8')).decode('utf-8')
        if result:
            self.dashboard_json = result
            # print("AddPage: Updated dashboard JSON:", self.dashboard_json)
        else:
            print("Error adding page:", result)
        return self

    def AddHTML(self, page, text):
        result = gophers.AddHTMLWrapper(self.dashboard_json.encode('utf-8'), page.encode('utf-8'), text.encode('utf-8')).decode('utf-8')
        if result:
            self.dashboard_json = result
        else:
            print("Error adding HTML:", result)
        return self

    def AddDataframe(self, page, df):
        result = gophers.AddDataframeWrapper(self.dashboard_json.encode('utf-8'), page.encode('utf-8'), df.df_json.encode('utf-8')).decode('utf-8')
        if result:
            self.dashboard_json = result
        else:
            print("Error adding dataframe:", result)
        return self

    def AddChart(self, page, chart):
        chart_json = chart.html
        # print(f"Chart JSON: {chart_json}")

        result = gophers.AddChartWrapper(
            self.dashboard_json.encode('utf-8'),
            page.encode('utf-8'),
            chart_json.encode('utf-8')
        ).decode('utf-8')

        if result:
            # print(f"Chart added successfully, result: {result[:100]}...")
            self.dashboard_json = result
        else:
            print(f"Error adding chart, empty result")
        return self
    def AddHeading(self, page, text, size):
        result = gophers.AddHeadingWrapper(self.dashboard_json.encode('utf-8'), page.encode('utf-8'), text.encode('utf-8'), size).decode('utf-8')
        if result:
            self.dashboard_json = result
        else:
            print("Error adding heading:", result)
        return self

    def AddText(self, page, text):
        result = gophers.AddTextWrapper(self.dashboard_json.encode('utf-8'), page.encode('utf-8'), text.encode('utf-8')).decode('utf-8')
        if result:
            self.dashboard_json = result
        else:
            print("Error adding text:", result)
        return self

    def AddSubText(self, page, text):
        result = gophers.AddSubTextWrapper(self.dashboard_json.encode('utf-8'), page.encode('utf-8'), text.encode('utf-8')).decode('utf-8')
        if result:
            self.dashboard_json = result
        else:
            print("Error adding subtext:", result)
        return self

    def AddBullets(self, page, bullets):
        bullets_json = json.dumps(bullets)
        result = gophers.AddBulletsWrapper(self.dashboard_json.encode('utf-8'), page.encode('utf-8'), bullets_json.encode('utf-8')).decode('utf-8')
        if result:
            self.dashboard_json = result
        else:
            print("Error adding bullets:", result)
        return self
    
def Sum(column_name):
    # Call the Go SumWrapper function with only the column name
    sum_agg_json = gophers.SumWrapper(column_name.encode('utf-8')).decode('utf-8')
    # Parse the JSON string into a Python dict before returning it
    return json.loads(sum_agg_json)

def Agg(*aggregations):
    # Simply return the list of aggregations
    return list(aggregations)

# Helper for extraction (already available in Go as Col)
def Col(source):
    return FuncColumn("Col",source)

# Helper for extraction (already available in Go as Col)
def Lit(source):
    return FuncColumn("Lit",source)

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

def ReadJSON(json_data):
    # Store the JSON representation of DataFrame from Go.
    df_json = gophers.ReadJSON(json_data.encode('utf-8')).decode('utf-8')
    return DataFrame(df_json)

# PANDAS FUNCTIONS
# loc
# iloc

class DataFrame:
    def __init__(self, df_json=None):
        self.df_json = df_json

    def Show(self, chars, record_count=100):
        result = gophers.Show(self.df_json.encode('utf-8'), c_int(chars), c_int(record_count)).decode('utf-8')
        print(result)

    def Columns(self):
        cols_json = gophers.DFColumns(self.df_json.encode('utf-8')).decode('utf-8')
        return json.loads(cols_json)

    def Count(self):
        return gophers.DFCount(self.df_json.encode('utf-8'))

    def CountDuplicates(self, cols=None):
        if cols is None:
            cols_json = json.dumps([])
        else:
            cols_json = json.dumps(cols)
        return gophers.DFCountDuplicates(self.df_json.encode('utf-8'),
                                              cols_json.encode('utf-8'))

    def CountDistinct(self, cols=None):
        if cols is None:
            cols_json = json.dumps([])
        else:
            cols_json = json.dumps(cols)
        return gophers.DFCountDistinct(self.df_json.encode('utf-8'),
                                            cols_json.encode('utf-8'))

    def Collect(self, col_name):
        collected = gophers.DFCollect(self.df_json.encode('utf-8'),
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
    
    def Column(self, col_name, col_spec):
        # If col_spec is an instance of ColumnExpr, use ColumnFrom.
        if isinstance(col_spec, FuncColumn):
            cols_json = json.dumps(col_spec.cols)
            self.df_json = gophers.ColumnOp(
                self.df_json.encode('utf-8'),
                col_name.encode('utf-8'),
                col_spec.func_name.encode('utf-8'),
                cols_json.encode('utf-8')
            ).decode('utf-8')
        # Check for CollectList marker (a string) and call ColumnCollectList.
        elif isinstance(col_spec, str) and col_spec.startswith("CollectList"):
            # col_spec is in the form "CollectList:colname"
            src = col_spec.split(":", 1)[1]
            self.df_json = gophers.ColumnCollectList(
                self.df_json.encode('utf-8'),
                col_name.encode('utf-8'),
                src.encode('utf-8')
            ).decode('utf-8')
        # Similarly for CollectSet.
        elif isinstance(col_spec, str) and col_spec.startswith("CollectSet"):
            src = col_spec.split(":", 1)[1]
            self.df_json = gophers.ColumnCollectSet(
                self.df_json.encode('utf-8'),
                col_name.encode('utf-8'),
                src.encode('utf-8')
            ).decode('utf-8')
        # For Split, expect a tuple: (source, delimiter)
        elif isinstance(col_spec, SplitColumn):
            src, delim = col_spec
            self.df_json = gophers.ColumnSplit(
                self.df_json.encode('utf-8'),
                col_name.encode('utf-8'),
                src.encode('utf-8'),
                delim.encode('utf-8')
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
    def CreateDashboard(self, title):
        dashboard_json = gophers.CreateDashboardWrapper(self.df_json.encode('utf-8'), title.encode('utf-8')).decode('utf-8')
        # print("CreateDashboard: Created dashboard JSON:", dashboard_json)
        return Dashboard(dashboard_json)
    def ToCSVFile(self, filename):
        gophers.ToCSVFileWrapper(self.df_json.encode('utf-8'), filename.encode('utf-8'))
        # add output giving file name/location
        return self
    
# Example usage:
def main():
    json_data = '[{"col1": "value1", "col2": 2, "col3": 3}, {"col1": "value4", "col2": 5, "col3": 3}, {"col1": "value7", "col2": 1, "col3": 3}]'
        # Ensure json_data is a string before encoding
    if not isinstance(json_data, str):
        json_data = str(json_data)
    df = ReadJSON(json_data)
    # print("Head:")
    # df.Head(25)
    # print("Tail:")
    # df.Tail(25)
    # print("Vertical:")
    # df.Vertical(25, record_count=3)
    # print("Columns:")
    # print(df.Columns())
    # df.Display()
    
    # Example dashboard usage
    dashboard = df.CreateDashboard("My Dashboard")
    dashboard.AddPage("Page1")
    dashboard.AddText("Page1", "This is some text on Page 1")
    dashboard.AddHeading("Page1", "Text on Page 1",4)
    dashboard.AddPage("Page2")
    
    chart = df.ColumnChart("barchart","subtext","col1", Agg(Sum("col2")))
    # DisplayChart(chart)
    dashboard.AddChart("Page1", chart)
    df.GroupBy("col1", Agg(Sum("col2"),Sum("col3"))).Show(25)
    # dashboard.Save("dashboard.html")
    # dashboard.Open()
    df.ToCSVFile('newcsvgophers.csv')
    # print(chart)

if __name__ == '__main__':
    main()