from ctypes import cdll, c_char_p, c_int
import os
import json
from IPython.display import HTML, display

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

class Dashboard:
    def __init__(self, gophers, dashboard_json):
        self.gophers = gophers
        self.dashboard_json = dashboard_json

    def Open(self):
        err = self.gophers.OpenDashboardWrapper(self.dashboard_json.encode('utf-8')).decode('utf-8')
        if err:
            print("Error opening dashboard:", err)
        return self

    def Save(self, filename):
        err = self.gophers.SaveDashboardWrapper(self.dashboard_json.encode('utf-8'), filename.encode('utf-8')).decode('utf-8')
        if err:
            print("Error saving dashboard:", err)
        return self

    def AddPage(self, name):
        result = self.gophers.AddPageWrapper(self.dashboard_json.encode('utf-8'), name.encode('utf-8')).decode('utf-8')
        if result:
            self.dashboard_json = result
            print("AddPage: Updated dashboard JSON:", self.dashboard_json)
        else:
            print("Error adding page:", result)
        return self

    def AddHTML(self, page, text):
        result = self.gophers.AddHTMLWrapper(self.dashboard_json.encode('utf-8'), page.encode('utf-8'), text.encode('utf-8')).decode('utf-8')
        if result:
            self.dashboard_json = result
        else:
            print("Error adding HTML:", result)
        return self

    def AddDataframe(self, page, df):
        result = self.gophers.AddDataframeWrapper(self.dashboard_json.encode('utf-8'), page.encode('utf-8'), df.df_json.encode('utf-8')).decode('utf-8')
        if result:
            self.dashboard_json = result
        else:
            print("Error adding dataframe:", result)
        return self

    def AddChart(self, page, chart):
        chart_json = json.dumps(chart.__dict__)
        result = self.gophers.AddChartWrapper(self.dashboard_json.encode('utf-8'), page.encode('utf-8'), chart_json.encode('utf-8')).decode('utf-8')
        if result:
            self.dashboard_json = result
        else:
            print("Error adding chart:", result)
        return self

    def AddHeading(self, page, text):
        result = self.gophers.AddHeadingWrapper(self.dashboard_json.encode('utf-8'), page.encode('utf-8'), text.encode('utf-8')).decode('utf-8')
        if result:
            self.dashboard_json = result
        else:
            print("Error adding heading:", result)
        return self

    def AddText(self, page, text):
        result = self.gophers.AddTextWrapper(self.dashboard_json.encode('utf-8'), page.encode('utf-8'), text.encode('utf-8')).decode('utf-8')
        if result:
            self.dashboard_json = result
        else:
            print("Error adding text:", result)
        return self

    def AddSubText(self, page, text):
        result = self.gophers.AddSubTextWrapper(self.dashboard_json.encode('utf-8'), page.encode('utf-8'), text.encode('utf-8')).decode('utf-8')
        if result:
            self.dashboard_json = result
        else:
            print("Error adding subtext:", result)
        return self

    def AddBullets(self, page, bullets):
        bullets_json = json.dumps(bullets)
        result = self.gophers.AddBulletsWrapper(self.dashboard_json.encode('utf-8'), page.encode('utf-8'), bullets_json.encode('utf-8')).decode('utf-8')
        if result:
            self.dashboard_json = result
        else:
            print("Error adding bullets:", result)
        return self
    
# Helper for extraction (already available in Go as Col)
def Col(source):
    return FuncColumn("Col",source)

# Helper for extraction (already available in Go as Col)
def Lit(source):
    return FuncColumn("Lit",source)

# sum
def Sum(source):
    return FuncColumn("Sum", source)

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
        self.gophers.DisplayWrapper.restype = c_char_p
        self.gophers.DisplayToFileWrapper.restype = c_char_p
        self.gophers.DisplayHTMLWrapper.restype = c_char_p
        self.gophers.DisplayChartWrapper.restype = c_char_p
        self.gophers.BarChartWrapper.restype = c_char_p
        self.gophers.ColumnChartWrapper.restype = c_char_p
        self.gophers.StackedBarChartWrapper.restype = c_char_p
        self.gophers.StackedPercentChartWrapper.restype = c_char_p
        self.gophers.GroupByWrapper.restype = c_char_p
        self.gophers.SumWrapper.restype = c_char_p
        self.gophers.MaxWrapper.restype = c_char_p
        self.gophers.MinWrapper.restype = c_char_p
        self.gophers.MedianWrapper.restype = c_char_p
        self.gophers.MeanWrapper.restype = c_char_p
        self.gophers.ModeWrapper.restype = c_char_p
        self.gophers.UniqueWrapper.restype = c_char_p
        self.gophers.FirstWrapper.restype = c_char_p
        self.gophers.CreateDashboardWrapper.restype = c_char_p
        self.gophers.OpenDashboardWrapper.restype = c_char_p
        self.gophers.SaveDashboardWrapper.restype = c_char_p
        self.gophers.AddPageWrapper.restype = c_char_p
        self.gophers.AddHTMLWrapper.restype = c_char_p
        self.gophers.AddDataframeWrapper.restype = c_char_p
        self.gophers.AddChartWrapper.restype = c_char_p
        self.gophers.AddHeadingWrapper.restype = c_char_p
        self.gophers.AddTextWrapper.restype = c_char_p
        self.gophers.AddSubTextWrapper.restype = c_char_p
        self.gophers.AddBulletsWrapper.restype = c_char_p

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
    
    def Display(self):
        html = self.gophers.DisplayWrapper(self.df_json.encode('utf-8')).decode('utf-8')
        display(HTML(html))
        return self
    
    def DisplayToFile(self, file_path):
        err = self.gophers.DisplayToFileWrapper(self.df_json.encode('utf-8'), file_path.encode('utf-8')).decode('utf-8')
        if err:
            print("Error writing to file:", err)
        return self
        
    def DisplayHTML(self, html_content):
        html = self.gophers.DisplayHTMLWrapper(html_content.encode('utf-8')).decode('utf-8')
        display(HTML(html))
        return self
    
    def DisplayChart(self, chart):
        chart_json = json.dumps(chart.__dict__)
        html = self.gophers.DisplayChartWrapper(chart_json.encode('utf-8')).decode('utf-8')
        display(HTML(html))
        return self
    
    def BarChart(self, title, subtitle, groupcol, aggs):
        aggs_json = json.dumps([agg.__dict__ for agg in aggs])
        html = self.gophers.BarChartWrapper(self.df_json.encode('utf-8'), title.encode('utf-8'), subtitle.encode('utf-8'), groupcol.encode('utf-8'), aggs_json.encode('utf-8')).decode('utf-8')
        display(HTML(html))
        return self
    
    def ColumnChart(self, title, subtitle, groupcol, aggs):
        aggs_json = json.dumps([agg.__dict__ for agg in aggs])
        html = self.gophers.ColumnChartWrapper(self.df_json.encode('utf-8'), title.encode('utf-8'), subtitle.encode('utf-8'), groupcol.encode('utf-8'), aggs_json.encode('utf-8')).decode('utf-8')
        display(HTML(html))
        return self
    
    def StackedBarChart(self, title, subtitle, groupcol, aggs):
        aggs_json = json.dumps([agg.__dict__ for agg in aggs])
        html = self.gophers.StackedBarChartWrapper(self.df_json.encode('utf-8'), title.encode('utf-8'), subtitle.encode('utf-8'), groupcol.encode('utf-8'), aggs_json.encode('utf-8')).decode('utf-8')
        display(HTML(html))
        return self
    
    def StackedPercentChart(self, title, subtitle, groupcol, aggs):
        aggs_json = json.dumps([agg.__dict__ for agg in aggs])
        html = self.gophers.StackedPercentChartWrapper(self.df_json.encode('utf-8'), title.encode('utf-8'), subtitle.encode('utf-8'), groupcol.encode('utf-8'), aggs_json.encode('utf-8')).decode('utf-8')
        display(HTML(html))
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
        elif isinstance(col_spec, str) and col_spec.startswith("CollectList"):
            # col_spec is in the form "CollectList:colname"
            src = col_spec.split(":", 1)[1]
            self.df_json = self.gophers.ColumnCollectList(
                self.df_json.encode('utf-8'),
                col_name.encode('utf-8'),
                src.encode('utf-8')
            ).decode('utf-8')
        # Similarly for CollectSet.
        elif isinstance(col_spec, str) and col_spec.startswith("CollectSet"):
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
    
    def CreateDashboard(self, title):
        dashboard_json = self.gophers.CreateDashboardWrapper(self.df_json.encode('utf-8'), title.encode('utf-8')).decode('utf-8')
        print("CreateDashboard: Created dashboard JSON:", dashboard_json)
        return Dashboard(self.gophers, dashboard_json)
    
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
    df.Display()
    
    # Example dashboard usage
    dashboard = df.CreateDashboard("My Dashboard")
    dashboard.AddPage("Page1")
    dashboard.AddText("Page1", "This is some text on Page 1")
    # dashboard.Save("dashboard.html")
    dashboard.Open()

if __name__ == '__main__':
    main()