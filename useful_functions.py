################################ BI USEFUL FUNCTIONS MODULE ################################

import io
import xlsxwriter
from string import ascii_lowercase
import pandas as pd
import s3fs, boto3, os
import datetime
import re
from dateutil.relativedelta import relativedelta

s3fs = s3fs.S3FileSystem()
s3resource = boto3.resource('s3')
s3client = boto3.client('s3')

################# Excel extraction ################  

class SmartExcel:
    
    """
    =======================================================================================
                    Class to extract multiple tables in multiple .xlsx sheets
    =======================================================================================
    
    -----------------------------------------Args------------------------------------------
    
    tabs:
        -input a dictionary of tables to be downloaded from Dremio where keys are
         preferred table names and items are SQL select statements
    
    dremio_access:
        -Your dremio_access instance    
    
    file_name:
        -name of the final .xlsx file
    
    path:
        -path to the S3 folder 
        -Default = f"{reports}"
        
    sheet_name:
        -name of the sheet that the data will be presented on
        -Default = 'Data'
    
    bucket:
        -S3 bucket
        -Default = None
    
    ----------------------------------------Example----------------------------------------
    
    test = SmartExcel({'Account Level Latest'     : f"select * from {temp}.account_level",
                       'Account Level History ME' : f"select * from {temp}.account_history_36_months",
                       'Transaction History'      : f"select * from {temp}.transactions_36_months",
                       'Account Rates History'    : f"select * from {temp}.aprs_history_36_months",
                       'Cycle Level History ME'   : f"select * from {temp}.cycle_history_36_months"},
                        dremio_access)

    test.export('test','enterprise_risk_data/reports')
    """    
    
    
    def __init__(self, tabs, dremio_access):
        
        assert isinstance(tabs, dict), "Please input tabs as dictionary, e.g. {'Sheet1' : 'select * from x'}"
        
        self.dremio_access = dremio_access
        self.tabs = tabs
        self._get_tables(tabs)

    def _get_tables(self, tabs):
        
        status = 1
        total = len(tabs)
        self.tables = {}
        
        for sheet, sql in tabs.items():
            
            print(f"Loading {sheet}... Status: {status}/{total}")
            self.tables[sheet] = self.dremio_access.read_sql_to_dataframe(sql)
            status += 1
    
    def _excel_column_width(self, df: pd.DataFrame, with_col: bool = True, default = 8.43, startcol = 0) -> dict:
    
        return {i:round(min(1.0528*max([len(str(x)) for x in df[col]] + [len(col) if with_col else default])+2.2974, 80), 2) for i, col in enumerate(df.columns, startcol)}
    
    def export(self, file_name, path, autowidth = True, bucket = s3_access.get_sandbox_bucket_name()):
        
        assert isinstance(file_name, str), 'Please provide file_name as string'
        assert isinstance(path, str), 'Please provide path as string'
        
        self.bucket = bucket
        
        with io.BytesIO() as out:
            with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
                
                for sheet_name, df in self.tables.items():
                    
                    df.to_excel(writer, sheet_name, index = False)
                    
                    if autowidth:
                        d_col_width = self._excel_column_width(df)
                        worksheet = writer.sheets[sheet_name]

                        for x,y in d_col_width.items():
                            worksheet.set_column(x, x, y)
                            
                    print(f"Loaded {sheet_name!r} sheet")

                
            self.excel_data = out.getvalue()

            s3resource.Bucket(self.bucket).put_object(Key=f'{path}/{file_name}.xlsx', Body=self.excel_data)
            print(f"\nCreated excel for {file_name!r} at {path!r}") 
            
            
            
################ List of  UK Holidays ###################

from pandas.tseries.holiday import (
    AbstractHolidayCalendar, DateOffset, EasterMonday,
    GoodFriday, Holiday, MO,
    next_monday, next_monday_or_tuesday)

class EnglandHolidayCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('New Years Day', month=1, day=1, observance=next_monday),
        GoodFriday,
        EasterMonday,
        Holiday('Early May bank holiday',
                month=5, day=1, offset=DateOffset(weekday=MO(1))),
        Holiday('Spring bank holiday',
                month=5, day=31, offset=DateOffset(weekday=MO(-1))),
        Holiday('Summer bank holiday',
                month=8, day=31, offset=DateOffset(weekday=MO(-1))),
        Holiday('Christmas Day', month=12, day=25, observance=next_monday),
        Holiday('Boxing Day',
                month=12, day=26, observance=next_monday_or_tuesday)
    ]

#all holidays from 2000 to 2030
holidays = [x.date() for x in EnglandHolidayCalendar().holidays(datetime.date(2000,1,1))]




def bi_date_ago(date, days = 0, months = 0, years = 0, start_or_end = None):
    """
    Calculates and returns a historic date based on the parameters provided
    
    Args:
        -date: the date to use
        -days: days ago (default = 0)
        -months: months ago (default = 0)
        -years: years ago (default = 0)
        -start_or_end: whether to give start or end of the derived months. Returns the derived date if not specified (default None)
    
    Examples:
        
        days_ago1 = bi_date_ago(run_date, days = 1)
        months_ago6 = bi_date_ago(run_date, months = 6)
        
        months_ago48_start = bi_date_ago(run_date, months = 48, start_or_end = 'start')
        
        years_ago4_end = bi_date_ago(run_date, years = 4, start_or_end = 'end')
    """
    
    
    if start_or_end:
        date = date - relativedelta(days = days)

        months += years*12
        
        if start_or_end.lower() == 'end':
            return (date - pd.offsets.MonthEnd(months)).date()
        
        elif start_or_end.lower() == 'start':
            months += 0 if date.day == 1 else 1
            return (date - pd.offsets.MonthBegin(months)).date()
        
        else:
            raise ValueError(f"start_or_end can either be 'start' or 'end'")
    
    else:
        return date - relativedelta(days = days, months = months, years = years)
