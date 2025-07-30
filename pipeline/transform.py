import pandas as pd
import datetime
from pipeline import utils
from pipeline.utils import get_month_year
from pipeline.utils import drop_rows, drop_empty_rows, clean_columns, define_snow, define_date

def clean_daytime_sheets(sheets, column_names):
    """ Processing all sheets and concatenating them into a single DataFrame """
    all_sheets = []
    for k in sheets.keys():
        month, year = get_month_year(k)   
        df = sheets[k]
        
        # change column names
        df.columns = column_names
        # or
        # df = df.drop(df.index[[0, 1]])
        
        # drop first n rows and any empties
        df = drop_rows(df, 2)
        df = drop_empty_rows(df).reset_index(drop=True)
        
        # create sheet flag
        # df['sheet'] = k 
        
        # clean columns
        for col in df.columns:
            if col != 'date':
                df[col] = df[col].apply(clean_columns)
        for col in ['sunrise','sunset','solar_noon_time']:
            df[col] = df[col].str.split('(', expand=True).loc[:,0] 
            
        # update date
        df["date"] = df["date"].apply(lambda x: datetime.date(year, month, int(x)))
        all_sheets.append(df)
        
    return pd.concat(all_sheets, ignore_index=True)


def clean_weather_sheets(sheets, column_names):
    """ Processing all sheets and concatenating them into a single DataFrame """
    all_sheets = []
    for k in sheets.keys():
        # print(f"YYMM: {k}")
        month, year = get_month_year(k)
        df = sheets[k]
        
        # # change column names
        df.columns = column_names
        
        # # drop first five rows and any empties
        df = drop_rows(df, 5)
        df = drop_empty_rows(df).reset_index(drop=True)
        # # or
        # # df = df.drop(df.index[[0, 1]])
        
        # define snow column
        df = define_snow(df)
        
        # # clean columns
        for col in df.columns:
            if col not in ['date', 'wind_direction']:
                df[col] = df[col].apply(clean_columns)
        # update date
        df = define_date(df, year, month)
        all_sheets.append(df)
        
    return pd.concat(all_sheets, ignore_index=True)