# %%
import pandas as pd
from pipeline import ingest, transform, utils, resample
from config.config import EDINBURGH_COLUMNS, EDINBURGH_DATA, STRATHSPEY_COLUMNS, STRATHSPEY_DATA, LONDON_DATA, ROYSTON_COLUMNS, ROYSTON_DATA
from pipeline.ingest import load_sheets
from pipeline.transform import clean_daytime_sheets
from pipeline.transform import clean_weather_sheets
from pipeline.utils import merge_datasets
from pipeline.utils import get_mins_from_time

# %%
edin_sheets = load_sheets(EDINBURGH_DATA)
edin_df = clean_daytime_sheets(edin_sheets, EDINBURGH_COLUMNS)
edin_df.head(10)

# %%
strat_sheets = load_sheets(STRATHSPEY_DATA)
strat_df = clean_weather_sheets(strat_sheets, STRATHSPEY_COLUMNS, drop_n = 5, drop_start=True)
strat_df.head(10)

# %%
scottish_merged = merge_datasets(edin_df, strat_df)
scottish_merged.to_csv('outputs/scottish_weather_data.csv', index=False)
scottish_merged.head()

# %%
scottish_merged_filt = utils.filter_by_year(scottish_merged, 2012)
scottish_minute_temp = resample.generate_minute_estimates(scottish_merged_filt)
scottish_minute_temp.to_csv('outputs/scottish_minute_temp.csv', index=False)
scottish_minute_temp.head()

# %%
london_sheets = ingest.load_sheets(LONDON_DATA)
london_df = transform.clean_daytime_sheets(london_sheets, EDINBURGH_COLUMNS)
london_df.head()

# %%
royston_sheets = ingest.load_sheets(ROYSTON_DATA)
royston_df = transform.clean_weather_sheets(royston_sheets, ROYSTON_COLUMNS, 3, drop_start=False)
royston_df.head()

# %%
london_merged = utils.merge_datasets(london_df, royston_df)
london_merged.to_csv('outputs/london_weather_data.csv', index=False)
london_merged.head()

# %%
london_merged_filt = utils.filter_by_year(london_merged, 2012)
london_minute_temp = resample.generate_minute_estimates(london_merged_filt)
london_minute_temp.to_csv('outputs/london_minute_temp.csv', index=False)
london_minute_temp.head()


