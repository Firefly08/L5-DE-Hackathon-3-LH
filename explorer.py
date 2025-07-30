# %%
import pandas as pd
from pipeline import ingest, transform, utils, resample
from config.config import EDINBURGH_COLUMNS, EDINBURGH_DATA, STRATHSPEY_COLUMNS, STRATHSPEY_DATA
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
strat_df = clean_weather_sheets(strat_sheets, STRATHSPEY_COLUMNS)
strat_df.head(10)

# %%
merged = merge_datasets(edin_df, strat_df)
merged.head(10)

# %%
merged.to_csv('outputs/scottish_weather_data1.csv', index=False)


