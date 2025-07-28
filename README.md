# L5-DE-Hackathon-3-LH

## Start with the pipeline structure:

/data-pipeline-hackathon/ │ ├── data/ │ ├── Edinburgh-daytime.xlsx │ └── Strathspey-weather.xlsx │ ├── notebooks/ │ └── exploration.ipynb # optional │ ├── pipeline/ │ ├── init.py │ ├── ingest.py # read_excel and basic structure │ ├── clean.py # cleaning logic │ ├── transform.py # transformations and derived columns │ ├── merge.py # integration logic │ ├── output/ │ └── merged_summary.csv # or .parquet/.json │ ├── main.py # runs the full pipeline ├── requirements.txt # pandas, openpyxl └── README.md # instructions
