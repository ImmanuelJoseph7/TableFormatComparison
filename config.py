import os
from dotenv import load_dotenv

load_dotenv()

from urllib.parse import quote_plus


TABLE_NAME = "nyc_parking"
SCHEMA_NAME = "datastorage"
CSV_PATH = "data/nyc_parking_violations.csv"
DELTA_PATH = "data/delta_table"

# Vehicle Color: ~20 unique values across ~42M rows — great encoding demo
ENCODING_DEMO_COLUMN = "Vehicle_Color"
