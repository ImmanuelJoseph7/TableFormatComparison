"""
03_encoding_demo.py
Isolate one low-cardinality column and show exactly how dictionary encoding
compresses it — with byte-level numbers suitable for a Substack screenshot.
"""

import os
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable
import config

# ── Load the column from the Delta Table ────────────────────────────────────
dt = DeltaTable(config.DELTA_PATH)
col = config.ENCODING_DEMO_COLUMN
arrow_table = dt.to_pyarrow_table(columns=[col])
array = arrow_table.column(col)

total_rows = len(array)
unique_vals = array.unique().to_pylist()
unique_count = len(unique_vals)

# ── Raw (unencoded) size: every value stored as a full string ────────────────
raw_bytes = sum(len(str(v).encode()) for v in array.to_pylist())

# ── Dictionary-encoded size ──────────────────────────────────────────────────
# Dictionary: unique string values
dict_bytes = sum(len(str(v).encode()) for v in unique_vals)
# Indices: 1 byte per row is enough when unique_count <= 256, else 2 bytes
index_bytes_per_row = 1 if unique_count <= 256 else 2
index_bytes = total_rows * index_bytes_per_row
encoded_bytes = dict_bytes + index_bytes

# ── Write the column to CSV and Parquet for direct size comparison ────────────
csv_path = "data/encoding_demo.csv"
parquet_path = "data/encoding_demo.parquet"

arrow_table.to_pandas().to_csv(csv_path, index=False)
pq.write_table(arrow_table, parquet_path, compression="snappy", use_dictionary=True)

csv_bytes = os.path.getsize(csv_path)
parquet_bytes = os.path.getsize(parquet_path)

ratio_vs_raw   = raw_bytes / encoded_bytes
ratio_csv_vs_parquet = csv_bytes / parquet_bytes

print(f"""
{'='*55}
  DICTIONARY ENCODING DEMO  —  column: '{col}'
{'='*55}
  Total rows          : {total_rows:>12,}
  Unique values       : {unique_count:>12,}

  CSV file (one value per row, plain text)
    = {csv_bytes / 1024**2:,.1f} MB

  Dictionary-encoded (logical)
    Dictionary : {dict_bytes:,} bytes  ({unique_count} strings, stored once)
    Indices    : {index_bytes / 1024**2:,.1f} MB  ({index_bytes_per_row} byte/row × {total_rows:,} rows)
    Total      : {encoded_bytes / 1024**2:,.1f} MB
    Saving     : {raw_bytes / encoded_bytes:.0f}x smaller than raw strings

  Parquet file on disk (dict + Snappy)
    = {parquet_bytes / 1024**2:,.2f} MB
    Saving     : {ratio_csv_vs_parquet:.0f}x smaller than CSV

  WHY IT WORKS:
    Instead of repeating "{unique_vals[0] if unique_vals else 'value'}" {total_rows:,} times,
    Parquet stores the string ONCE in a dictionary and writes
    a {index_bytes_per_row}-byte integer index per row.
    {unique_count} unique values → {index_bytes_per_row}-byte index is sufficient.
{'='*55}
""")
