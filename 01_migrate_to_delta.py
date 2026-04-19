"""
02_migrate_to_delta.py
Read from CSV, write to a local Delta Table, print size comparison.
"""

import os
import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake
import config

CHUNKSIZE = 1_000_000

# ── CSV size ─────────────────────────────────────────────────────────────────
csv_mb = os.path.getsize(config.CSV_PATH) / (1024 ** 2)

# ── Read CSV → write Delta ───────────────────────────────────────────────────
print("Migrating CSV → Delta Table...")
row_count = 0
first = True

for chunk in pd.read_csv(config.CSV_PATH, chunksize=CHUNKSIZE, low_memory=False, dtype=str):
    chunk.columns = [c.replace(" ", "_").replace("/", "_") for c in chunk.columns]
    # Cast null-typed columns (all-NaN) to string so Delta accepts them
    table = pa.Table.from_pandas(chunk, preserve_index=False)
    schema = pa.schema([
        f.with_type(pa.string()) if f.type == pa.null() else f
        for f in table.schema
    ])
    table = table.cast(schema)
    write_deltalake(config.DELTA_PATH, table, mode="overwrite" if first else "append", schema_mode="merge")
    first = False
    row_count += len(chunk)
    print(f"  Written {row_count:,} rows", end="\r")

print(f"\nMigration complete: {row_count:,} rows")

# ── Delta on-disk size ───────────────────────────────────────────────────────
delta_mb = sum(
    os.path.getsize(os.path.join(dp, f))
    for dp, _, files in os.walk(config.DELTA_PATH)
    for f in files
) / (1024 ** 2)

ratio = csv_mb / delta_mb

print(f"""
{'='*45}
  CSV file         : {csv_mb:>10,.1f} MB
  Delta Table      : {delta_mb:>10,.1f} MB
  Compression ratio: {ratio:>10.1f}x smaller
{'='*45}
""")
