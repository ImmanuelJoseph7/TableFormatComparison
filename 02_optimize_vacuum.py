"""
02_optimize_vacuum.py
Compact small Parquet files (OPTIMIZE) and remove old file versions (VACUUM),
then print before/after size comparison.
"""

import os
from deltalake import DeltaTable
import config


def dir_size_mb(path):
    return sum(
        os.path.getsize(os.path.join(dp, f))
        for dp, _, files in os.walk(path)
        for f in files
    ) / (1024 ** 2)


dt = DeltaTable(config.DELTA_PATH)

before_mb = dir_size_mb(config.DELTA_PATH)
before_files = dt.get_add_actions().num_rows
print(f"Before optimize: {before_files} files, {before_mb:,.1f} MB")

print("Running OPTIMIZE (compaction)...")
dt.optimize.compact()
dt = DeltaTable(config.DELTA_PATH)  # reload after optimize

after_compact_mb = dir_size_mb(config.DELTA_PATH)
after_compact_files = dt.get_add_actions().num_rows
print(f"After optimize : {after_compact_files} files, {after_compact_mb:,.1f} MB")

print("Running VACUUM (retention=0h)...")
dt.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)

after_vacuum_mb = dir_size_mb(config.DELTA_PATH)
print(f"""
{'='*45}
  Before optimize  : {before_files:>6} files  {before_mb:>10,.1f} MB
  After optimize   : {after_compact_files:>6} files  {after_compact_mb:>10,.1f} MB
  After vacuum     :          {after_vacuum_mb:>10,.1f} MB
  Space reclaimed  :          {before_mb - after_vacuum_mb:>10,.1f} MB
{'='*45}
""")
