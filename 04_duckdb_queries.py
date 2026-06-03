"""
04_duckdb_queries.py
Query the Delta Table with DuckDB — no Spark required.
Runs four queries and prints results + timing for each.
"""

import time
import duckdb
import config

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

DELTA = f"delta_scan('{config.DELTA_PATH}')"
CSV   = f"read_csv_auto('{config.CSV_PATH}', ignore_errors=true)"


def run(label, sql):
    print(f"\n{'='*55}")
    print(f"  {label}")
    print(f"{'='*55}")
    t0 = time.perf_counter()
    result = con.execute(sql).fetchdf()
    elapsed = time.perf_counter() - t0
    print(result.to_string(index=False))
    print(f"\n  Time: {elapsed:.2f}s")


# 0. Sanity check — first 5 rows (select readable columns)
run(
    "First 5 rows (Delta Table)",
    f"""
    SELECT Plate_ID, Registration_State, Issue_Date, Violation_Description, Vehicle_Color, Vehicle_Make
    FROM {DELTA}
    LIMIT 5
    """,
)

# 1. Top 10 violation types — Delta Table
run(
    "Top 10 violation types (Delta Table)",
    f"""
    SELECT Violation_Description, COUNT(*) AS tickets
    FROM {DELTA}
    GROUP BY Violation_Description
    ORDER BY tickets DESC
    LIMIT 10
    """,
)

# 2. Same query — raw CSV (for comparison)
# CSV retains original column names with spaces; Delta Table has underscores
run(
    "Top 10 violation types (CSV)",
    f"""
    SELECT "Violation Description", COUNT(*) AS tickets
    FROM {CSV}
    GROUP BY "Violation Description"
    ORDER BY tickets DESC
    LIMIT 10
    """,
)

# 3. Predicate pushdown — filter by year
# DuckDB uses _delta_log min/max stats to skip files that can't match
run(
    "Tickets in 2014 — predicate pushdown (Delta Table)",
    f"""
    SELECT COUNT(*) AS tickets_2014
    FROM {DELTA}
    WHERE SPLIT_PART(Issue_Date, '/', 3) = '2014'
    """,
)

# 4. Tickets by year — full table aggregation
run(
    "Tickets by year (Delta Table)",
    f"""
    SELECT
        SPLIT_PART(Issue_Date, '/', 3) AS year,
        COUNT(*) AS tickets
    FROM {DELTA}
    WHERE SPLIT_PART(Issue_Date, '/', 3) BETWEEN '2013' AND '2017'
    GROUP BY year
    ORDER BY year
    """,
)
