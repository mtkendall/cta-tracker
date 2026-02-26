"""
Exports dbt mart tables from local DuckDB to Parquet files in exports/.
Commit the exports/ directory to git and push — Replit reads from these files.

Workflow:
    python scripts/collect_data.py      # collect some data
    python scripts/run_dbt.py           # run dbt transforms
    python scripts/export_for_replit.py # export to Parquet
    git add exports/ && git push        # sync to Replit
"""

import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "data/cta.duckdb")
EXPORTS_DIR = Path("exports")
EXPORTS_DIR.mkdir(exist_ok=True)

TABLES = [
    "on_time_by_route_hour",
    "arrival_history",
]


def export():
    conn = duckdb.connect(DB_PATH, read_only=True)

    for table in TABLES:
        out_path = EXPORTS_DIR / f"{table}.parquet"
        conn.execute(f"COPY {table} TO '{out_path}' (FORMAT PARQUET)")
        row_count = conn.execute(f"SELECT count(*) FROM '{out_path}'").fetchone()[0]
        size_kb = out_path.stat().st_size / 1024
        print(f"  {table} → {out_path} ({row_count:,} rows, {size_kb:.1f} KB)")

    conn.close()
    print(f"\nDone. Now run:\n  git add exports/ && git commit -m 'Update data export' && git push")


if __name__ == "__main__":
    export()
