"""
One-time script to download CTA GTFS static data and load it into DuckDB.
Run this before starting data collection:
    python scripts/load_gtfs.py
"""

import io
import os
import zipfile

import duckdb
import requests
from dotenv import load_dotenv

load_dotenv()

GTFS_URL = "https://www.transitchicago.com/downloads/sch_data/google_transit.zip"
DB_PATH = os.getenv("DB_PATH", "data/cta.duckdb")

# GTFS files we care about and the table name they map to
GTFS_FILES = {
    "stops.txt": "gtfs_stops",
    "routes.txt": "gtfs_routes",
    "trips.txt": "gtfs_trips",
    "stop_times.txt": "gtfs_stop_times",
    "calendar.txt": "gtfs_calendar",
    "calendar_dates.txt": "gtfs_calendar_dates",
}


def load_gtfs():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    print(f"Connecting to DuckDB at {DB_PATH}")
    conn = duckdb.connect(DB_PATH)

    print(f"Downloading GTFS data from {GTFS_URL} ...")
    response = requests.get(GTFS_URL, timeout=60)
    response.raise_for_status()
    print(f"Downloaded {len(response.content) / 1024:.1f} KB")

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        available = set(zf.namelist())
        for filename, table_name in GTFS_FILES.items():
            if filename not in available:
                print(f"  Skipping {filename} (not in zip)")
                continue

            print(f"  Loading {filename} → {table_name} ...")
            csv_bytes = zf.read(filename)

            # Write to a temp file so DuckDB's CSV reader can use it
            tmp_path = f"/tmp/{filename}"
            with open(tmp_path, "wb") as f:
                f.write(csv_bytes)

            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM read_csv_auto('{tmp_path}', header=true, all_varchar=true)
            """)
            count = conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
            print(f"    → {count:,} rows loaded")
            os.remove(tmp_path)

    # Create a helpful view that pre-joins stop_times with stops and routes
    # for easy delay calculation later
    conn.execute("DROP VIEW IF EXISTS gtfs_scheduled_arrivals")
    conn.execute("""
        CREATE VIEW gtfs_scheduled_arrivals AS
        SELECT
            st.trip_id,
            st.stop_id,
            st.arrival_time AS scheduled_arrival_time,
            st.departure_time AS scheduled_departure_time,
            st.stop_sequence,
            s.stop_name,
            s.stop_lat,
            s.stop_lon,
            t.route_id,
            t.direction_id,
            t.service_id
        FROM gtfs_stop_times st
        JOIN gtfs_stops s ON s.stop_id = st.stop_id
        JOIN gtfs_trips t ON t.trip_id = st.trip_id
    """)
    print("  Created gtfs_scheduled_arrivals view")

    conn.close()
    print("\nGTFS data loaded successfully.")
    print(f"Database: {DB_PATH}")


if __name__ == "__main__":
    load_gtfs()
