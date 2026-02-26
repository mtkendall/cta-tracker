"""
Polls CTA Train Tracker and Bus Tracker APIs and stores raw predictions in DuckDB.

Run once manually:
    python scripts/collect_data.py

Run continuously (every N seconds, set in config.yml):
    python scripts/collect_data.py --loop

On Replit, use Scheduled Deployments to call this script every 1 minute instead.
"""

import argparse
import os
import time
from datetime import datetime
from typing import Optional

import duckdb
import requests
import yaml
from dotenv import load_dotenv

load_dotenv()

TRAIN_API_BASE = "https://lapi.transitchicago.com/api/1.0"
BUS_API_BASE = "https://www.ctabustracker.com/bustime/api/v3"
DB_PATH = os.getenv("DB_PATH", "data/cta.duckdb")
TRAIN_KEY = os.getenv("CTA_TRAIN_KEY", "")
BUS_KEY = os.getenv("CTA_BUS_KEY", "")


def load_config():
    with open("config.yml") as f:
        return yaml.safe_load(f)


def get_db(db_path: str) -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = duckdb.connect(db_path)
    _ensure_tables(conn)
    return conn


def _ensure_tables(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_train_arrivals (
            run_number      VARCHAR,
            route           VARCHAR,
            stop_id         VARCHAR,
            station_id      VARCHAR,
            station_name    VARCHAR,
            stop_desc       VARCHAR,
            dest_station_id VARCHAR,
            dest_name       VARCHAR,
            direction       VARCHAR,
            predicted_arrival   TIMESTAMP,
            prediction_made_at  TIMESTAMP,
            is_delayed      BOOLEAN,
            is_scheduled    BOOLEAN,
            is_fault        BOOLEAN,
            heading         INTEGER,
            collected_at    TIMESTAMP DEFAULT now()
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_bus_predictions (
            vehicle_id      VARCHAR,
            route           VARCHAR,
            route_direction VARCHAR,
            stop_id         VARCHAR,
            stop_name       VARCHAR,
            destination     VARCHAR,
            predicted_arrival   TIMESTAMP,
            prediction_made_at  TIMESTAMP,
            is_delayed      BOOLEAN,
            prediction_type VARCHAR,   -- 'A' = arrival, 'D' = departure
            collected_at    TIMESTAMP DEFAULT now()
        )
    """)


def _parse_cta_timestamp(s: str) -> Optional[datetime]:
    """Parse CTA timestamp. Train Tracker returns ISO 8601 (2026-02-26T14:43:08),
    Bus Tracker returns 'YYYYMMDD HH:MM'. Try both."""
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y%m%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def fetch_train_arrivals(conn: duckdb.DuckDBPyConnection, config: dict):
    if not TRAIN_KEY:
        print("  [train] Skipping — CTA_TRAIN_KEY not set")
        return

    routes_filter = set(config.get("train_routes", []))
    station_ids = [str(s["id"]) for s in config.get("train_stations", [])]
    if not station_ids:
        return

    total_inserted = 0
    for station_id in station_ids:
        try:
            resp = requests.get(
                f"{TRAIN_API_BASE}/ttarrivals.aspx",
                params={
                    "key": TRAIN_KEY,
                    "mapid": station_id,
                    "outputType": "JSON",
                    "max": 20,
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  [train] Error fetching station {station_id}: {e}")
            continue

        etas = data.get("ctatt", {}).get("eta", [])
        rows = []
        for eta in etas:
            route = eta.get("rt", "")
            if routes_filter and route not in routes_filter:
                continue
            rows.append((
                eta.get("rn"),
                route,
                eta.get("stpId"),
                eta.get("staId"),
                eta.get("staNm"),
                eta.get("stpDe"),
                eta.get("destSt"),
                eta.get("destNm"),
                eta.get("trDr"),
                _parse_cta_timestamp(eta.get("arrT")),
                _parse_cta_timestamp(eta.get("prdt")),
                eta.get("isDly") == "1",
                eta.get("isSch") == "1",
                eta.get("isFlt") == "1",
                int(eta.get("heading", 0) or 0),
            ))

        if rows:
            conn.executemany("""
                INSERT INTO raw_train_arrivals (
                    run_number, route, stop_id, station_id, station_name,
                    stop_desc, dest_station_id, dest_name, direction,
                    predicted_arrival, prediction_made_at,
                    is_delayed, is_scheduled, is_fault, heading
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            total_inserted += len(rows)

    print(f"  [train] Inserted {total_inserted} arrival predictions")


def fetch_bus_predictions(conn: duckdb.DuckDBPyConnection, config: dict):
    if not BUS_KEY:
        print("  [bus] Skipping — CTA_BUS_KEY not set")
        return

    stop_ids = [str(s["id"]) for s in config.get("bus_stops", [])]
    if not stop_ids:
        return

    # Bus API accepts up to 10 stops per request
    total_inserted = 0
    for i in range(0, len(stop_ids), 10):
        batch = stop_ids[i : i + 10]
        try:
            resp = requests.get(
                f"{BUS_API_BASE}/getpredictions",
                params={
                    "key": BUS_KEY,
                    "stpid": ",".join(batch),
                    "format": "json",
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  [bus] Error fetching stops {batch}: {e}")
            continue

        predictions = data.get("bustime-response", {}).get("prd", [])
        rows = []
        for prd in predictions:
            rows.append((
                prd.get("vid"),
                prd.get("rt"),
                prd.get("rtdir"),
                prd.get("stpid"),
                prd.get("stpnm"),
                prd.get("des"),
                _parse_cta_timestamp(prd.get("prdtm")),
                _parse_cta_timestamp(prd.get("tmstmp")),
                prd.get("dly", False),
                prd.get("typ", "A"),
            ))

        if rows:
            conn.executemany("""
                INSERT INTO raw_bus_predictions (
                    vehicle_id, route, route_direction, stop_id, stop_name,
                    destination, predicted_arrival, prediction_made_at,
                    is_delayed, prediction_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            total_inserted += len(rows)

    print(f"  [bus] Inserted {total_inserted} bus predictions")


def collect_once():
    config = load_config()
    conn = get_db(DB_PATH)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Collecting data at {now}")
    fetch_train_arrivals(conn, config)
    fetch_bus_predictions(conn, config)
    conn.close()


def collect_loop(interval_seconds: int):
    print(f"Starting collection loop every {interval_seconds}s. Ctrl+C to stop.")
    while True:
        try:
            collect_once()
        except Exception as e:
            print(f"Error during collection: {e}")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect CTA transit data")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously on the interval set in config.yml",
    )
    args = parser.parse_args()

    if args.loop:
        cfg = load_config()
        interval = cfg.get("poll_interval_seconds", 60)
        collect_loop(interval)
    else:
        collect_once()
