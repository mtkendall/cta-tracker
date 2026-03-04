"""
Background worker for CTA data collection and export.

Two scheduled jobs:
  - collect:          every 60s  — polls CTA Train/Bus Tracker APIs → DuckDB
  - dbt_and_upload:   every 2h   — runs dbt, exports parquet, uploads to GCS

Required environment variables:
  CTA_TRAIN_KEY   Train Tracker API key
  CTA_BUS_KEY     Bus Tracker API key
  GCS_BUCKET      GCS bucket name (e.g. cta-tracker-data)

Optional:
  DB_PATH                Path to DuckDB file (default: data/cta.duckdb)
  EXPORT_INTERVAL_HOURS  Hours between dbt+export runs (default: 2)
  GCS_EXPORTS_PREFIX     GCS path prefix for mart parquet files (default: exports)
  GCS_RAW_PREFIX         GCS path prefix for raw archive parquet (default: raw)
  RAW_RETENTION_DAYS     Days of raw data to keep in DuckDB; older rows are
                         archived to GCS then deleted (default: 7)
"""

import logging
import os
import subprocess
import sys
import tempfile
import threading
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = os.getenv("DB_PATH", str(PROJECT_ROOT / "data" / "cta.duckdb"))
GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_EXPORTS_PREFIX = os.getenv("GCS_EXPORTS_PREFIX", "exports")
GCS_RAW_PREFIX = os.getenv("GCS_RAW_PREFIX", "raw")
EXPORT_INTERVAL_HOURS = int(os.getenv("EXPORT_INTERVAL_HOURS", "2"))
RAW_RETENTION_DAYS = int(os.getenv("RAW_RETENTION_DAYS", "7"))
DBT_BIN = Path(sys.executable).parent / "dbt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Shared lock so collection and dbt never open DuckDB simultaneously
_db_lock = threading.Lock()

# ── Jobs ──────────────────────────────────────────────────────────────────────

def job_collect():
    """Poll CTA APIs and insert raw predictions into DuckDB."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from scripts.collect_data import collect_once
    with _db_lock:
        try:
            collect_once()
        except Exception as e:
            log.error("Collection failed: %s", e)


def upload_exports_to_gcs():
    """Upload exports/*.parquet to GCS_BUCKET/GCS_EXPORTS_PREFIX/."""
    if not GCS_BUCKET:
        log.warning("GCS_BUCKET not set — skipping upload")
        return

    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    exports_dir = PROJECT_ROOT / "exports"

    for parquet_file in exports_dir.glob("*.parquet"):
        blob_name = f"{GCS_EXPORTS_PREFIX}/{parquet_file.name}"
        bucket.blob(blob_name).upload_from_filename(str(parquet_file))
        log.info("Uploaded %s → gs://%s/%s", parquet_file.name, GCS_BUCKET, blob_name)


def archive_and_trim_raw_tables():
    """
    Export raw rows older than RAW_RETENTION_DAYS to GCS as date-partitioned parquet,
    then delete them from DuckDB to keep the local database bounded.

    GCS layout: GCS_RAW_PREFIX/<table>/collected_date=YYYY-MM-DD/data-N.parquet
    """
    if not GCS_BUCKET:
        log.warning("GCS_BUCKET not set — skipping raw archive")
        return

    cutoff = datetime.now() - timedelta(days=RAW_RETENTION_DAYS)
    raw_tables = ("raw_train_arrivals", "raw_bus_predictions")
    tables_to_upload: list[tuple[str, Path]] = []  # (table_name, local_dir)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        # Phase 1: export old rows to temp dir (hold lock so collector can't write)
        with _db_lock:
            conn = duckdb.connect(DB_PATH)
            try:
                for table in raw_tables:
                    count = conn.execute(
                        f"SELECT count(*) FROM {table} WHERE collected_at < ?", [cutoff]
                    ).fetchone()[0]
                    if count == 0:
                        log.info("No rows to archive in %s", table)
                        continue
                    log.info("Exporting %d rows from %s (before %s)...", count, table, cutoff.date())
                    out_dir = tmp_path / table
                    out_dir.mkdir()
                    conn.execute(f"""
                        COPY (
                            SELECT *, collected_at::date AS collected_date
                            FROM {table}
                            WHERE collected_at < ?
                        ) TO '{out_dir}' (FORMAT PARQUET, PARTITION_BY (collected_date))
                    """, [cutoff])
                    tables_to_upload.append((table, out_dir))
            finally:
                conn.close()

        if not tables_to_upload:
            log.info("Nothing to archive — DuckDB is within the %d-day retention window", RAW_RETENTION_DAYS)
            return

        # Phase 2: upload to GCS (no DuckDB access, lock not held)
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)

        successfully_uploaded: set[str] = set()
        for table, out_dir in tables_to_upload:
            try:
                for parquet_file in out_dir.rglob("*.parquet"):
                    relative = parquet_file.relative_to(tmp_path)
                    blob_name = f"{GCS_RAW_PREFIX}/{relative}"
                    bucket.blob(blob_name).upload_from_filename(str(parquet_file))
                    log.info("Archived → gs://%s/%s", GCS_BUCKET, blob_name)
                successfully_uploaded.add(table)
            except Exception as e:
                log.error("Failed to upload %s to GCS: %s — skipping trim for this table", table, e)

        # Phase 3: delete archived rows + checkpoint (hold lock again)
        if not successfully_uploaded:
            log.error("No tables successfully uploaded — DuckDB not trimmed")
            return

        with _db_lock:
            conn = duckdb.connect(DB_PATH)
            try:
                for table in successfully_uploaded:
                    conn.execute(f"DELETE FROM {table} WHERE collected_at < ?", [cutoff])
                    log.info("Trimmed old rows from %s", table)
                conn.execute("CHECKPOINT")
                log.info(
                    "DuckDB trimmed. Retaining last %d days of raw data.", RAW_RETENTION_DAYS
                )
            finally:
                conn.close()


def job_dbt_and_upload():
    """Run dbt, export parquet files, and upload to GCS."""
    log.info("=== Starting dbt + export + upload ===")

    # 1. Run dbt and export parquet (hold lock so collector can't write concurrently)
    with _db_lock:
        for cmd in ["run", "test"]:
            log.info("Running: dbt %s", cmd)
            result = subprocess.run(
                [str(DBT_BIN), cmd],
                cwd=PROJECT_ROOT / "dbt",
            )
            if result.returncode != 0:
                log.error("dbt %s failed (exit %d) — skipping export", cmd, result.returncode)
                return

        log.info("Exporting parquet files")
        sys.path.insert(0, str(PROJECT_ROOT))
        from scripts.export_parquet import export
        try:
            export()
        except Exception as e:
            log.error("Export failed: %s", e)
            return

    # 2. Upload to GCS (no DuckDB access, lock not needed)
    try:
        upload_exports_to_gcs()
    except Exception as e:
        log.error("GCS upload failed: %s", e)
        return

    # 3. Archive raw data older than RAW_RETENTION_DAYS to GCS, then trim DuckDB
    try:
        archive_and_trim_raw_tables()
    except Exception as e:
        log.error("Raw archive/trim failed: %s", e)


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Validate required secrets are present
    missing = [k for k in ("CTA_TRAIN_KEY", "CTA_BUS_KEY") if not os.environ.get(k)]
    if missing:
        log.warning("Missing env vars: %s — some data sources will be skipped", missing)

    # Load poll interval from config
    try:
        import yaml
        with open(PROJECT_ROOT / "config.yml") as f:
            cfg = yaml.safe_load(f)
        poll_interval = cfg.get("poll_interval_seconds", 60)
    except Exception:
        poll_interval = 60

    if not GCS_BUCKET:
        log.warning("GCS_BUCKET not set — exports will not be uploaded")

    log.info("Starting CTA collector (collect every %ds, export every %dh)", poll_interval, EXPORT_INTERVAL_HOURS)

    scheduler = BlockingScheduler()

    scheduler.add_job(
        job_collect,
        IntervalTrigger(seconds=poll_interval),
        id="collect",
        next_run_time=datetime.now(),   # run immediately on startup
    )

    scheduler.add_job(
        job_dbt_and_upload,
        IntervalTrigger(hours=EXPORT_INTERVAL_HOURS),
        id="dbt_and_upload",
        next_run_time=datetime.now(),   # run immediately on startup
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")
