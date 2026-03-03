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
  GCS_EXPORTS_PREFIX     GCS path prefix for parquet files (default: exports)
"""

import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent.parent
GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_EXPORTS_PREFIX = os.getenv("GCS_EXPORTS_PREFIX", "exports")
EXPORT_INTERVAL_HOURS = int(os.getenv("EXPORT_INTERVAL_HOURS", "2"))
DBT_BIN = Path(sys.executable).parent / "dbt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Jobs ──────────────────────────────────────────────────────────────────────

def job_collect():
    """Poll CTA APIs and insert raw predictions into DuckDB."""
    # Import here so module-level env vars are already loaded
    sys.path.insert(0, str(PROJECT_ROOT))
    from scripts.collect_data import collect_once
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


def job_dbt_and_upload():
    """Run dbt, export parquet files, and upload to GCS."""
    log.info("=== Starting dbt + export + upload ===")

    # 1. Run dbt
    for cmd in ["run", "test"]:
        log.info("Running: dbt %s", cmd)
        result = subprocess.run(
            [str(DBT_BIN), cmd],
            cwd=PROJECT_ROOT / "dbt",
        )
        if result.returncode != 0:
            log.error("dbt %s failed (exit %d) — skipping export", cmd, result.returncode)
            return

    # 2. Export parquet files
    log.info("Exporting parquet files")
    sys.path.insert(0, str(PROJECT_ROOT))
    from scripts.export_for_replit import export
    try:
        export()
    except Exception as e:
        log.error("Export failed: %s", e)
        return

    # 3. Upload to GCS
    try:
        upload_exports_to_gcs()
    except Exception as e:
        log.error("GCS upload failed: %s", e)


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
