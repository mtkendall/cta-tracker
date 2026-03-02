"""
Fly.io background worker for CTA data collection and export.

Two scheduled jobs:
  - collect:      every 60s  — polls CTA Train/Bus Tracker APIs → DuckDB
  - dbt_and_push: every 2h   — runs dbt, exports parquet, pushes to GitHub

Required environment variables (set as Fly secrets):
  CTA_TRAIN_KEY   Train Tracker API key
  CTA_BUS_KEY     Bus Tracker API key
  GH_TOKEN        GitHub PAT with Contents: Read+Write on this repo

Optional:
  DB_PATH         Path to DuckDB file (default: data/cta.duckdb)
  GH_REPO         GitHub repo slug (default: mtkendall/cta-tracker)
  GH_BRANCH       Branch to push to (default: main)
  EXPORT_INTERVAL_HOURS  Hours between dbt+export runs (default: 2)
"""

import logging
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent.parent
GH_REPO = os.getenv("GH_REPO", "mtkendall/cta-tracker")
GH_BRANCH = os.getenv("GH_BRANCH", "main")
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


def job_dbt_and_push():
    """Run dbt, export parquet files, and push to GitHub."""
    log.info("=== Starting dbt + export + push ===")

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

    # 2. Export parquet files (reuse export_for_replit logic)
    log.info("Exporting parquet files")
    sys.path.insert(0, str(PROJECT_ROOT))
    from scripts.export_for_replit import export
    try:
        export()
    except Exception as e:
        log.error("Export failed: %s", e)
        return

    # 3. Push exports/ to GitHub via shallow clone
    token = os.environ.get("GH_TOKEN")
    if not token:
        log.warning("GH_TOKEN not set — skipping git push")
        return

    remote = f"https://x-access-token:{token}@github.com/{GH_REPO}.git"
    exports_src = PROJECT_ROOT / "exports"

    log.info("Pushing exports to GitHub (%s)", GH_REPO)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)

            subprocess.run(
                ["git", "clone", "--depth=1", remote, tmp],
                check=True, capture_output=True,
            )

            # Copy parquet files into the clone
            for f in exports_src.glob("*.parquet"):
                shutil.copy(f, tmp_path / "exports" / f.name)

            subprocess.run(["git", "-C", tmp, "config", "user.email", "collector@fly.io"], check=True)
            subprocess.run(["git", "-C", tmp, "config", "user.name", "CTA Collector"], check=True)
            subprocess.run(["git", "-C", tmp, "add", "exports/"], check=True)

            result = subprocess.run(
                ["git", "-C", tmp, "commit", "-m",
                 f"Update data export [{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC]"],
            )

            if result.returncode == 0:
                subprocess.run(
                    ["git", "-C", tmp, "push", remote, GH_BRANCH],
                    check=True, capture_output=True,
                )
                log.info("Pushed exports to GitHub successfully")
            else:
                log.info("No changes in exports — nothing to push")

    except subprocess.CalledProcessError as e:
        log.error("Git push failed: %s", e)


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Validate required secrets are present
    missing = [k for k in ("CTA_TRAIN_KEY", "CTA_BUS_KEY") if not os.environ.get(k)]
    if missing:
        log.warning("Missing env vars: %s — some data sources will be skipped", missing)

    log.info("Starting CTA collector (collect every 60s, export every %dh)", EXPORT_INTERVAL_HOURS)

    # Load poll interval from config
    try:
        import yaml
        with open(PROJECT_ROOT / "config.yml") as f:
            cfg = yaml.safe_load(f)
        poll_interval = cfg.get("poll_interval_seconds", 60)
    except Exception:
        poll_interval = 60

    scheduler = BlockingScheduler()

    scheduler.add_job(
        job_collect,
        IntervalTrigger(seconds=poll_interval),
        id="collect",
        next_run_time=datetime.now(),   # run immediately on startup
    )

    scheduler.add_job(
        job_dbt_and_push,
        IntervalTrigger(hours=EXPORT_INTERVAL_HOURS),
        id="dbt_and_push",
        next_run_time=datetime.now(),   # run immediately on startup
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")
