#!/bin/bash
# Run dbt, export parquet files, and push to GitHub.
# Called by the com.cta-tracker.dbt-export launchd agent every 2 hours.

set -euo pipefail

REPO="/Users/mattkendall/projects/playground/cta-tracker"
PYTHON="$REPO/.venv/bin/python3"

cd "$REPO"

echo "=== $(date) starting dbt + export + push ==="

"$PYTHON" scripts/run_dbt.py

"$PYTHON" scripts/export_for_replit.py

git add exports/
if git diff --cached --quiet; then
    echo "No changes in exports/ — skipping commit"
else
    git commit -m "Update data export [$(date -u '+%Y-%m-%d %H:%M') UTC]"
    git push
fi

echo "=== Done ==="
