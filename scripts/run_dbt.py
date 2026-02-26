"""
Runs dbt transformations against the DuckDB database.
Call this after collect_data.py to update the mart models.

    python scripts/run_dbt.py
"""

import os
import subprocess
import sys
from pathlib import Path

# Run dbt from the project root so paths in profiles.yml resolve correctly
PROJECT_ROOT = Path(__file__).parent.parent


def run_dbt(*commands: str):
    # Resolve the dbt binary from the same directory as the Python executable,
    # so it works correctly inside a virtualenv without needing activation.
    dbt_bin = Path(sys.executable).parent / "dbt"
    for cmd in commands:
        args = [str(dbt_bin)] + cmd.split()
        print(f"Running: {' '.join(args)}")
        result = subprocess.run(args, cwd=PROJECT_ROOT / "dbt", check=False)
        if result.returncode != 0:
            print(f"dbt command failed: {cmd}", file=sys.stderr)
            sys.exit(result.returncode)


if __name__ == "__main__":
    run_dbt("run", "test")
