"""
Rerun earthquake–well associations against the configured database, overwriting
existing association tables and excluding production links.

Usage:
  - Ensure your DB URI is set via `EQ_DB_URI` or uses the default in
    `src/eqassoc/config.py`.
  - Run: `python rerun_associations.py`

This script calls the internal CLI with:
  --mode full         → truncates and overwrites result tables
  --types HF WD       → excludes production (PROD)
"""

from __future__ import annotations
import os, sys
import pandas as pd


def main() -> None:
    # Make `src` importable without requiring installation
    here = os.path.dirname(os.path.abspath(__file__))
    src_path = os.path.join(here, "src")
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    # Import CLI and loader after path adjustment
    from eqassoc.cli import main as eqassoc_main
    from eqassoc.loaders import load_engine

    # Build argv to run a full rebuild excluding production
    argv = [
        "eq-assoc",          # dummy program name
        "--mode", "full",   # overwrite tables
        "--types", "HF", "WD",  # omit production
    ]

    # Optionally honor verbose mode via environment variable
    if os.getenv("EQASSOC_VERBOSE", "0") not in ("0", "false", "False"):
        argv.append("--verbose")

    # Execute the CLI with our arguments
    old_argv = sys.argv
    try:
        sys.argv = argv
        eqassoc_main()
    finally:
        sys.argv = old_argv

    # After run: print quick counts from DB to confirm overwrite
    try:
        from sqlalchemy import inspect
        db_uri = os.getenv("EQ_DB_URI")
        if not db_uri:
            # Import default only if env not set (avoids cyclic import earlier)
            from eqassoc.config import DEFAULT_DB_URI
            db_uri = DEFAULT_DB_URI
        eng = load_engine(db_uri)
        insp = inspect(eng)
        for tbl in ("eq_well_association", "eq_well_association_classified"):
            if insp.has_table(tbl):
                cnt = pd.read_sql_query(f"SELECT COUNT(*) AS n FROM {tbl}", eng)["n"].iat[0]
                print(f"{tbl}: {cnt} rows")
                if cnt:
                    head = pd.read_sql_query(f"SELECT * FROM {tbl} LIMIT 1", eng)
                    print(f"Sample from {tbl}:")
                    print(head.to_string(index=False))
            else:
                print(f"{tbl}: table not found")
    except Exception as e:
        print(f"Post-run check failed: {e}")


if __name__ == "__main__":
    main()
