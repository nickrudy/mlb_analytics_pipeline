#!/usr/bin/env python3
"""
scripts/migrate_add_obp_to_daily_top_batting_supabase.py
------------------------------------------------------------
Adds projected_on_base_pct / proj_plate_appearances_per_game /
projected_times_on_base to daily_top_batting -- the actual Looker-facing
table populated by export_to_daily_tables.py. Separate from
migrate_add_obp_projection_supabase.py, which covers the source table
(fact_matchup_batter_pitcher); this one covers the export destination.

Safe to re-run — uses ADD COLUMN IF NOT EXISTS.

Run:
    DB_BACKEND=supabase python scripts/migrate_add_obp_to_daily_top_batting_supabase.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.db import execute, DB_BACKEND, ping

DDL = [
    "ALTER TABLE daily_top_batting ADD COLUMN IF NOT EXISTS projected_on_base_pct DOUBLE PRECISION",
    "ALTER TABLE daily_top_batting ADD COLUMN IF NOT EXISTS proj_plate_appearances_per_game DOUBLE PRECISION",
    "ALTER TABLE daily_top_batting ADD COLUMN IF NOT EXISTS projected_times_on_base DOUBLE PRECISION",
]


def main():
    if DB_BACKEND != "supabase":
        print(f"[ERROR] DB_BACKEND={DB_BACKEND!r} — set DB_BACKEND=supabase first.")
        sys.exit(1)

    print("[migrate] Connecting to Supabase...")
    if not ping():
        print("[migrate] Cannot reach Supabase.")
        sys.exit(1)

    for stmt in DDL:
        try:
            execute(stmt)
            print(f"  ✓ {stmt}")
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
            raise

    print("\n[migrate] daily_top_batting OBP columns are live ✓")


if __name__ == "__main__":
    main()
