#!/usr/bin/env python3
"""
scripts/migrate_add_recency_diff_to_matchups_supabase.py
------------------------------------------------------------
Adds recency_raw_diff to fact_matchup_batter_pitcher in Supabase --
persists the raw (unshrunk) recency signal alongside the already-applied
projected_total_bases, so dashboards/local tools can show "why did this
move" as a companion column to the adjusted projection (matches the
originally validated local design). Display/observability only -- the
projected_total_bases adjustment itself does not depend on this column;
see scripts/migrate_add_recency_signal_supabase.py for that piece.

Safe to re-run — uses ADD COLUMN IF NOT EXISTS.

Run:
    DB_BACKEND=supabase python scripts/migrate_add_recency_diff_to_matchups_supabase.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.db import execute, DB_BACKEND, ping

DDL = [
    "ALTER TABLE fact_matchup_batter_pitcher ADD COLUMN IF NOT EXISTS recency_raw_diff DOUBLE PRECISION",
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

    print("\n[migrate] fact_matchup_batter_pitcher.recency_raw_diff is live ✓")


if __name__ == "__main__":
    main()
