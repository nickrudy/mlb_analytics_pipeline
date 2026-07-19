#!/usr/bin/env python3
"""
scripts/migrate_add_obp_projection_supabase.py
--------------------------------------------------
Adds the walk-rate/on-base-percentage projection columns to
fact_matchup_batter_pitcher, mirroring the existing projected_total_bases
architecture (see ARCHITECTURE.md "Projection Model"):

    batter_vs_hand_on_base_pct           -- baseline component (batter side)
    pitcher_vs_hand_on_base_pct_allowed   -- baseline component (pitcher side)
    projected_on_base_pct                 -- regressed 30/70 baseline blend
    proj_plate_appearances_per_game       -- empirical SLOT_PA lookup
    projected_times_on_base               -- projected_on_base_pct * PA/game

Safe to re-run — uses ADD COLUMN IF NOT EXISTS.

Run:
    DB_BACKEND=supabase python scripts/migrate_add_obp_projection_supabase.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.db import execute, DB_BACKEND, ping

DDL = [
    "ALTER TABLE fact_matchup_batter_pitcher ADD COLUMN IF NOT EXISTS batter_vs_hand_on_base_pct DOUBLE PRECISION",
    "ALTER TABLE fact_matchup_batter_pitcher ADD COLUMN IF NOT EXISTS pitcher_vs_hand_on_base_pct_allowed DOUBLE PRECISION",
    "ALTER TABLE fact_matchup_batter_pitcher ADD COLUMN IF NOT EXISTS projected_on_base_pct DOUBLE PRECISION",
    "ALTER TABLE fact_matchup_batter_pitcher ADD COLUMN IF NOT EXISTS proj_plate_appearances_per_game DOUBLE PRECISION",
    "ALTER TABLE fact_matchup_batter_pitcher ADD COLUMN IF NOT EXISTS projected_times_on_base DOUBLE PRECISION",
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

    print("\n[migrate] OBP projection columns are live ✓")


if __name__ == "__main__":
    main()
