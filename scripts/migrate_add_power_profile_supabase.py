#!/usr/bin/env python3
"""
scripts/migrate_add_power_profile_supabase.py
----------------------------------------------
Adds hc_x / hc_y columns to stg_statcast_pitches and creates
fact_batter_power_profile and fact_pitcher_hr_vulnerability in Supabase.

Safe to re-run — uses IF NOT EXISTS / IF NOT EXISTS for all DDL.

Run:
    DB_BACKEND=supabase python scripts/migrate_add_power_profile_supabase.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.db import execute, DB_BACKEND, ping

DDL = [
    # ── stg_statcast_pitches — add hit coordinate columns ─────────────────
    # PostgreSQL ALTER TABLE ... ADD COLUMN IF NOT EXISTS (PG 9.6+, safe to re-run)
    "ALTER TABLE stg_statcast_pitches ADD COLUMN IF NOT EXISTS hc_x DOUBLE PRECISION",
    "ALTER TABLE stg_statcast_pitches ADD COLUMN IF NOT EXISTS hc_y DOUBLE PRECISION",

    # ── fact_batter_power_profile ─────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fact_batter_power_profile (
        as_of_date              TEXT            NOT NULL,
        player_id               INTEGER         NOT NULL,
        season                  INTEGER         NOT NULL,
        window_code             TEXT            NOT NULL,
        batted_ball_events      INTEGER,
        plate_appearances       INTEGER,
        at_bats                 INTEGER,
        barrels                 INTEGER,
        barrels_per_pa          DOUBLE PRECISION,
        barrels_per_bbe         DOUBLE PRECISION,
        hard_hit_count          INTEGER,
        hard_hit_rate           DOUBLE PRECISION,
        avg_exit_velocity       DOUBLE PRECISION,
        max_exit_velocity       DOUBLE PRECISION,
        avg_launch_angle        DOUBLE PRECISION,
        xba                     DOUBLE PRECISION,
        xwoba                   DOUBLE PRECISION,
        home_runs               INTEGER,
        hr_per_pa               DOUBLE PRECISION,
        hr_per_bbe              DOUBLE PRECISION,
        fly_ball_rate           DOUBLE PRECISION,
        ground_ball_rate        DOUBLE PRECISION,
        line_drive_rate         DOUBLE PRECISION,
        pull_rate               DOUBLE PRECISION,
        oppo_rate               DOUBLE PRECISION,
        barrels_per_pa_vs_rhp   DOUBLE PRECISION,
        barrels_per_pa_vs_lhp   DOUBLE PRECISION,
        hard_hit_rate_vs_rhp    DOUBLE PRECISION,
        hard_hit_rate_vs_lhp    DOUBLE PRECISION,
        avg_ev_vs_rhp           DOUBLE PRECISION,
        avg_ev_vs_lhp           DOUBLE PRECISION,
        PRIMARY KEY (as_of_date, player_id, season, window_code),
        FOREIGN KEY (player_id)   REFERENCES dim_players(player_id),
        FOREIGN KEY (window_code) REFERENCES dim_split_windows(window_code)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_power_profile_player ON fact_batter_power_profile(as_of_date, player_id, window_code)",

    # ── fact_pitcher_hr_vulnerability ─────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fact_pitcher_hr_vulnerability (
        as_of_date                  TEXT            NOT NULL,
        pitcher_id                  INTEGER         NOT NULL,
        season                      INTEGER         NOT NULL,
        split_hand                  TEXT            NOT NULL,
        window_code                 TEXT            NOT NULL,
        batted_ball_events          INTEGER,
        batters_faced               INTEGER,
        barrels_allowed             INTEGER,
        barrel_rate_allowed         DOUBLE PRECISION,
        hard_hit_rate_allowed       DOUBLE PRECISION,
        avg_exit_velocity_allowed   DOUBLE PRECISION,
        max_exit_velocity_allowed   DOUBLE PRECISION,
        home_runs_allowed           INTEGER,
        hr_per_bbe_allowed          DOUBLE PRECISION,
        hr_per_bf_allowed           DOUBLE PRECISION,
        xwoba_allowed               DOUBLE PRECISION,
        fly_ball_rate_allowed       DOUBLE PRECISION,
        ground_ball_rate_allowed    DOUBLE PRECISION,
        line_drive_rate_allowed     DOUBLE PRECISION,
        barrel_rate_on_fastballs    DOUBLE PRECISION,
        barrel_rate_on_breaking     DOUBLE PRECISION,
        barrel_rate_on_offspeed     DOUBLE PRECISION,
        hr_rate_on_fastballs        DOUBLE PRECISION,
        hr_rate_on_breaking         DOUBLE PRECISION,
        hr_rate_on_offspeed         DOUBLE PRECISION,
        PRIMARY KEY (as_of_date, pitcher_id, season, split_hand, window_code),
        FOREIGN KEY (pitcher_id)  REFERENCES dim_players(player_id),
        FOREIGN KEY (window_code) REFERENCES dim_split_windows(window_code)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_pitcher_hr_vuln_pitcher ON fact_pitcher_hr_vulnerability(as_of_date, pitcher_id, split_hand, window_code)",
]


def main():
    if DB_BACKEND != "supabase":
        print(f"[ERROR] DB_BACKEND={DB_BACKEND!r} — set DB_BACKEND=supabase first.")
        sys.exit(1)

    print("[migrate] Connecting to Supabase...")
    if not ping():
        print("[migrate] Cannot reach Supabase.")
        sys.exit(1)

    print(f"[migrate] Running {len(DDL)} DDL statements...")
    for stmt in DDL:
        stmt = stmt.strip()
        label = stmt.split("\n")[0][:60]
        try:
            execute(stmt)
            print(f"  ✓ {label}")
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
            raise

    print("\n[migrate] Power profile migration complete ✓")
    print("         fact_batter_power_profile and fact_pitcher_hr_vulnerability are live.")


if __name__ == "__main__":
    main()
