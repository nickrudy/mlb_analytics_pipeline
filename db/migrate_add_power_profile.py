"""
migrate_add_power_profile.py
-----------------------------
Adds hc_x / hc_y columns to stg_statcast_pitches, and creates the
fact_batter_power_profile and fact_pitcher_hr_vulnerability tables.

Safe to run multiple times — column additions use try/except to skip
duplicates; table creation uses CREATE TABLE IF NOT EXISTS.

Run this BEFORE re-running transform_splits.py or ingest_statcast.py.

Usage:
    python db/migrate_add_power_profile.py --db-path data/mlb_pregame.db
"""

import sqlite3
import logging
import argparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def add_column_safe(conn: sqlite3.Connection, table: str,
                    column: str, col_type: str) -> bool:
    """
    Add a column to a table if it doesn't already exist.
    Returns True if added, False if already existed.
    """
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        log.info("Added column: %s.%s (%s)", table, column, col_type)
        return True
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            log.info("Column already exists (skipped): %s.%s", table, column)
            return False
        raise


def run_migration(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")
    conn.execute("PRAGMA journal_mode=WAL;")

    log.info("Running power profile migration on %s", db_path)

    # ── stg_statcast_pitches — hit coordinates ─────────────────────────────
    # hc_x / hc_y are present in pybaseball's statcast() DataFrame but were
    # not previously stored. Required for pull rate and oppo rate in the
    # batter power profile. ingest_statcast.py now captures these columns.
    log.info("--- stg_statcast_pitches ---")
    add_column_safe(conn, "stg_statcast_pitches", "hc_x", "REAL")
    add_column_safe(conn, "stg_statcast_pitches", "hc_y", "REAL")

    conn.commit()

    # ── fact_batter_power_profile — new table ──────────────────────────────
    log.info("--- fact_batter_power_profile ---")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_batter_power_profile (
            as_of_date              TEXT    NOT NULL,
            player_id               INTEGER NOT NULL,
            season                  INTEGER NOT NULL,
            window_code             TEXT    NOT NULL,

            -- Volume / sample denominators
            batted_ball_events      INTEGER,
            plate_appearances       INTEGER,
            at_bats                 INTEGER,

            -- Barrel metrics (corrected expanding-angle Statcast definition)
            barrels                 INTEGER,
            barrels_per_pa          REAL,
            barrels_per_bbe         REAL,

            -- Hard contact (EV >= 95 mph)
            hard_hit_count          INTEGER,
            hard_hit_rate           REAL,

            -- Exit velocity
            avg_exit_velocity       REAL,
            max_exit_velocity       REAL,

            -- Launch angle
            avg_launch_angle        REAL,

            -- Expected stats
            xba                     REAL,
            xwoba                   REAL,

            -- HR actuals
            home_runs               INTEGER,
            hr_per_pa               REAL,
            hr_per_bbe              REAL,

            -- Batted ball type profile (derived from launch_angle bands)
            fly_ball_rate           REAL,   -- LA >= 25
            ground_ball_rate        REAL,   -- LA < 10
            line_drive_rate         REAL,   -- 10 <= LA < 25

            -- Pull tendency (NULL until hc_x backfilled by re-running ingest)
            pull_rate               REAL,
            oppo_rate               REAL,

            -- Handedness splits (pre-aggregated for matchup convenience)
            barrels_per_pa_vs_rhp   REAL,
            barrels_per_pa_vs_lhp   REAL,
            hard_hit_rate_vs_rhp    REAL,
            hard_hit_rate_vs_lhp    REAL,
            avg_ev_vs_rhp           REAL,
            avg_ev_vs_lhp           REAL,

            PRIMARY KEY (as_of_date, player_id, season, window_code),
            FOREIGN KEY (player_id)   REFERENCES dim_players(player_id),
            FOREIGN KEY (window_code) REFERENCES dim_split_windows(window_code)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_power_profile_player
            ON fact_batter_power_profile(as_of_date, player_id, window_code)
    """)
    log.info("Table ready: fact_batter_power_profile")

    # ── fact_pitcher_hr_vulnerability — new table ──────────────────────────
    log.info("--- fact_pitcher_hr_vulnerability ---")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_pitcher_hr_vulnerability (
            as_of_date                  TEXT    NOT NULL,
            pitcher_id                  INTEGER NOT NULL,
            season                      INTEGER NOT NULL,
            split_hand                  TEXT    NOT NULL,   -- batter hand: R or L
            window_code                 TEXT    NOT NULL,

            -- Volume
            batted_ball_events          INTEGER,
            batters_faced               INTEGER,

            -- Power allowed
            barrels_allowed             INTEGER,
            barrel_rate_allowed         REAL,
            hard_hit_rate_allowed       REAL,
            avg_exit_velocity_allowed   REAL,
            max_exit_velocity_allowed   REAL,

            -- HR allowed
            home_runs_allowed           INTEGER,
            hr_per_bbe_allowed          REAL,
            hr_per_bf_allowed           REAL,

            -- Expected stats allowed
            xwoba_allowed               REAL,

            -- Batted ball type profile allowed
            fly_ball_rate_allowed       REAL,
            ground_ball_rate_allowed    REAL,
            line_drive_rate_allowed     REAL,

            -- HR / barrel vulnerability by pitch group
            -- denominator is BBE against that pitch group
            barrel_rate_on_fastballs    REAL,   -- FF, SI, FC
            barrel_rate_on_breaking     REAL,   -- SL, ST, CU, KC, CS
            barrel_rate_on_offspeed     REAL,   -- CH, FS, SV, FO
            hr_rate_on_fastballs        REAL,
            hr_rate_on_breaking         REAL,
            hr_rate_on_offspeed         REAL,

            PRIMARY KEY (as_of_date, pitcher_id, season, split_hand, window_code),
            FOREIGN KEY (pitcher_id)  REFERENCES dim_players(player_id),
            FOREIGN KEY (window_code) REFERENCES dim_split_windows(window_code)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_pitcher_hr_vuln_pitcher
            ON fact_pitcher_hr_vulnerability(as_of_date, pitcher_id, split_hand, window_code)
    """)
    log.info("Table ready: fact_pitcher_hr_vulnerability")

    conn.commit()
    conn.close()

    # ── Validation ─────────────────────────────────────────────────────────
    conn = sqlite3.connect(db_path)
    tables  = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    sc_cols = {r[1] for r in conn.execute(
        "PRAGMA table_info(stg_statcast_pitches)"
    ).fetchall()}
    conn.close()

    for col in ("hc_x", "hc_y"):
        status = "OK" if col in sc_cols else "MISSING — check for errors above"
        log.info("  stg_statcast_pitches.%s: %s", col, status)
    for tbl in ("fact_batter_power_profile", "fact_pitcher_hr_vulnerability"):
        status = "OK" if tbl in tables else "MISSING — check for errors above"
        log.info("  %s: %s", tbl, status)

    log.info("Migration complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add hc_x/hc_y + power profile tables to mlb_pregame.db"
    )
    parser.add_argument("--db-path", default="data/mlb_pregame.db")
    args = parser.parse_args()
    run_migration(args.db_path)
