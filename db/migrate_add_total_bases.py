"""
migrate_add_total_bases.py
--------------------------
Adds total bases projection columns to fact_matchup_batter_pitcher
and slugging columns to fact_batter_pitch_type_splits and
fact_batter_zone_splits.

Safe to run multiple times — uses try/except to skip columns that
already exist.

Usage:
    python db/migrate_add_total_bases.py --db-path data/mlb_pregame.db
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

    log.info("Running total bases projection migration on %s", db_path)

    # ── fact_matchup_batter_pitcher — new projection columns ───────────────
    matchup_cols = [
        ("proj_at_bats_per_game",   "REAL"),  # personalized AB/game rate
        ("pt_slg_score",            "REAL"),  # pitch-type weighted slugging
        ("zone_slg_score",          "REAL"),  # zone weighted slugging
        ("projected_slugging",      "REAL"),  # blended projected SLG
        ("projected_total_bases",   "REAL"),  # proj_slg × proj_ab_per_game
    ]
    for col, col_type in matchup_cols:
        add_column_safe(conn, "fact_matchup_batter_pitcher", col, col_type)

    # ── fact_batter_overall — add ab_per_game ──────────────────────────────
    add_column_safe(conn, "fact_batter_overall", "games_played", "INTEGER")
    add_column_safe(conn, "fact_batter_overall", "ab_per_game",  "REAL")

    # ── fact_batter_pitch_type_splits — add slugging ───────────────────────
    add_column_safe(conn, "fact_batter_pitch_type_splits", "slugging_pct", "REAL")

    # ── fact_batter_zone_splits — add slugging ─────────────────────────────
    add_column_safe(conn, "fact_batter_zone_splits", "slugging_pct", "REAL")

    conn.commit()
    conn.close()
    log.info("Migration complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="data/mlb_pregame.db")
    args = parser.parse_args()
    run_migration(args.db_path)
