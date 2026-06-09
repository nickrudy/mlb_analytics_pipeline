"""
migrate_add_boxscore_table.py
------------------------------
Adds fact_player_game_results to store actual per-player batting
line actuals from completed games. Used as the ground truth table
for backtesting projected_batting_avg, projected_total_bases,
and projected_hr_probability against real outcomes.

Safe to run multiple times — uses CREATE TABLE IF NOT EXISTS.

Usage:
    python db/migrate_add_boxscore_table.py --db-path data/mlb_pregame.db
"""

import sqlite3
import logging
import argparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def run_migration(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")
    conn.execute("PRAGMA journal_mode=WAL;")

    log.info("Adding fact_player_game_results to %s", db_path)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_player_game_results (
            game_date       TEXT    NOT NULL,
            game_id         INTEGER NOT NULL,
            player_id       INTEGER NOT NULL,
            team_id         INTEGER NOT NULL,

            -- Batting line actuals
            at_bats         INTEGER,
            plate_appearances INTEGER,
            hits            INTEGER,
            doubles         INTEGER,
            triples         INTEGER,
            home_runs       INTEGER,
            rbi             INTEGER,
            walks           INTEGER,
            strikeouts      INTEGER,
            hit_by_pitch    INTEGER,
            sac_flies       INTEGER,
            stolen_bases    INTEGER,

            -- Derived actuals (computed on ingest for convenience)
            total_bases     INTEGER,   -- 1×1B + 2×2B + 3×3B + 4×HR
            batting_avg     REAL,      -- hits / at_bats
            slugging_pct    REAL,      -- total_bases / at_bats
            hr_flag         INTEGER,   -- 1 if home_runs > 0, else 0

            -- Lineup context
            lineup_slot     INTEGER,   -- batting order position (1-9)
            position        TEXT,      -- fielding position played

            -- Metadata
            load_timestamp_utc TEXT,

            PRIMARY KEY (game_date, game_id, player_id),
            FOREIGN KEY (player_id) REFERENCES dim_players(player_id),
            FOREIGN KEY (team_id)   REFERENCES dim_teams(team_id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_boxscore_player_date
            ON fact_player_game_results(player_id, game_date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_boxscore_game
            ON fact_player_game_results(game_id, game_date)
    """)

    conn.commit()

    # Validate
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    conn.close()

    status = "OK" if "fact_player_game_results" in tables else "MISSING"
    log.info("fact_player_game_results: %s", status)
    log.info("Migration complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add fact_player_game_results table for boxscore actuals"
    )
    parser.add_argument("--db-path", default="data/mlb_pregame.db")
    args = parser.parse_args()
    run_migration(args.db_path)
