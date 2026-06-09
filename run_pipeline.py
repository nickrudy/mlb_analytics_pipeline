#!/usr/bin/env python3
"""
run_pipeline.py
---------------
Orchestrates the full daily pre-game data pipeline.

Typical pre-game run order:
  1. init_db      – create schema (once, or idempotent)
  2. statsapi     – schedule + lineups for today
  3. statcast     – incremental pitch data for recent days
  4. weather      – game-time forecast for today's games
  5. transforms   – compute all split fact tables + matchups

Usage:
    python run_pipeline.py --today --db-path data/mlb_pregame.db
    python run_pipeline.py --date 2025-04-15 --db-path data/mlb_pregame.db
    python run_pipeline.py --today --seed-dimensions  # first run of the season
"""

import sys
import logging
import argparse
from datetime import date
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s]: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log"),
    ],
)
log = logging.getLogger("run_pipeline")

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))


def main():
    parser = argparse.ArgumentParser(description="MLB pre-game pipeline runner")
    parser.add_argument("--db-path",         default="data/mlb_pregame.db")
    parser.add_argument("--date",            help="Game date YYYY-MM-DD")
    parser.add_argument("--today",           action="store_true")
    parser.add_argument("--seed-dimensions", action="store_true",
                        help="Refresh teams/venues/rosters (run once at season start)")
    parser.add_argument("--statcast-days",   type=int, default=35,
                        help="How many days of Statcast history to ingest (default: 35)")
    parser.add_argument("--skip-statcast",   action="store_true",
                        help="Skip Statcast pull (fast run using existing pitch data)")
    parser.add_argument("--skip-weather",    action="store_true")
    parser.add_argument("--windows",         default="SEASON,L30D,L14D,L7D")
    args = parser.parse_args()

    game_date = date.today().isoformat() if args.today else args.date
    if not game_date:
        parser.error("Provide --date YYYY-MM-DD or --today")

    db_path = args.db_path
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Init DB ────────────────────────────────────────────────────
    log.info("=== Step 1: Init DB ===")
    from db.init_db import init_db
    init_db(db_path)

    # ── Step 2: MLB Stats API ──────────────────────────────────────────────
    log.info("=== Step 2: MLB Stats API (schedule + lineups) ===")
    from ingest.ingest_mlb_statsapi import run as statsapi_run
    statsapi_run(
        db_path=db_path,
        game_date=game_date,
        as_of_date=game_date,
        seed_dimensions=args.seed_dimensions,
    )

    # ── Step 2b: Batter splits from MLB Stats API ────────────────────────────
    log.info("=== Step 2b: Batter splits (MLB Stats API) ===")
    from ingest.ingest_batter_splits_statsapi import run as splits_run
    splits_run(
        db_path     = db_path,
        as_of_date  = game_date,
        season      = date.fromisoformat(game_date).year,
        window_code = "SEASON",
    )

    # ── Step 3: Statcast ───────────────────────────────────────────────────
    if not args.skip_statcast:
        log.info("=== Step 3: Statcast ingestion (last %d days) ===", args.statcast_days)
        from datetime import timedelta
        from ingest.ingest_statcast import run as statcast_run
        start = (date.fromisoformat(game_date) - timedelta(days=args.statcast_days))
        end   = date.fromisoformat(game_date)
        statcast_run(db_path=db_path, start=start, end=end)
    else:
        log.info("=== Step 3: Statcast SKIPPED ===")

    # ── Step 4: Weather ────────────────────────────────────────────────────
    if not args.skip_weather:
        log.info("=== Step 4: Weather forecast ===")
        import sqlite3
        from ingest.ingest_weather import ingest_weather
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys=OFF;")
        ingest_weather(conn, game_date=game_date, as_of_date=game_date)
        conn.close()
    else:
        log.info("=== Step 4: Weather SKIPPED ===")

    # ── Step 5: Transforms ─────────────────────────────────────────────────
    log.info("=== Step 5: Split transforms + matchups ===")
    import sqlite3
    from datetime import timedelta
    from ingest.transform_splits import transform_splits, build_matchups, _window_dates
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    for wc in [w.strip() for w in args.windows.split(",")]:
        start, end = _window_dates(wc, game_date)
        transform_splits(conn, as_of_date=game_date, window_code=wc,
                         start_date=start, end_date=end)
    build_matchups(conn, as_of_date=game_date)
    conn.close()

    # ── Step 6: Match scores ───────────────────────────────────────────────
    log.info("=== Step 6: Pitch type + zone match scores ===")
    from ingest.compute_match_scores import compute_match_scores
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")
    conn.execute("PRAGMA journal_mode=WAL;")
    for wc in [w.strip() for w in args.windows.split(",")]:
        compute_match_scores(conn, as_of_date=game_date, window_code=wc)
    conn.close()

    # ── Step 7: Export to Google Sheets ──────────────────────────────────
    log.info("=== Step 7: Export to Google Sheets ===")
    try:
        from ingest.export_to_sheets import run as sheets_run
        sheets_run(db_path=db_path, as_of_date=game_date)
    except Exception as e:
        log.warning("Google Sheets export failed (non-fatal): %s", e)
        log.warning("Dashboard will need manual refresh if using Sheets connection.")

    log.info("=== Pipeline complete for %s ===", game_date)


if __name__ == "__main__":
    main()
