#!/usr/bin/env python3
"""
run_pipeline.py
---------------
Orchestrates the full daily pre-game data pipeline.

Works with both SQLite (local) and Supabase (cloud) via utils/db.py.
Backend is controlled by DB_BACKEND in .env (sqlite) or GitHub Actions
secrets (supabase).

Usage:
    python run_pipeline.py --today
    python run_pipeline.py --date 2025-04-15
    python run_pipeline.py --today --seed-dimensions
"""
import sys
import logging
import argparse
from datetime import date, timedelta
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

sys.path.insert(0, str(Path(__file__).parent))


def main():
    parser = argparse.ArgumentParser(description="MLB pre-game pipeline runner")
    parser.add_argument("--date",            help="Game date YYYY-MM-DD")
    parser.add_argument("--today",           action="store_true")
    parser.add_argument("--seed-dimensions", action="store_true",
                        help="Refresh teams/venues/rosters (run once at season start)")
    parser.add_argument("--statcast-days",   type=int, default=2)
    parser.add_argument("--skip-statcast",   action="store_true")
    parser.add_argument("--skip-weather",    action="store_true")
    parser.add_argument("--windows", default="SEASON,L30D,L14D,L7D")
    args = parser.parse_args()

    game_date = date.today().isoformat() if args.today else args.date
    if not game_date:
        parser.error("Provide --date YYYY-MM-DD or --today")

    # Import DB utilities — backend controlled by DB_BACKEND in .env
    from utils.db import DB_BACKEND, get_connection
    log.info("=== Pipeline starting | backend=%s | date=%s ===", DB_BACKEND, game_date)

    # Deduplicated window list used in Steps 5 and 6
    all_windows = list(dict.fromkeys(["SEASON"] + [w.strip() for w in args.windows.split(",")]))

    # ── Step 1: Init DB ────────────────────────────────────────────────────
    # SQLite only — Supabase schema is pre-created by migration script
    if DB_BACKEND == "sqlite":
        log.info("=== Step 1: Init DB (SQLite) ===")
        from db.init_db import init_db
        db_path = Path("data/mlb_pregame.db")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        init_db(str(db_path))
    else:
        log.info("=== Step 1: Init DB SKIPPED (Supabase schema pre-migrated) ===")

    # ── Step 2: MLB Stats API ──────────────────────────────────────────────
    log.info("=== Step 2: MLB Stats API (schedule + lineups) ===")
    from ingest.ingest_mlb_statsapi import run as statsapi_run
    statsapi_run(
        game_date       = game_date,
        as_of_date      = game_date,
        seed_dimensions = args.seed_dimensions,
    )

    # ── Step 2b: Batter splits ─────────────────────────────────────────────
    log.info("=== Step 2b: Batter splits (MLB Stats API) ===")
    from ingest.ingest_batter_splits_statsapi import run as splits_run
    splits_run(
        as_of_date  = game_date,
        season      = date.fromisoformat(game_date).year,
        window_code = "SEASON",
    )

    # ── Step 3: Statcast ───────────────────────────────────────────────────
    if not args.skip_statcast:
        log.info("=== Step 3: Statcast (last %d days) ===", args.statcast_days)
        from ingest.ingest_statcast import run as statcast_run
        start = date.fromisoformat(game_date) - timedelta(days=args.statcast_days)
        end   = date.fromisoformat(game_date)
        statcast_run(start=start, end=end)
    else:
        log.info("=== Step 3: Statcast SKIPPED ===")

    # ── Step 4: Weather ────────────────────────────────────────────────────
    if not args.skip_weather:
        log.info("=== Step 4: Weather forecast ===")
        from ingest.ingest_weather import ingest_weather
        with get_connection() as conn:
            if DB_BACKEND == "sqlite":
                conn.execute("PRAGMA foreign_keys=OFF;")
            ingest_weather(conn, game_date=game_date, as_of_date=game_date)
    else:
        log.info("=== Step 4: Weather SKIPPED ===")

    # ── Step 5: Transforms ─────────────────────────────────────────────────
    log.info("=== Step 5: Split transforms + matchups ===")
    from ingest.transform_splits import transform_splits, build_matchups, _window_dates
    with get_connection() as conn:
        if DB_BACKEND == "sqlite":
            conn.execute("PRAGMA foreign_keys=OFF;")
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")

        # Statcast-derived split tables only change when new Statcast data is
        # pulled. Skip the expensive table rewrites on intraday refreshes
        # (--skip-statcast) to avoid burning the Supabase Nano disk IO budget.
        # The morning full run populates all split tables; intraday runs
        # rebuild matchups only against the already-populated tables.
        if not args.skip_statcast:
            for wc in all_windows:
                start, end = _window_dates(wc, game_date)
                transform_splits(conn, as_of_date=game_date, window_code=wc,
                                 start_date=start, end_date=end)
        else:
            log.info("  Split transforms SKIPPED (--skip-statcast) — using existing split data.")

        for wc in all_windows:
            build_matchups(conn, as_of_date=game_date, window_code=wc)

    # ── Step 6: Match scores ───────────────────────────────────────────────
    log.info("=== Step 6: Match scores ===")
    from ingest.compute_match_scores import compute_match_scores
    with get_connection() as conn:
        if DB_BACKEND == "sqlite":
            conn.execute("PRAGMA foreign_keys=OFF;")
            conn.execute("PRAGMA journal_mode=WAL;")
        for wc in all_windows:
            compute_match_scores(conn, as_of_date=game_date, window_code=wc)

    # ── Step 7: Export to Google Sheets ───────────────────────────────────
    log.info("=== Step 7: Export to Google Sheets ===")
    try:
        from ingest.export_to_sheets import run as sheets_run
        sheets_run(as_of_date=game_date)
    except Exception as e:
        log.warning("Google Sheets export failed (non-fatal): %s", e)

    log.info("=== Pipeline complete for %s ===", game_date)


if __name__ == "__main__":
    main()
