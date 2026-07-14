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
import zoneinfo
from datetime import date, timedelta, datetime
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
    parser.add_argument("--windows", default="SEASON")
    args = parser.parse_args()

    game_date = (datetime.now(zoneinfo.ZoneInfo("America/Chicago")).date().isoformat() 
             if args.today else args.date)
    if not game_date:
        parser.error("Provide --date YYYY-MM-DD or --today")

    # Import DB utilities — backend controlled by DB_BACKEND in .env
    from utils.db import DB_BACKEND, get_connection
    log.info("=== Pipeline starting | backend=%s | date=%s ===", DB_BACKEND, game_date)

    # Deduplicated window list used in Steps 5 and 6
    all_windows = list(dict.fromkeys(["SEASON"] + [w.strip() for w in args.windows.split(",")]))

    # ── Step 1: Init DB ────────────────────────────────────────────────────
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
        # (--skip-statcast) to avoid burning Supabase disk IO budget and
        # causing statement timeouts on the 307k-row pitch table.
        if not args.skip_statcast:
            for wc in all_windows:
                start, end = _window_dates(wc, game_date)
                transform_splits(conn, as_of_date=game_date, window_code=wc,
                                 start_date=start, end_date=end)
        else:
            log.info("  Split transforms SKIPPED (--skip-statcast) — using existing split data.")

        for wc in all_windows:
            build_matchups(conn, as_of_date=game_date, window_code=wc)

    # ── Step 5b: Cleanup stale split data (full runs only, Supabase only) ──
    # Supabase-only: this cleanup exists purely to control storage/IO cost on
    # the metered Supabase instance (see the P0/P1 refactor -- this table
    # list is the exact set of Supabase's daily IO burden). None of that
    # applies to local SQLite, where disk is effectively free and there is
    # no burst budget to protect. Skipping it locally lets every day's
    # snapshot accumulate, which is required for point-in-time backtesting
    # (see ARCHITECTURE.md's original design intent, and backtest/*.py,
    # which join across as_of_date history that this cleanup would otherwise
    # destroy the next day). Local SQLite is intentionally treated as the
    # deeper historical/analytical store; Supabase stays the lean, today-only
    # production serving layer.
    if not args.skip_statcast and DB_BACKEND == "supabase":
        log.info("=== Step 5b: Cleanup stale split data (Supabase) ===")
        stale_tables = [
            "fact_pitcher_zone_profile",
            "fact_batter_zone_splits",
            "fact_batter_pitch_type_splits",
            "fact_pitcher_pitch_mix",
            "fact_batter_overall",
            "fact_batter_hand_splits",
            "fact_pitcher_overall",
            "fact_pitcher_hand_splits",
            "fact_batter_power_profile",
            "fact_pitcher_hr_vulnerability",
            "fact_matchup_batter_pitcher",
        ]
        with get_connection() as conn:
            for table in stale_tables:
                cur = conn.cursor()
                cur.execute(
                    f"DELETE FROM {table} WHERE as_of_date != :today",
                    {"today": game_date},
                )
                log.info("  %s: stale rows deleted.", table)

    # ── Check: do any matchups exist yet for today? ────────────────────────
    # Every trigger (full run AND each intraday run) reaches this point
    # regardless of whether lineups have posted. Historically Steps 6/7/7b
    # would still run, hit zero matchup rows, and Step 7b's own guard
    # (RuntimeError, see export_to_daily_tables.py) would fail the whole
    # job -- correct behavior (never blank Looker on empty data), but it
    # meant EVERY early-morning/early-intraday trigger failed loudly and
    # emailed a failure notification, every single day, even though "no
    # lineups yet" is expected and not an error.
    #
    # This check moves the same "nothing to do yet" detection one step
    # earlier and treats it as a clean, successful no-op instead of a
    # failure -- for ANY trigger, not just the morning one, since it's
    # driven by actual data state rather than a hardcoded time assumption.
    # Step 7b's guard stays in place as defense-in-depth for the (different)
    # case where matchups exist but scoring/export still produces nothing.
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM fact_matchup_batter_pitcher WHERE as_of_date = :aod",
            {"aod": game_date},
        )
        matchup_count = cur.fetchone()[0]

    if matchup_count == 0:
        log.info("=== No matchups available yet for %s (lineups not posted) -- "
                  "skipping Steps 6/7/7b. ===", game_date)
        log.info("=== Pipeline complete for %s (seed-only; no lineups yet) ===", game_date)
        return

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

    # ── Step 7b: Export to daily flat tables (Looker Studio source) ────────
    log.info("=== Step 7b: Export to daily flat tables ===")
    try:
        from ingest.export_to_daily_tables import export_daily_tables
        export_daily_tables(as_of_date=game_date)
    except Exception:
        log.error("Daily table export FAILED — Looker source not updated. Failing run.", exc_info=True)
        raise

    log.info("=== Pipeline complete for %s ===", game_date)


if __name__ == "__main__":
    main()