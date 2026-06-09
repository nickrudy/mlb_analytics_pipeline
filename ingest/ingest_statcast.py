"""
ingest_statcast.py
------------------
Downloads Statcast pitch-by-pitch data via pybaseball (which scrapes
Baseball Savant) and loads it into stg_statcast_pitches.

From the raw staging table, subsequent transform scripts will compute
all fact_batter_* and fact_pitcher_* split tables.

Free data source:
    https://baseballsavant.mlb.com/csv-docs
    https://github.com/jldbc/pybaseball/blob/master/docs/statcast.md

Usage:
    pip install pybaseball
    python ingest/ingest_statcast.py --start 2025-04-01 --end 2025-04-15 --db-path data/mlb_pregame.db
    python ingest/ingest_statcast.py --season 2025 --db-path data/mlb_pregame.db
    python ingest/ingest_statcast.py --last-n-days 30 --db-path data/mlb_pregame.db

Notes:
    - Baseball Savant rate-limits heavy requests; the script chunks by
      two-week windows and sleeps between calls.
    - First full-season load (~700k rows) may take 10-20 minutes.
    - Subsequent incremental loads (last N days) are fast.
"""

import sqlite3
import json
import time
import logging
import argparse
from datetime import date, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# Statcast CSV columns we care about → SQLite column name mapping
# Left side = pybaseball / Savant column name
# Right side = stg_statcast_pitches column name
COLUMN_MAP = {
    "game_date":                      "game_date",
    "game_pk":                        "game_pk",
    "at_bat_number":                  "at_bat_number",
    "pitch_number":                   "pitch_number",
    "pitcher":                        "pitcher_id",
    "batter":                         "batter_id",
    "pitch_type":                     "pitch_type_code",
    "stand":                          "stand",
    "p_throws":                       "p_throws",
    "balls":                          "balls",
    "strikes":                        "strikes",
    "zone":                           "zone",
    "plate_x":                        "plate_x",
    "plate_z":                        "plate_z",
    "release_speed":                  "release_speed",
    "release_spin_rate":              "release_spin_rate",
    "release_extension":              "release_extension",
    "release_pos_x":                  "release_pos_x",
    "release_pos_z":                  "release_pos_z",
    "pfx_x":                          "pfx_x",
    "pfx_z":                          "pfx_z",
    "description":                    "description",
    "events":                         "events",
    "bb_type":                        "bb_type",
    "launch_speed":                   "launch_speed",
    "launch_angle":                   "launch_angle",
    "estimated_ba_using_speedangle":  "estimated_ba_using_speedangle",
    "estimated_woba_using_speedangle":"estimated_woba_using_speedangle",
    # Hit coordinates — required for pull rate / oppo rate in power profile.
    # Present in pybaseball statcast() output; not previously stored.
    # Run db/migrate_add_power_profile.py before ingesting with this version.
    "hc_x":                           "hc_x",
    "hc_y":                           "hc_y",
}


def _check_pybaseball() -> bool:
    try:
        import pybaseball  # noqa: F401
        return True
    except ImportError:
        log.error(
            "pybaseball not installed. Run: pip install pybaseball\n"
            "Then re-run this script."
        )
        return False


def _date_chunks(start: date, end: date, chunk_days: int = 14):
    """Yield (chunk_start, chunk_end) pairs over a date range."""
    cur = start
    while cur <= end:
        yield cur, min(cur + timedelta(days=chunk_days - 1), end)
        cur += timedelta(days=chunk_days)


def _load_chunk(conn: sqlite3.Connection, start_str: str, end_str: str) -> int:
    """
    Pull one date chunk via pybaseball.statcast() and INSERT OR IGNORE into
    stg_statcast_pitches. Returns number of rows inserted.
    """
    import pybaseball
    pybaseball.cache.enable()

    log.info("  Statcast pull: %s -> %s", start_str, end_str)
    try:
        df = pybaseball.statcast(start_dt=start_str, end_dt=end_str)
    except Exception as e:
        log.warning("  pybaseball error: %s – skipping chunk.", e)
        return 0

    if df is None or df.empty:
        log.info("  No data returned for this window.")
        return 0

    # Rename columns per our map; keep only mapped columns
    df = df.rename(columns=COLUMN_MAP)
    keep_cols = list(COLUMN_MAP.values())
    available = [c for c in keep_cols if c in df.columns]
    df = df[available].copy()

    # Coerce types
    for int_col in ["game_pk", "at_bat_number", "pitch_number",
                     "pitcher_id", "batter_id", "balls", "strikes", "zone"]:
        if int_col in df.columns:
            df[int_col] = df[int_col].where(df[int_col].notna(), None)

    df["game_date"] = df["game_date"].astype(str)

    # Build rows; store full raw row as JSON for lineage
    src_df_full = None
    try:
        import pybaseball as _pb
        src_df_full = _pb.statcast(start_dt=start_str, end_dt=end_str)
    except Exception:
        pass

    rows = []
    for i, row in df.iterrows():
        raw_json = None
        if src_df_full is not None and i in src_df_full.index:
            try:
                raw_json = src_df_full.loc[i].to_json()
            except Exception:
                pass

        rows.append((
            row.get("game_date"),
            _int(row.get("game_pk")),
            _int(row.get("at_bat_number")),
            _int(row.get("pitch_number")),
            _int(row.get("pitcher_id")),
            _int(row.get("batter_id")),
            row.get("pitch_type_code"),
            row.get("stand"),
            row.get("p_throws"),
            _int(row.get("balls")),
            _int(row.get("strikes")),
            _int(row.get("zone")),
            _float(row.get("plate_x")),
            _float(row.get("plate_z")),
            _float(row.get("release_speed")),
            _float(row.get("release_spin_rate")),
            _float(row.get("release_extension")),
            _float(row.get("release_pos_x")),
            _float(row.get("release_pos_z")),
            _float(row.get("pfx_x")),
            _float(row.get("pfx_z")),
            row.get("description"),
            row.get("events"),
            row.get("bb_type"),
            _float(row.get("launch_speed")),
            _float(row.get("launch_angle")),
            _float(row.get("estimated_ba_using_speedangle")),
            _float(row.get("estimated_woba_using_speedangle")),
            _float(row.get("hc_x")),
            _float(row.get("hc_y")),
            raw_json,
        ))

    conn.executemany(
        """
        INSERT OR IGNORE INTO stg_statcast_pitches VALUES
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    conn.commit()
    log.info("  Inserted %d rows.", len(rows))
    return len(rows)


def _int(val):
    try:
        if val is None or (hasattr(val, '__class__') and val != val):  # NaN check
            return None
        return int(val)
    except (TypeError, ValueError):
        return None


def _float(val):
    try:
        if val is None:
            return None
        f = float(val)
        return None if f != f else f  # NaN → None
    except (TypeError, ValueError):
        return None


def run(db_path: str, start: date, end: date, sleep_between_chunks: float = 3.0) -> None:
    if not _check_pybaseball():
        return

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    total = 0
    for chunk_start, chunk_end in _date_chunks(start, end, chunk_days=14):
        n = _load_chunk(conn, chunk_start.isoformat(), chunk_end.isoformat())
        total += n
        if chunk_end < end:
            log.info("  Sleeping %.1fs between chunks...", sleep_between_chunks)
            time.sleep(sleep_between_chunks)

    conn.close()
    log.info("Statcast ingestion complete. Total rows inserted: %d", total)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path",     default="data/mlb_pregame.db")
    parser.add_argument("--start",       help="Start date YYYY-MM-DD")
    parser.add_argument("--end",         help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--season",      type=int, help="Full season pull (e.g. 2025)")
    parser.add_argument("--last-n-days", type=int, help="Pull last N calendar days")
    args = parser.parse_args()

    today = date.today()

    if args.season:
        start_d = date(args.season, 3, 1)
        end_d   = min(date(args.season, 11, 30), today)
    elif args.last_n_days:
        start_d = today - timedelta(days=args.last_n_days)
        end_d   = today
    elif args.start:
        start_d = date.fromisoformat(args.start)
        end_d   = date.fromisoformat(args.end) if args.end else today
    else:
        parser.error("Provide --season, --last-n-days, or --start/--end")

    run(db_path=args.db_path, start=start_d, end=end_d)
