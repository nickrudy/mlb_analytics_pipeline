"""
ingest_statcast.py
------------------
Downloads Statcast pitch-by-pitch data via pybaseball and loads it into
stg_statcast_pitches.

Works with both SQLite (local) and Supabase (cloud) via utils/db.py.

Usage:
    python ingest/ingest_statcast.py --start 2025-04-01 --end 2025-04-15
    python ingest/ingest_statcast.py --season 2025
    python ingest/ingest_statcast.py --last-n-days 30
"""
import json
import time
import logging
import argparse
from datetime import date, timedelta

from utils.db import get_connection, DB_BACKEND
from utils.db_bulk import bulk_upsert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

COLUMN_MAP = {
    "game_date":                       "game_date",
    "game_pk":                         "game_pk",
    "at_bat_number":                   "at_bat_number",
    "pitch_number":                    "pitch_number",
    "pitcher":                         "pitcher_id",
    "batter":                          "batter_id",
    "pitch_type":                      "pitch_type_code",
    "stand":                           "stand",
    "p_throws":                        "p_throws",
    "balls":                           "balls",
    "strikes":                         "strikes",
    "zone":                            "zone",
    "plate_x":                         "plate_x",
    "plate_z":                         "plate_z",
    "release_speed":                   "release_speed",
    "release_spin_rate":               "release_spin_rate",
    "release_extension":               "release_extension",
    "release_pos_x":                   "release_pos_x",
    "release_pos_z":                   "release_pos_z",
    "pfx_x":                           "pfx_x",
    "pfx_z":                           "pfx_z",
    "description":                     "description",
    "events":                          "events",
    "bb_type":                         "bb_type",
    "launch_speed":                    "launch_speed",
    "launch_angle":                    "launch_angle",
    "estimated_ba_using_speedangle":   "estimated_ba_using_speedangle",
    "estimated_woba_using_speedangle": "estimated_woba_using_speedangle",
    "hc_x":                            "hc_x",
    "hc_y":                            "hc_y",
}


# ── Type helpers ───────────────────────────────────────────────────────────

def _int(val):
    try:
        return None if (val is None or val != val) else int(val)
    except (TypeError, ValueError):
        return None

def _float(val):
    try:
        if val is None:
            return None
        f = float(val)
        return None if f != f else f
    except (TypeError, ValueError):
        return None


# ── Dependency check ───────────────────────────────────────────────────────

def _check_pybaseball() -> bool:
    try:
        import pybaseball  # noqa: F401
        return True
    except ImportError:
        log.error("pybaseball not installed. Run: pip install pybaseball")
        return False


# ── Date chunking ──────────────────────────────────────────────────────────

def _date_chunks(start: date, end: date, chunk_days: int = 14):
    cur = start
    while cur <= end:
        yield cur, min(cur + timedelta(days=chunk_days - 1), end)
        cur += timedelta(days=chunk_days)


# ── Chunk loader ───────────────────────────────────────────────────────────

def _load_chunk(conn, start_str: str, end_str: str) -> int:
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

    df = df.rename(columns=COLUMN_MAP)
    keep = [c for c in COLUMN_MAP.values() if c in df.columns]
    df   = df[keep].copy()
    df["game_date"] = df["game_date"].astype(str)

    rows = []
    for _, row in df.iterrows():
        rows.append({
            "game_date":    row.get("game_date"),
            "game_pk":      _int(row.get("game_pk")),
            "at_bat_number":_int(row.get("at_bat_number")),
            "pitch_number": _int(row.get("pitch_number")),
            "pitcher_id":   _int(row.get("pitcher_id")),
            "batter_id":    _int(row.get("batter_id")),
            "pitch_type_code": row.get("pitch_type_code"),
            "stand":        row.get("stand"),
            "p_throws":     row.get("p_throws"),
            "balls":        _int(row.get("balls")),
            "strikes":      _int(row.get("strikes")),
            "zone":         _int(row.get("zone")),
            "plate_x":      _float(row.get("plate_x")),
            "plate_z":      _float(row.get("plate_z")),
            "release_speed":       _float(row.get("release_speed")),
            "release_spin_rate":   _float(row.get("release_spin_rate")),
            "release_extension":   _float(row.get("release_extension")),
            "release_pos_x":       _float(row.get("release_pos_x")),
            "release_pos_z":       _float(row.get("release_pos_z")),
            "pfx_x":        _float(row.get("pfx_x")),
            "pfx_z":        _float(row.get("pfx_z")),
            "description":  row.get("description"),
            "events":       row.get("events"),
            "bb_type":      row.get("bb_type"),
            "launch_speed": _float(row.get("launch_speed")),
            "launch_angle": _float(row.get("launch_angle")),
            "estimated_ba_using_speedangle":   _float(row.get("estimated_ba_using_speedangle")),
            "estimated_woba_using_speedangle": _float(row.get("estimated_woba_using_speedangle")),
            "hc_x":         _float(row.get("hc_x")),
            "hc_y":         _float(row.get("hc_y")),
            "raw_payload_json": None,
        })

    n = bulk_upsert(conn, "stg_statcast_pitches", rows,
        conflict_cols="game_date,game_pk,at_bat_number,pitch_number",
        update_cols=[])   # [] == DO NOTHING (Supabase) / INSERT OR IGNORE (SQLite): keep existing row
    conn.commit()
    log.info("  Inserted %d rows (attempted; conflicts skipped).", n)
    return n


# ── Main runner ────────────────────────────────────────────────────────────

def run(start: date, end: date, sleep_between_chunks: float = 3.0) -> None:
    if not _check_pybaseball():
        return

    with get_connection() as conn:
        if DB_BACKEND == "sqlite":
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")

        total = 0
        for chunk_start, chunk_end in _date_chunks(start, end, chunk_days=14):
            n = _load_chunk(conn, chunk_start.isoformat(), chunk_end.isoformat())
            total += n
            if chunk_end < end:
                log.info("  Sleeping %.1fs between chunks...", sleep_between_chunks)
                time.sleep(sleep_between_chunks)

    log.info("Statcast ingestion complete. Total rows inserted: %d", total)


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start",       help="Start date YYYY-MM-DD")
    parser.add_argument("--end",         help="End date YYYY-MM-DD")
    parser.add_argument("--season",      type=int)
    parser.add_argument("--last-n-days", type=int)
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

    run(start=start_d, end=end_d)