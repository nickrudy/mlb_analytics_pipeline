"""
ingest_batter_splits_statsapi.py
---------------------------------
Pulls near-realtime batting splits (vs RHP / vs LHP) from the MLB Stats API
and writes them into fact_batter_hand_splits.

Works with both SQLite (local) and Supabase (cloud) via utils/db.py.

Usage:
    python ingest/ingest_batter_splits_statsapi.py --today
    python ingest/ingest_batter_splits_statsapi.py --date 2026-05-01
"""
import json
import time
import logging
import argparse
from datetime import date
from pathlib import Path
import urllib.request
import urllib.error
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.db import get_connection, DB_BACKEND

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

MLB_BASE = "https://statsapi.mlb.com/api/v1"
HEADERS  = {"User-Agent": "mlb-pregame-pipeline/1.0"}


# ── HTTP helper ────────────────────────────────────────────────────────────

def _get(url: str, params: dict = None, retries: int = 3) -> dict:
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{qs}"
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            log.warning("HTTP %s for %s (attempt %d)", e.code, url, attempt)
            if e.code in (429, 500, 502, 503) and attempt < retries:
                time.sleep(2 ** attempt)
            else:
                return {}
        except Exception as e:
            log.warning("Request error %s (attempt %d)", e, attempt)
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                return {}
    return {}


# ── Safe type helpers ──────────────────────────────────────────────────────

def _float(val):
    try:
        if val is None:
            return None
        f = float(val)
        return None if f != f else f
    except (TypeError, ValueError):
        return None

def _int(val):
    try:
        return None if val is None else int(val)
    except (TypeError, ValueError):
        return None


# ── Split fetch ────────────────────────────────────────────────────────────

def fetch_player_splits(player_id: int, season: int) -> dict:
    url    = f"{MLB_BASE}/people/{player_id}/stats"
    params = {"stats": "statSplits", "group": "hitting",
               "season": str(season), "sitCodes": "vr,vl"}
    data   = _get(url, params)
    if not data:
        return {}
    results = {}
    for stat_group in data.get("stats", []):
        for split in stat_group.get("splits", []):
            site_code = split.get("split", {}).get("code", "")
            if site_code not in ("vr", "vl"):
                continue
            hand = "R" if site_code == "vr" else "L"
            s    = split.get("stat", {})
            results[hand] = {
                "plate_appearances": _int(s.get("plateAppearances")),
                "at_bats":          _int(s.get("atBats")),
                "hits":             _int(s.get("hits")),
                "batting_avg":      _float(s.get("avg")),
                "on_base_pct":      _float(s.get("obp")),
                "slugging_pct":     _float(s.get("slg")),
                "ops":              _float(s.get("ops")),
                "walks":            _int(s.get("baseOnBalls")),
                "strikeouts":       _int(s.get("strikeOuts")),
                "woba":             None,
                "xba":              None,
                "xwoba":            None,
            }
    return results


# ── SQL helpers ────────────────────────────────────────────────────────────

def _upsert_split_sql():
    if DB_BACKEND == "supabase":
        return """
            INSERT INTO fact_batter_hand_splits
                (as_of_date, player_id, season, split_hand, window_code,
                 plate_appearances, at_bats, hits,
                 batting_avg, on_base_pct, slugging_pct, ops,
                 woba, xba, xwoba, bb_rate, k_rate)
            VALUES
                (:aod,:pid,:season,:hand,:wc,
                 :pa,:ab,:hits,
                 :avg,:obp,:slg,:ops,
                 :woba,:xba,:xwoba,:bb,:k)
            ON CONFLICT (as_of_date, player_id, season, split_hand, window_code)
            DO UPDATE SET
                plate_appearances = EXCLUDED.plate_appearances,
                at_bats           = EXCLUDED.at_bats,
                hits              = EXCLUDED.hits,
                batting_avg       = EXCLUDED.batting_avg,
                on_base_pct       = EXCLUDED.on_base_pct,
                slugging_pct      = EXCLUDED.slugging_pct,
                ops               = EXCLUDED.ops,
                bb_rate           = EXCLUDED.bb_rate,
                k_rate            = EXCLUDED.k_rate
        """
    return """
        INSERT OR REPLACE INTO fact_batter_hand_splits
            (as_of_date, player_id, season, split_hand, window_code,
             plate_appearances, at_bats, hits,
             batting_avg, on_base_pct, slugging_pct, ops,
             woba, xba, xwoba, bb_rate, k_rate)
        VALUES
            (:aod,:pid,:season,:hand,:wc,
             :pa,:ab,:hits,
             :avg,:obp,:slg,:ops,
             :woba,:xba,:xwoba,:bb,:k)
    """


# ── Database write ─────────────────────────────────────────────────────────

def upsert_splits(conn, player_id, season, as_of_date, splits,
                  window_code="SEASON") -> int:
    written = 0
    for split_hand, s in splits.items():
        if not s.get("at_bats") or s["at_bats"] == 0:
            continue
        pa     = s.get("plate_appearances") or 0
        bb_rate = _float(s["walks"] / pa)      if pa and s.get("walks")      else None
        k_rate  = _float(s["strikeouts"] / pa) if pa and s.get("strikeouts") else None
        conn.execute(_upsert_split_sql(), {
            "aod": as_of_date, "pid": player_id, "season": season,
            "hand": split_hand, "wc": window_code,
            "pa":   s.get("plate_appearances"),
            "ab":   s.get("at_bats"),
            "hits": s.get("hits"),
            "avg":  s.get("batting_avg"),
            "obp":  s.get("on_base_pct"),
            "slg":  s.get("slugging_pct"),
            "ops":  s.get("ops"),
            "woba": s.get("woba"),
            "xba":  s.get("xba"),
            "xwoba":s.get("xwoba"),
            "bb":   bb_rate,
            "k":    k_rate,
        })
        written += 1
    return written


# ── Main ingestion ─────────────────────────────────────────────────────────

def ingest_batter_splits(conn, as_of_date, season, window_code="SEASON"):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT player_id, full_name, primary_position
        FROM   dim_players
        WHERE  active_flag = 1
          AND  (primary_position IS NULL OR primary_position != 'P')
        ORDER  BY full_name
        """,
    )
    players = cur.fetchall()
    if not players:
        log.warning("No active position players in dim_players. Run seed-dimensions first.")
        return

    log.info("Fetching splits for %d active position players (season %d)...",
             len(players), season)
    total_written = total_skipped = 0
    batch_size    = 50

    for i, (player_id, full_name, _) in enumerate(players, start=1):
        splits = fetch_player_splits(player_id, season)
        if not splits:
            total_skipped += 1
        else:
            total_written += upsert_splits(conn, player_id, season,
                                            as_of_date, splits, window_code)
        if i % batch_size == 0:
            conn.commit()
            log.info("  Progress: %d / %d (%d written, %d skipped)...",
                     i, len(players), total_written, total_skipped)
        time.sleep(0.1)

    conn.commit()
    log.info("Split ingestion complete — %d rows written, %d skipped.",
             total_written, total_skipped)


# ── Refresh matchup baselines ──────────────────────────────────────────────

def refresh_matchup_baselines(conn, as_of_date, window_code="SEASON"):
    log.info("Refreshing matchup baseline averages from updated splits...")
    cur = conn.cursor()
    cur.execute(
        """
        SELECT m.game_id, m.batter_id, m.pitcher_id, p.throws
        FROM   fact_matchup_batter_pitcher m
        JOIN   dim_players p ON p.player_id = m.pitcher_id
        WHERE  m.as_of_date = :aod AND m.window_code = :wc
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    matchups = cur.fetchall()
    updated  = 0
    for game_id, batter_id, pitcher_id, pitcher_throws in matchups:
        if not pitcher_throws or pitcher_throws not in ("R", "L"):
            continue
        cur.execute(
            """
            SELECT batting_avg, woba
            FROM   fact_batter_hand_splits
            WHERE  as_of_date = :aod AND player_id  = :bid
              AND  split_hand = :hand AND window_code = :wc
            """,
            {"aod": as_of_date, "bid": batter_id,
             "hand": pitcher_throws, "wc": window_code},
        )
        split = cur.fetchone()
        if not split or split[0] is None:
            continue
        conn.execute(
            """
            UPDATE fact_matchup_batter_pitcher
            SET    batter_vs_hand_batting_avg = :avg,
                   batter_vs_hand_woba        = :woba
            WHERE  as_of_date = :aod AND game_id    = :gid
              AND  batter_id  = :bid AND pitcher_id = :pid
              AND  window_code = :wc
            """,
            {
                "avg": split[0], "woba": split[1],
                "aod": as_of_date, "gid": game_id,
                "bid": batter_id, "pid": pitcher_id, "wc": window_code,
            },
        )
        updated += 1
    conn.commit()
    log.info("Matchup baselines refreshed: %d rows updated.", updated)


# ── Entry point ────────────────────────────────────────────────────────────

def run(as_of_date, season, window_code="SEASON"):
    with get_connection() as conn:
        if DB_BACKEND == "sqlite":
            conn.execute("PRAGMA foreign_keys=OFF;")
            conn.execute("PRAGMA journal_mode=WAL;")
        ingest_batter_splits(conn, as_of_date, season, window_code)
        refresh_matchup_baselines(conn, as_of_date, window_code)
    log.info("Batter splits ingestion complete for %s.", as_of_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",   help="as_of_date YYYY-MM-DD")
    parser.add_argument("--today",  action="store_true")
    parser.add_argument("--season", type=int, default=date.today().year)
    parser.add_argument("--window", default="SEASON")
    args = parser.parse_args()

    as_of = args.date if args.date else (
        date.today().isoformat() if args.today else None
    )
    if not as_of:
        parser.error("Provide --date YYYY-MM-DD or --today")

    run(as_of_date=as_of, season=args.season, window_code=args.window)