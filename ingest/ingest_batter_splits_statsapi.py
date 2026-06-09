"""
ingest_batter_splits_statsapi.py
---------------------------------
Pulls near-realtime batting splits (vs RHP / vs LHP) directly from the
MLB Stats API for every active player in dim_players, and writes them
into fact_batter_hand_splits.

This supplements the Statcast-derived splits (which lag 24-48 hours)
with MLB Stats API data that updates within ~2-4 hours of game completion.
The result is a more current baseline_avg in fact_matchup_batter_pitcher.

What this pulls per player:
  - Batting average vs RHP (split_hand = R)
  - Batting average vs LHP (split_hand = L)
  - Supporting stats: OBP, SLG, OPS, PA, AB, H, HR, BB, K, wOBA

Free API endpoint:
  https://statsapi.mlb.com/api/v1/people/{player_id}/stats
  ?stats=statSplits&group=hitting&season={season}&sitCodes=vr,vl

Usage:
    python ingest/ingest_batter_splits_statsapi.py --today --db-path data/mlb_pregame.db
    python ingest/ingest_batter_splits_statsapi.py --date 2026-05-01 --db-path data/mlb_pregame.db

Notes:
    - Runs after ingest_mlb_statsapi.py so dim_players is populated
    - Writes INSERT OR REPLACE so re-running is safe and idempotent
    - Skips pitchers (primary_position = 'P') to save API calls
    - Sleeps 0.1s between player requests to respect rate limits
    - A full 30-team active roster (~750 position players) takes ~2-3 minutes
"""

import sqlite3
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

MLB_BASE = "https://statsapi.mlb.com/api/v1"
HEADERS  = {"User-Agent": "mlb-pregame-pipeline/1.0"}

# MLB Stats API site codes for handedness splits
SPLIT_CODES = {
    "R": "vr",  # vs right-handed pitchers
    "L": "vl",  # vs left-handed pitchers
}


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
        if val is None:
            return None
        return int(val)
    except (TypeError, ValueError):
        return None


# ── Split fetch ────────────────────────────────────────────────────────────

def fetch_player_splits(player_id: int, season: int) -> dict[str, dict]:
    """
    Fetch vs-RHP and vs-LHP splits for one player from the MLB Stats API.

    Returns a dict keyed by split_hand ('R' or 'L'), each containing
    a dict of stat values. Returns empty dict on failure.
    """
    url = f"{MLB_BASE}/people/{player_id}/stats"
    params = {
        "stats":   "statSplits",
        "group":   "hitting",
        "season":  str(season),
        "sitCodes": "vr,vl",
    }
    data = _get(url, params)
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
                "doubles":          _int(s.get("doubles")),
                "triples":          _int(s.get("triples")),
                "home_runs":        _int(s.get("homeRuns")),
                "walks":            _int(s.get("baseOnBalls")),
                "strikeouts":       _int(s.get("strikeOuts")),
                "hit_by_pitch":     _int(s.get("hitByPitch")),
                "sac_flies":        _int(s.get("sacFlies")),
                "batting_avg":      _float(s.get("avg")),
                "on_base_pct":      _float(s.get("obp")),
                "slugging_pct":     _float(s.get("slg")),
                "ops":              _float(s.get("ops")),
                # wOBA not in standard splits endpoint — leave for Statcast
                "woba":             None,
                "xba":              None,
                "xwoba":            None,
            }

    return results


# ── Database write ─────────────────────────────────────────────────────────

def upsert_splits(conn: sqlite3.Connection, player_id: int,
                  season: int, as_of_date: str,
                  splits: dict[str, dict],
                  window_code: str = "SEASON") -> int:
    """
    Write split rows to fact_batter_hand_splits.
    Returns number of rows written.
    """
    written = 0
    for split_hand, s in splits.items():
        # Skip splits with no meaningful data
        if not s.get("at_bats") or s["at_bats"] == 0:
            continue

        # Derive bb_rate and k_rate from raw counts
        pa = s.get("plate_appearances") or 0
        bb_rate = _float(s["walks"] / pa)  if pa and s.get("walks")     else None
        k_rate  = _float(s["strikeouts"] / pa) if pa and s.get("strikeouts") else None

        conn.execute(
            """
            INSERT OR REPLACE INTO fact_batter_hand_splits
                (as_of_date, player_id, season, split_hand, window_code,
                 plate_appearances, at_bats, hits,
                 batting_avg, on_base_pct, slugging_pct, ops,
                 woba, xba, xwoba,
                 bb_rate, k_rate)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                as_of_date, player_id, season, split_hand, window_code,
                s.get("plate_appearances"),
                s.get("at_bats"),
                s.get("hits"),
                s.get("batting_avg"),
                s.get("on_base_pct"),
                s.get("slugging_pct"),
                s.get("ops"),
                s.get("woba"),
                s.get("xba"),
                s.get("xwoba"),
                bb_rate,
                k_rate,
            ),
        )
        written += 1
    return written


# ── Main ingestion ─────────────────────────────────────────────────────────

def ingest_batter_splits(conn: sqlite3.Connection,
                         as_of_date: str,
                         season: int,
                         window_code: str = "SEASON") -> None:
    """
    Pull and store MLB Stats API handedness splits for all active
    position players in dim_players.
    """
    # Fetch all active position players (skip pitchers)
    players = conn.execute(
        """
        SELECT player_id, full_name, primary_position
        FROM   dim_players
        WHERE  active_flag = 1
          AND  (primary_position IS NULL OR primary_position != 'P')
        ORDER  BY full_name
        """,
    ).fetchall()

    if not players:
        log.warning("No active position players found in dim_players. "
                    "Run seed-dimensions first.")
        return

    log.info("Fetching splits for %d active position players (season %d)...",
             len(players), season)

    total_written = 0
    total_skipped = 0
    batch_size    = 50

    for i, (player_id, full_name, position) in enumerate(players, start=1):
        splits = fetch_player_splits(player_id, season)

        if not splits:
            total_skipped += 1
        else:
            written = upsert_splits(
                conn, player_id, season, as_of_date, splits, window_code
            )
            total_written += written

        # Commit in batches
        if i % batch_size == 0:
            conn.commit()
            log.info("  Progress: %d / %d players processed "
                     "(%d split rows written, %d skipped)...",
                     i, len(players), total_written, total_skipped)

        # Rate limit: 0.1s between requests
        time.sleep(0.1)

    conn.commit()
    log.info("Split ingestion complete.")
    log.info("  Players processed: %d", len(players))
    log.info("  Split rows written: %d", total_written)
    log.info("  Players skipped (no data): %d", total_skipped)


# ── Update matchup baseline averages ──────────────────────────────────────

def refresh_matchup_baselines(conn: sqlite3.Connection,
                               as_of_date: str,
                               window_code: str = "SEASON") -> None:
    """
    After writing fresh splits, update batter_vs_hand_batting_avg in
    fact_matchup_batter_pitcher to reflect the new API-sourced averages.

    This is the key step that makes the fresher data flow through to
    the projected batting averages in Tableau.
    """
    log.info("Refreshing matchup baseline averages from updated splits...")

    # Get pitcher handedness for each matchup row
    matchups = conn.execute(
        """
        SELECT m.game_id, m.batter_id, m.pitcher_id, p.throws
        FROM   fact_matchup_batter_pitcher m
        JOIN   dim_players p ON p.player_id = m.pitcher_id
        WHERE  m.as_of_date  = ?
          AND  m.window_code = ?
        """,
        (as_of_date, window_code),
    ).fetchall()

    updated = 0
    for game_id, batter_id, pitcher_id, pitcher_throws in matchups:
        if not pitcher_throws or pitcher_throws not in ("R", "L"):
            continue

        # Look up fresh split from fact_batter_hand_splits
        split = conn.execute(
            """
            SELECT batting_avg, on_base_pct, slugging_pct, ops,
                   woba, bb_rate, k_rate
            FROM   fact_batter_hand_splits
            WHERE  as_of_date  = ?
              AND  player_id   = ?
              AND  split_hand  = ?
              AND  window_code = ?
            """,
            (as_of_date, batter_id, pitcher_throws, window_code),
        ).fetchone()

        if not split or split[0] is None:
            continue

        # Update the matchup row with fresh baseline
        conn.execute(
            """
            UPDATE fact_matchup_batter_pitcher
            SET    batter_vs_hand_batting_avg = ?,
                   batter_vs_hand_woba        = ?
            WHERE  as_of_date  = ?
              AND  game_id     = ?
              AND  batter_id   = ?
              AND  pitcher_id  = ?
              AND  window_code = ?
            """,
            (
                split[0],  # batting_avg
                split[4],  # woba
                as_of_date, game_id, batter_id, pitcher_id, window_code,
            ),
        )
        updated += 1

    conn.commit()
    log.info("Matchup baselines refreshed: %d rows updated.", updated)


# ── Entry point ────────────────────────────────────────────────────────────

def run(db_path: str, as_of_date: str, season: int,
        window_code: str = "SEASON") -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")
    conn.execute("PRAGMA journal_mode=WAL;")

    ingest_batter_splits(conn, as_of_date, season, window_code)
    refresh_matchup_baselines(conn, as_of_date, window_code)

    conn.close()
    log.info("Batter splits ingestion complete for %s.", as_of_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest near-realtime batter handedness splits from MLB Stats API"
    )
    parser.add_argument("--db-path",  default="data/mlb_pregame.db")
    parser.add_argument("--date",     help="as_of_date YYYY-MM-DD")
    parser.add_argument("--today",    action="store_true")
    parser.add_argument("--season",   type=int,
                        default=date.today().year,
                        help="MLB season year (default: current year)")
    parser.add_argument("--window",   default="SEASON")
    args = parser.parse_args()

    as_of = args.date if args.date else (
        date.today().isoformat() if args.today else None
    )
    if not as_of:
        parser.error("Provide --date YYYY-MM-DD or --today")

    run(
        db_path    = args.db_path,
        as_of_date = as_of,
        season     = args.season,
        window_code = args.window,
    )
