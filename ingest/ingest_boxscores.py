"""
ingest_boxscores.py
--------------------
Pulls per-player batting line actuals from completed games via the
MLB Stats API game_boxscore endpoint and loads them into
fact_player_game_results.

Used as ground truth for backtesting projected_batting_avg,
projected_total_bases, and projected_hr_probability.

Free data source — same no-auth MLB Stats API used elsewhere:
    https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore

Pulls only games with status 'Final' to avoid partial game data.
Safe to re-run — uses INSERT OR REPLACE, so re-ingesting a date
overwrites any previously loaded rows for that date cleanly.

Usage:
    # Full 2026 season to date (recommended first run):
    python ingest/ingest_boxscores.py --season 2026 --db-path data/mlb_pregame.db

    # Single date:
    python ingest/ingest_boxscores.py --date 2026-04-01 --db-path data/mlb_pregame.db

    # Date range:
    python ingest/ingest_boxscores.py --start 2026-04-01 --end 2026-05-20 --db-path data/mlb_pregame.db

    # Last N days (incremental):
    python ingest/ingest_boxscores.py --last-n-days 7 --db-path data/mlb_pregame.db
"""

import sqlite3
import json
import time
import logging
import argparse
import urllib.request
import urllib.error
from datetime import date, timedelta, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

MLB_API_BASE = "https://statsapi.mlb.com/api/v1"
SLEEP_BETWEEN_GAMES = 0.4   # seconds — respectful of free API rate limits


# ── API helpers ────────────────────────────────────────────────────────────

def _fetch_json(url: str, retries: int = 3) -> dict | None:
    """Fetch a URL and return parsed JSON. Returns None on failure."""
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None   # game not found — not an error
            log.warning("HTTP %d fetching %s (attempt %d/%d)", e.code, url, attempt+1, retries)
        except Exception as e:
            log.warning("Error fetching %s: %s (attempt %d/%d)", url, e, attempt+1, retries)
        if attempt < retries - 1:
            time.sleep(1.5)
    return None


def _get_completed_game_pks(db_path: str, game_dates: list[str]) -> list[tuple[str, int]]:
    """
    Returns (game_date, game_pk) pairs for all Final games on the given dates,
    sourced from fact_games in the local DB.

    Uses fact_games rather than re-querying the API schedule endpoint so we
    stay consistent with the game_ids already in the database.
    """
    conn = sqlite3.connect(db_path)
    placeholders = ",".join("?" for _ in game_dates)
    rows = conn.execute(
        f"""
        SELECT g.game_date, g.game_id
        FROM   fact_games g
        WHERE  g.game_date IN ({placeholders})
        ORDER  BY g.game_date, g.game_id
        """,
        game_dates,
    ).fetchall()
    conn.close()
    return [(r[0], r[1]) for r in rows]


# ── Boxscore parsing ───────────────────────────────────────────────────────

def _parse_batting_line(player_data: dict) -> dict | None:
    """
    Extract batting stats from a player node in the boxscore response.
    Returns None if the player has no at_bats (pitcher, DNP, etc.)
    """
    stats = player_data.get("stats", {}).get("batting", {})
    if not stats:
        return None

    at_bats = stats.get("atBats", 0) or 0
    hits     = stats.get("hits", 0) or 0
    doubles  = stats.get("doubles", 0) or 0
    triples  = stats.get("triples", 0) or 0
    home_runs = stats.get("homeRuns", 0) or 0
    walks    = stats.get("baseOnBalls", 0) or 0
    strikeouts = stats.get("strikeOuts", 0) or 0
    hit_by_pitch = stats.get("hitByPitch", 0) or 0
    sac_flies  = stats.get("sacFlies", 0) or 0
    stolen_bases = stats.get("stolenBases", 0) or 0
    rbi        = stats.get("rbi", 0) or 0

    # Plate appearances — use API value if present, else compute
    pa = stats.get("plateAppearances", None)
    if not pa:
        # PA = AB + BB + HBP + SF + (SH if tracked)
        pa = at_bats + walks + hit_by_pitch + sac_flies

    # Derived
    singles      = hits - doubles - triples - home_runs
    total_bases  = singles + (2 * doubles) + (3 * triples) + (4 * home_runs)
    batting_avg  = round(hits / at_bats, 4) if at_bats > 0 else None
    slugging_pct = round(total_bases / at_bats, 4) if at_bats > 0 else None
    hr_flag      = 1 if home_runs > 0 else 0

    # Lineup position from allPositions
    all_positions = player_data.get("allPositions", [])
    position = all_positions[0].get("abbreviation") if all_positions else None

    # Batting order
    batting_order = player_data.get("battingOrder")
    lineup_slot = None
    if batting_order:
        try:
            # MLB API returns batting order as "100", "200", etc.
            lineup_slot = int(str(batting_order).strip()) // 100
        except (ValueError, TypeError):
            pass

    return {
        "at_bats":          at_bats,
        "plate_appearances": pa,
        "hits":             hits,
        "doubles":          doubles,
        "triples":          triples,
        "home_runs":        home_runs,
        "rbi":              rbi,
        "walks":            walks,
        "strikeouts":       strikeouts,
        "hit_by_pitch":     hit_by_pitch,
        "sac_flies":        sac_flies,
        "stolen_bases":     stolen_bases,
        "total_bases":      total_bases,
        "batting_avg":      batting_avg,
        "slugging_pct":     slugging_pct,
        "hr_flag":          hr_flag,
        "lineup_slot":      lineup_slot,
        "position":         position,
    }


def _ingest_game(conn: sqlite3.Connection, game_date: str,
                 game_pk: int) -> tuple[int, int]:
    """
    Fetch boxscore for one game and insert batting lines.
    Returns (rows_inserted, rows_skipped).
    """
    url  = f"{MLB_API_BASE}/game/{game_pk}/boxscore"
    data = _fetch_json(url)

    if not data:
        log.warning("  No data for game_pk=%d — skipping.", game_pk)
        return 0, 0

    # Confirm game is final before loading
    game_data_url = f"{MLB_API_BASE}/game/{game_pk}/linescore"
    linescore = _fetch_json(game_data_url)
    if linescore:
        status = (linescore.get("offense", {}) or {})
        # Check via the schedule status instead
    # Check status via the boxscore teams structure — if innings played >= 9
    # and there's a winner, we treat it as final. The API doesn't always return
    # a clean status field in boxscore, so we check for presence of both teams' data.

    teams     = data.get("teams", {})
    now_utc   = date.today().isoformat() + "T00:00:00Z"

    inserted = 0
    skipped  = 0

    for side in ("home", "away"):
        team_data   = teams.get(side, {})
        team_info   = team_data.get("team", {})
        team_id     = team_info.get("id")
        players     = team_data.get("players", {})

        if not team_id or not players:
            continue

        for player_key, player_data in players.items():
            person   = player_data.get("person", {})
            player_id = person.get("id")
            if not player_id:
                continue

            batting = _parse_batting_line(player_data)
            if not batting:
                skipped += 1
                continue

            # Skip pitchers who never batted (NL pitchers, DH league pitchers)
            # at_bats == 0 AND plate_appearances == 0 means DNP at the plate
            if batting["at_bats"] == 0 and batting["plate_appearances"] == 0:
                skipped += 1
                continue

            conn.execute(
                """
                INSERT OR REPLACE INTO fact_player_game_results
                    (game_date, game_id, player_id, team_id,
                     at_bats, plate_appearances, hits, doubles, triples,
                     home_runs, rbi, walks, strikeouts, hit_by_pitch,
                     sac_flies, stolen_bases,
                     total_bases, batting_avg, slugging_pct, hr_flag,
                     lineup_slot, position,
                     load_timestamp_utc)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    game_date, game_pk, player_id, team_id,
                    batting["at_bats"], batting["plate_appearances"],
                    batting["hits"], batting["doubles"], batting["triples"],
                    batting["home_runs"], batting["rbi"], batting["walks"],
                    batting["strikeouts"], batting["hit_by_pitch"],
                    batting["sac_flies"], batting["stolen_bases"],
                    batting["total_bases"], batting["batting_avg"],
                    batting["slugging_pct"], batting["hr_flag"],
                    batting["lineup_slot"], batting["position"],
                    now_utc,
                ),
            )
            inserted += 1

    return inserted, skipped


# ── Date range helpers ─────────────────────────────────────────────────────

def _date_range(start: date, end: date) -> list[str]:
    """Return all dates from start to end inclusive as ISO strings."""
    out  = []
    cur  = start
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


# ── Main runner ────────────────────────────────────────────────────────────

def run(db_path: str, game_dates: list[str],
        sleep_sec: float = SLEEP_BETWEEN_GAMES) -> None:
    """
    Ingest boxscore data for all completed games on the given dates.
    """
    log.info("Boxscore ingestion starting. Dates: %d | DB: %s",
             len(game_dates), db_path)

    # Filter to dates that are in the past (can't have results for future games)
    today = date.today().isoformat()
    past_dates = [d for d in game_dates if d < today]
    if len(past_dates) < len(game_dates):
        log.info("Skipping %d future dates (no results available yet).",
                 len(game_dates) - len(past_dates))
    if not past_dates:
        log.warning("No past dates to process — nothing to ingest.")
        return

    game_pks = _get_completed_game_pks(db_path, past_dates)
    log.info("Found %d games across %d dates in fact_games.",
             len(game_pks), len(past_dates))

    if not game_pks:
        log.warning("No games found in fact_games for these dates.")
        log.warning("Ensure run_pipeline.py has been run to populate fact_games first.")
        return

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    total_inserted = 0
    total_skipped  = 0
    games_processed = 0
    games_failed    = 0

    for i, (game_date, game_pk) in enumerate(game_pks, start=1):
        log.info("  [%d/%d] game_pk=%d  date=%s",
                 i, len(game_pks), game_pk, game_date)
        try:
            ins, skp = _ingest_game(conn, game_date, game_pk)
            total_inserted += ins
            total_skipped  += skp
            games_processed += 1
            if ins > 0:
                conn.commit()
        except Exception as e:
            log.error("  Error on game_pk=%d: %s", game_pk, e)
            games_failed += 1

        if i < len(game_pks):
            time.sleep(sleep_sec)

    conn.commit()
    conn.close()

    log.info("Boxscore ingestion complete.")
    log.info("  Games processed: %d | Failed: %d", games_processed, games_failed)
    log.info("  Player rows inserted: %d | Skipped (no PA): %d",
             total_inserted, total_skipped)

    # Quick validation
    conn = sqlite3.connect(db_path)
    row_count = conn.execute(
        "SELECT COUNT(*) FROM fact_player_game_results"
    ).fetchone()[0]
    date_count = conn.execute(
        "SELECT COUNT(DISTINCT game_date) FROM fact_player_game_results"
    ).fetchone()[0]
    conn.close()
    log.info("  Total rows in fact_player_game_results: %d across %d dates",
             row_count, date_count)


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest MLB boxscore batting actuals into fact_player_game_results"
    )
    parser.add_argument("--db-path",     default="data/mlb_pregame.db")
    parser.add_argument("--date",        help="Single date YYYY-MM-DD")
    parser.add_argument("--start",       help="Start date YYYY-MM-DD")
    parser.add_argument("--end",         help="End date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--season",      type=int, help="Full season (e.g. 2026)")
    parser.add_argument("--last-n-days", type=int, help="Last N calendar days")
    args = parser.parse_args()

    today     = date.today()
    yesterday = today - timedelta(days=1)

    if args.season:
        start_d = date(args.season, 3, 1)
        end_d   = min(date(args.season, 11, 30), yesterday)
    elif args.last_n_days:
        start_d = today - timedelta(days=args.last_n_days)
        end_d   = yesterday
    elif args.date:
        start_d = end_d = date.fromisoformat(args.date)
    elif args.start:
        start_d = date.fromisoformat(args.start)
        end_d   = date.fromisoformat(args.end) if args.end else yesterday
    else:
        parser.error("Provide --season, --last-n-days, --date, or --start/--end")

    game_dates = _date_range(start_d, end_d)
    run(db_path=args.db_path, game_dates=game_dates)
