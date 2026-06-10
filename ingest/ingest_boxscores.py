"""
ingest_boxscores.py
--------------------
Pulls per-player batting line actuals from completed games via the
MLB Stats API and loads them into fact_player_game_results.

Works with both SQLite (local) and Supabase (cloud) via utils/db.py.

Usage:
    python ingest/ingest_boxscores.py --season 2026
    python ingest/ingest_boxscores.py --date 2026-04-01
    python ingest/ingest_boxscores.py --start 2026-04-01 --end 2026-05-20
    python ingest/ingest_boxscores.py --last-n-days 7
"""
import json
import time
import logging
import argparse
import urllib.request
import urllib.error
from datetime import date, timedelta
from pathlib import Path

from utils.db import get_connection, DB_BACKEND

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

MLB_API_BASE        = "https://statsapi.mlb.com/api/v1"
SLEEP_BETWEEN_GAMES = 0.4


# ── API helpers ────────────────────────────────────────────────────────────

def _fetch_json(url: str, retries: int = 3):
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            log.warning("HTTP %d fetching %s (attempt %d/%d)", e.code, url, attempt+1, retries)
        except Exception as e:
            log.warning("Error fetching %s: %s (attempt %d/%d)", url, e, attempt+1, retries)
        if attempt < retries - 1:
            time.sleep(1.5)
    return None


# ── Game PK lookup ─────────────────────────────────────────────────────────
# Receives an open conn so it works with both SQLite and Supabase

def _get_completed_game_pks(conn, game_dates: list) -> list:
    placeholders = ",".join(f":d{i}" for i in range(len(game_dates)))
    params       = {f"d{i}": d for i, d in enumerate(game_dates)}
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT game_date, game_id
        FROM   fact_games
        WHERE  game_date IN ({placeholders})
        ORDER  BY game_date, game_id
        """,
        params,
    )
    return [(r[0], r[1]) for r in cur.fetchall()]


# ── Boxscore parsing ───────────────────────────────────────────────────────

def _parse_batting_line(player_data: dict):
    stats = player_data.get("stats", {}).get("batting", {})
    if not stats:
        return None
    at_bats      = stats.get("atBats", 0) or 0
    hits         = stats.get("hits", 0) or 0
    doubles      = stats.get("doubles", 0) or 0
    triples      = stats.get("triples", 0) or 0
    home_runs    = stats.get("homeRuns", 0) or 0
    walks        = stats.get("baseOnBalls", 0) or 0
    strikeouts   = stats.get("strikeOuts", 0) or 0
    hit_by_pitch = stats.get("hitByPitch", 0) or 0
    sac_flies    = stats.get("sacFlies", 0) or 0
    stolen_bases = stats.get("stolenBases", 0) or 0
    rbi          = stats.get("rbi", 0) or 0
    pa = stats.get("plateAppearances") or (at_bats + walks + hit_by_pitch + sac_flies)
    singles     = hits - doubles - triples - home_runs
    total_bases = singles + (2 * doubles) + (3 * triples) + (4 * home_runs)
    batting_avg  = round(hits / at_bats, 4)  if at_bats > 0 else None
    slugging_pct = round(total_bases / at_bats, 4) if at_bats > 0 else None
    all_positions = player_data.get("allPositions", [])
    position      = all_positions[0].get("abbreviation") if all_positions else None
    batting_order = player_data.get("battingOrder")
    lineup_slot   = None
    if batting_order:
        try:
            lineup_slot = int(str(batting_order).strip()) // 100
        except (ValueError, TypeError):
            pass
    return {
        "at_bats": at_bats, "plate_appearances": pa,
        "hits": hits, "doubles": doubles, "triples": triples,
        "home_runs": home_runs, "rbi": rbi, "walks": walks,
        "strikeouts": strikeouts, "hit_by_pitch": hit_by_pitch,
        "sac_flies": sac_flies, "stolen_bases": stolen_bases,
        "total_bases": total_bases, "batting_avg": batting_avg,
        "slugging_pct": slugging_pct, "hr_flag": 1 if home_runs > 0 else 0,
        "lineup_slot": lineup_slot, "position": position,
    }


# ── SQL helper ─────────────────────────────────────────────────────────────

def _upsert_result_sql():
    if DB_BACKEND == "supabase":
        return """
            INSERT INTO fact_player_game_results
                (game_date, game_id, player_id, team_id,
                 at_bats, plate_appearances, hits, doubles, triples,
                 home_runs, rbi, walks, strikeouts, hit_by_pitch,
                 sac_flies, stolen_bases,
                 total_bases, batting_avg, slugging_pct, hr_flag,
                 lineup_slot, position, load_timestamp_utc)
            VALUES
                (:gd,:gid,:pid,:tid,
                 :ab,:pa,:h,:d,:t,
                 :hr,:rbi,:bb,:k,:hbp,
                 :sf,:sb,
                 :tb,:avg,:slg,:hrf,
                 :slot,:pos,:ts)
            ON CONFLICT (game_date, game_id, player_id) DO UPDATE SET
                at_bats           = EXCLUDED.at_bats,
                plate_appearances = EXCLUDED.plate_appearances,
                hits              = EXCLUDED.hits,
                home_runs         = EXCLUDED.home_runs,
                total_bases       = EXCLUDED.total_bases,
                batting_avg       = EXCLUDED.batting_avg,
                slugging_pct      = EXCLUDED.slugging_pct,
                hr_flag           = EXCLUDED.hr_flag,
                load_timestamp_utc= EXCLUDED.load_timestamp_utc
        """
    return """
        INSERT OR REPLACE INTO fact_player_game_results
            (game_date, game_id, player_id, team_id,
             at_bats, plate_appearances, hits, doubles, triples,
             home_runs, rbi, walks, strikeouts, hit_by_pitch,
             sac_flies, stolen_bases,
             total_bases, batting_avg, slugging_pct, hr_flag,
             lineup_slot, position, load_timestamp_utc)
        VALUES
            (:gd,:gid,:pid,:tid,
             :ab,:pa,:h,:d,:t,
             :hr,:rbi,:bb,:k,:hbp,
             :sf,:sb,
             :tb,:avg,:slg,:hrf,
             :slot,:pos,:ts)
    """


# ── Game ingestion ─────────────────────────────────────────────────────────

def _ingest_game(conn, game_date: str, game_pk: int):
    url  = f"{MLB_API_BASE}/game/{game_pk}/boxscore"
    data = _fetch_json(url)
    if not data:
        log.warning("  No data for game_pk=%d — skipping.", game_pk)
        return 0, 0

    teams    = data.get("teams", {})
    now_utc  = date.today().isoformat() + "T00:00:00Z"
    inserted = skipped = 0

    for side in ("home", "away"):
        team_data = teams.get(side, {})
        team_id   = team_data.get("team", {}).get("id")
        players   = team_data.get("players", {})
        if not team_id or not players:
            continue
        for _, player_data in players.items():
            player_id = player_data.get("person", {}).get("id")
            if not player_id:
                continue
            batting = _parse_batting_line(player_data)
            if not batting or (batting["at_bats"] == 0 and batting["plate_appearances"] == 0):
                skipped += 1
                continue
            conn.execute(_upsert_result_sql(), {
                "gd": game_date, "gid": game_pk, "pid": player_id, "tid": team_id,
                "ab": batting["at_bats"], "pa": batting["plate_appearances"],
                "h": batting["hits"], "d": batting["doubles"], "t": batting["triples"],
                "hr": batting["home_runs"], "rbi": batting["rbi"],
                "bb": batting["walks"], "k": batting["strikeouts"],
                "hbp": batting["hit_by_pitch"], "sf": batting["sac_flies"],
                "sb": batting["stolen_bases"], "tb": batting["total_bases"],
                "avg": batting["batting_avg"], "slg": batting["slugging_pct"],
                "hrf": batting["hr_flag"], "slot": batting["lineup_slot"],
                "pos": batting["position"], "ts": now_utc,
            })
            inserted += 1
    return inserted, skipped


# ── Date range helper ──────────────────────────────────────────────────────

def _date_range(start: date, end: date) -> list:
    out, cur = [], start
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


# ── Main runner ────────────────────────────────────────────────────────────

def run(game_dates: list, sleep_sec: float = SLEEP_BETWEEN_GAMES) -> None:
    log.info("Boxscore ingestion starting. Dates: %d", len(game_dates))
    today      = date.today().isoformat()
    past_dates = [d for d in game_dates if d < today]
    if not past_dates:
        log.warning("No past dates to process.")
        return

    with get_connection() as conn:
        if DB_BACKEND == "sqlite":
            conn.execute("PRAGMA foreign_keys=OFF;")
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")

        game_pks = _get_completed_game_pks(conn, past_dates)
        log.info("Found %d games across %d dates.", len(game_pks), len(past_dates))
        if not game_pks:
            log.warning("No games found in fact_games. Run the pipeline first.")
            return

        total_inserted = total_skipped = games_processed = games_failed = 0

        for i, (game_date, game_pk) in enumerate(game_pks, start=1):
            log.info("  [%d/%d] game_pk=%d  date=%s", i, len(game_pks), game_pk, game_date)
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

        # Validation count
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fact_player_game_results")
        row_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT game_date) FROM fact_player_game_results")
        date_count = cur.fetchone()[0]

    log.info("Boxscore ingestion complete.")
    log.info("  Games processed: %d | Failed: %d", games_processed, games_failed)
    log.info("  Rows inserted: %d | Skipped: %d", total_inserted, total_skipped)
    log.info("  Total in fact_player_game_results: %d rows across %d dates",
             row_count, date_count)


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",        help="Single date YYYY-MM-DD")
    parser.add_argument("--start",       help="Start date YYYY-MM-DD")
    parser.add_argument("--end",         help="End date YYYY-MM-DD")
    parser.add_argument("--season",      type=int)
    parser.add_argument("--last-n-days", type=int)
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

    run(game_dates=_date_range(start_d, end_d))