"""
ingest_mlb_statsapi.py
----------------------
Pulls schedule, probable starters, lineups, teams, venues, and player
roster data from the free MLB Stats API and writes to the SQLite database.

Free endpoint docs:
    https://github.com/toddrob99/MLB-StatsAPI/wiki/Endpoints

Usage:
    python ingest/ingest_mlb_statsapi.py --date 2025-04-15 --db-path data/mlb_pregame.db
    python ingest/ingest_mlb_statsapi.py --today             --db-path data/mlb_pregame.db
"""

import sqlite3
import json
import time
import logging
import argparse
from datetime import date, datetime, timezone
from pathlib import Path

import urllib.request
import urllib.error

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
                raise
        except Exception as e:
            log.warning("Request error %s (attempt %d)", e, attempt)
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                raise
    return {}


# ── Teams & Venues ─────────────────────────────────────────────────────────

def ingest_teams_and_venues(conn: sqlite3.Connection) -> None:
    log.info("Fetching teams and venues...")
    data = _get(f"{MLB_BASE}/teams", {"sportId": "1", "season": str(date.today().year)})
    venues_inserted = set()

    for team in data.get("teams", []):
        # Venue
        venue = team.get("venue", {})
        venue_id = venue.get("id")
        if venue_id and venue_id not in venues_inserted:
            # Fetch venue detail for coordinates
            try:
                vdata = _get(f"{MLB_BASE}/venues/{venue_id}", {"hydrate": "location"})
                v = vdata.get("venues", [{}])[0]
                loc = v.get("location", {})
                conn.execute(
                    """
                    INSERT OR IGNORE INTO dim_venues
                        (venue_id, venue_name, city, state, time_zone_name,
                         roof_type, surface_type, lat, lon, altitude_ft)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        venue_id,
                        v.get("name"),
                        loc.get("city"),
                        loc.get("stateAbbrev"),
                        v.get("timeZone", {}).get("id"),
                        None,  # roof_type — curated manually or via separate source
                        None,  # surface_type — curated manually
                        loc.get("defaultCoordinates", {}).get("latitude"),
                        loc.get("defaultCoordinates", {}).get("longitude"),
                        None,  # altitude_ft — curated manually
                    ),
                )
                venues_inserted.add(venue_id)
                time.sleep(0.15)
            except Exception as e:
                log.warning("Could not fetch venue %s: %s", venue_id, e)

        # Team
        conn.execute(
            """
            INSERT OR REPLACE INTO dim_teams
                (team_id, team_name, team_abbr, league_name, division_name,
                 venue_id, active_flag)
            VALUES (?,?,?,?,?,?,?)
            """,
            (
                team.get("id"),
                team.get("name"),
                team.get("abbreviation"),
                team.get("league", {}).get("name"),
                team.get("division", {}).get("name"),
                venue_id,
                1 if team.get("active") else 0,
            ),
        )
    conn.commit()
    log.info("Teams/venues done.")


# ── Players ────────────────────────────────────────────────────────────────

def ingest_roster(conn: sqlite3.Connection, team_id: int) -> None:
    """Fetches 40-man roster for one team and upserts dim_players."""
    data = _get(f"{MLB_BASE}/teams/{team_id}/roster", {"rosterType": "40Man"})
    for entry in data.get("roster", []):
        p = entry.get("person", {})
        pid = p.get("id")
        if not pid:
            continue
        # Fetch full player detail for handedness
        try:
            pdata = _get(f"{MLB_BASE}/people/{pid}")
            player = pdata.get("people", [{}])[0]
            conn.execute(
                """
                INSERT OR REPLACE INTO dim_players
                    (player_id, full_name, first_name, last_name, birth_date,
                     bats, throws, primary_position, current_team_id,
                     active_flag, mlb_debut_date)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    pid,
                    player.get("fullName"),
                    player.get("firstName"),
                    player.get("lastName"),
                    player.get("birthDate"),
                    player.get("batSide", {}).get("code"),
                    player.get("pitchHand", {}).get("code"),
                    player.get("primaryPosition", {}).get("abbreviation"),
                    team_id,
                    1 if player.get("active") else 0,
                    player.get("mlbDebutDate"),
                ),
            )
            time.sleep(0.1)
        except Exception as e:
            log.warning("Could not fetch player %s: %s", pid, e)
    conn.commit()


def ingest_all_rosters(conn: sqlite3.Connection) -> None:
    log.info("Fetching rosters for all active teams...")
    teams = conn.execute(
        "SELECT team_id FROM dim_teams WHERE active_flag=1"
    ).fetchall()
    for (tid,) in teams:
        log.info("  Roster: team_id=%s", tid)
        ingest_roster(conn, tid)
        time.sleep(0.3)
    log.info("Rosters done.")


# ── Schedule & Probable Starters ───────────────────────────────────────────

def ingest_schedule(conn: sqlite3.Connection, game_date: str, as_of_date: str) -> list[int]:
    """
    Pulls the schedule for game_date, writes stg_mlb_schedule_games and
    fact_games, returns list of game_ids found.
    """
    conn.execute("PRAGMA foreign_keys=OFF;")
    log.info("Fetching schedule for %s (as_of=%s)...", game_date, as_of_date)
    data = _get(
        f"{MLB_BASE}/schedule",
        {
            "sportId": "1",
            "date": game_date,
            "hydrate": "probablePitcher,lineupType,team",
        },
    )
    now_utc = datetime.now(timezone.utc).isoformat()
    game_ids = []

    for day in data.get("dates", []):
        for game in day.get("games", []):
            gid    = game.get("gamePk")
            if not gid:
                continue
            game_ids.append(gid)
            home   = game.get("teams", {}).get("home", {})
            away   = game.get("teams", {}).get("away", {})
            hp_pit = home.get("probablePitcher", {}).get("id")
            ap_pit = away.get("probablePitcher", {}).get("id")
            status = game.get("status", {}).get("statusCode")
            season = game.get("season")
            gdt    = game.get("gameDate")  # ISO UTC string

            conn.execute(
                """
                INSERT OR REPLACE INTO stg_mlb_schedule_games
                    (as_of_date, game_id, season, game_date, game_datetime_utc,
                     home_team_id, away_team_id, venue_id, day_night,
                     doubleheader_flag, scheduled_innings,
                     home_probable_pitcher_id, away_probable_pitcher_id,
                     status_code, raw_payload_json, load_timestamp_utc)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    as_of_date,
                    gid,
                    int(season) if season else None,
                    game_date,
                    gdt,
                    home.get("team", {}).get("id"),
                    away.get("team", {}).get("id"),
                    game.get("venue", {}).get("id"),
                    game.get("dayNight"),
                    1 if game.get("doubleHeader") in ("Y", "S") else 0,
                    game.get("scheduledInnings", 9),
                    hp_pit,
                    ap_pit,
                    status,
                    json.dumps(game),
                    now_utc,
                ),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO fact_games
                    (as_of_date, game_id, season, game_date, game_datetime_utc,
                     home_team_id, away_team_id, venue_id, day_night,
                     doubleheader_flag, scheduled_innings,
                     home_probable_pitcher_id, away_probable_pitcher_id,
                     confirmed_home_lineup_flag, confirmed_away_lineup_flag)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    as_of_date,
                    gid,
                    int(season) if season else None,
                    game_date,
                    gdt,
                    home.get("team", {}).get("id"),
                    away.get("team", {}).get("id"),
                    game.get("venue", {}).get("id"),
                    game.get("dayNight"),
                    1 if game.get("doubleHeader") in ("Y", "S") else 0,
                    game.get("scheduledInnings", 9),
                    hp_pit,
                    ap_pit,
                    0, 0,  # confirmed flags default to 0 until lineups confirmed
                ),
            )
    conn.commit()
    log.info("Schedule: %d games found.", len(game_ids))
    return game_ids


# ── Lineups ────────────────────────────────────────────────────────────────

def ingest_lineups(conn: sqlite3.Connection, game_id: int, as_of_date: str) -> None:
    """
    Fetches lineups (confirmed or projected) for a single game.
    MLB Stats API supplies confirmed lineups once released; before that
    the probable starters serve as a proxy — this function handles both.
    """
    try:
        data = _get(
            f"{MLB_BASE}/game/{game_id}/linescore",
        )
    except Exception:
        data = {}

    # Fetch boxscore for lineup slots
    try:
        bs = _get(f"{MLB_BASE}/game/{game_id}/boxscore")
    except Exception:
        bs = {}

    teams_data = bs.get("teams", {})

    # Pull game row for probable pitchers & opponent pitcher lookup
    row = conn.execute(
        "SELECT home_team_id, away_team_id, home_probable_pitcher_id, "
        "away_probable_pitcher_id FROM fact_games "
        "WHERE as_of_date=? AND game_id=?",
        (as_of_date, game_id),
    ).fetchone()
    if not row:
        return
    home_tid, away_tid, home_pp, away_pp = row

    for side, team_id, opp_pid in [
        ("home", home_tid, away_pp),
        ("away", away_tid, home_pp),
    ]:
        side_data = teams_data.get(side, {})
        batters   = side_data.get("batters", [])
        lineup    = side_data.get("battingOrder", [])
        confirmed = 1 if batters else 0

        # Resolve pitcher hand from dim_players
        opp_hand = None
        if opp_pid:
            res = conn.execute(
                "SELECT throws FROM dim_players WHERE player_id=?", (opp_pid,)
            ).fetchone()
            opp_hand = res[0] if res else None

        for slot_idx, pid in enumerate(lineup or batters, start=1):
            if not pid:
                continue
            conn.execute(
                """
                INSERT OR REPLACE INTO fact_game_lineups
                    (as_of_date, game_id, team_id, player_id, lineup_slot,
                     batting_order, starter_flag, confirmed_flag, projected_flag,
                     opponent_pitcher_id, opponent_pitcher_hand)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    as_of_date, game_id, team_id, pid,
                    slot_idx,
                    str(slot_idx * 100),
                    1,
                    confirmed,
                    1 - confirmed,
                    opp_pid,
                    opp_hand,
                ),
            )

        # Update confirmed flags on fact_games
        col = "confirmed_home_lineup_flag" if side == "home" else "confirmed_away_lineup_flag"
        conn.execute(
            f"UPDATE fact_games SET {col}=? WHERE as_of_date=? AND game_id=?",
            (confirmed, as_of_date, game_id),
        )

    conn.commit()


# ── Main ───────────────────────────────────────────────────────────────────

def run(db_path: str, game_date: str, as_of_date: str,
        seed_dimensions: bool = False) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")
    conn.execute("PRAGMA journal_mode=WAL;")

    if seed_dimensions:
        ingest_teams_and_venues(conn)
        ingest_all_rosters(conn)

    game_ids = ingest_schedule(conn, game_date, as_of_date)
    for gid in game_ids:
        log.info("  Lineups: game_id=%s", gid)
        ingest_lineups(conn, gid, as_of_date)
        time.sleep(0.2)

    conn.close()
    log.info("MLB Stats API ingestion complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path",  default="data/mlb_pregame.db")
    parser.add_argument("--date",     help="Game date YYYY-MM-DD")
    parser.add_argument("--today",    action="store_true", help="Use today's date")
    parser.add_argument(
        "--seed-dimensions",
        action="store_true",
        help="Also refresh teams, venues, and rosters (run once at season start)",
    )
    args = parser.parse_args()

    if args.today:
        gdate = date.today().isoformat()
    elif args.date:
        gdate = args.date
    else:
        parser.error("Provide --date YYYY-MM-DD or --today")

    run(
        db_path=args.db_path,
        game_date=gdate,
        as_of_date=gdate,
        seed_dimensions=args.seed_dimensions,
    )
