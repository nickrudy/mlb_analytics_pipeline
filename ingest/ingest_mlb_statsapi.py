"""
ingest_mlb_statsapi.py
----------------------
Pulls schedule, probable starters, lineups, teams, venues, and player
roster data from the free MLB Stats API and writes to the database.

Works with both SQLite (local) and Supabase (cloud) via utils/db.py.

Free endpoint docs:
    https://github.com/toddrob99/MLB-StatsAPI/wiki/Endpoints

Usage:
    python ingest/ingest_mlb_statsapi.py --date 2025-04-15
    python ingest/ingest_mlb_statsapi.py --today
    python ingest/ingest_mlb_statsapi.py --today --seed-dimensions
"""
import json
import time
import logging
import argparse
from datetime import date, datetime, timezone

import urllib.request
import urllib.error

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
                raise
        except Exception as e:
            log.warning("Request error %s (attempt %d)", e, attempt)
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                raise
    return {}


# ── SQL helpers ────────────────────────────────────────────────────────────
# These emit the correct upsert syntax for each backend.

def _upsert_venue_sql():
    if DB_BACKEND == "supabase":
        return """
            INSERT INTO dim_venues
                (venue_id, venue_name, city, state, time_zone_name,
                 roof_type, surface_type, lat, lon, altitude_ft)
            VALUES
                (:venue_id,:venue_name,:city,:state,:time_zone_name,
                 :roof_type,:surface_type,:lat,:lon,:altitude_ft)
            ON CONFLICT (venue_id) DO NOTHING
        """
    return """
        INSERT OR IGNORE INTO dim_venues
            (venue_id, venue_name, city, state, time_zone_name,
             roof_type, surface_type, lat, lon, altitude_ft)
        VALUES
            (:venue_id,:venue_name,:city,:state,:time_zone_name,
             :roof_type,:surface_type,:lat,:lon,:altitude_ft)
    """

def _upsert_team_sql():
    if DB_BACKEND == "supabase":
        return """
            INSERT INTO dim_teams
                (team_id, team_name, team_abbr, league_name, division_name,
                 venue_id, active_flag)
            VALUES (:team_id,:team_name,:team_abbr,:league_name,:division_name,
                    :venue_id,:active_flag)
            ON CONFLICT (team_id) DO UPDATE SET
                team_name    = EXCLUDED.team_name,
                team_abbr    = EXCLUDED.team_abbr,
                league_name  = EXCLUDED.league_name,
                division_name= EXCLUDED.division_name,
                venue_id     = EXCLUDED.venue_id,
                active_flag  = EXCLUDED.active_flag
        """
    return """
        INSERT OR REPLACE INTO dim_teams
            (team_id, team_name, team_abbr, league_name, division_name,
             venue_id, active_flag)
        VALUES (:team_id,:team_name,:team_abbr,:league_name,:division_name,
                :venue_id,:active_flag)
    """

def _upsert_player_sql():
    if DB_BACKEND == "supabase":
        return """
            INSERT INTO dim_players
                (player_id, full_name, first_name, last_name, birth_date,
                 bats, throws, primary_position, current_team_id,
                 active_flag, mlb_debut_date)
            VALUES (:player_id,:full_name,:first_name,:last_name,:birth_date,
                    :bats,:throws,:primary_position,:current_team_id,
                    :active_flag,:mlb_debut_date)
            ON CONFLICT (player_id) DO UPDATE SET
                full_name        = EXCLUDED.full_name,
                first_name       = EXCLUDED.first_name,
                last_name        = EXCLUDED.last_name,
                bats             = EXCLUDED.bats,
                throws           = EXCLUDED.throws,
                primary_position = EXCLUDED.primary_position,
                current_team_id  = EXCLUDED.current_team_id,
                active_flag      = EXCLUDED.active_flag,
                mlb_debut_date   = EXCLUDED.mlb_debut_date
        """
    return """
        INSERT OR REPLACE INTO dim_players
            (player_id, full_name, first_name, last_name, birth_date,
             bats, throws, primary_position, current_team_id,
             active_flag, mlb_debut_date)
        VALUES (:player_id,:full_name,:first_name,:last_name,:birth_date,
                :bats,:throws,:primary_position,:current_team_id,
                :active_flag,:mlb_debut_date)
    """

def _upsert_schedule_sql():
    if DB_BACKEND == "supabase":
        return """
            INSERT INTO stg_mlb_schedule_games
                (as_of_date, game_id, season, game_date, game_datetime_utc,
                 home_team_id, away_team_id, venue_id, day_night,
                 doubleheader_flag, scheduled_innings,
                 home_probable_pitcher_id, away_probable_pitcher_id,
                 status_code, raw_payload_json, load_timestamp_utc)
            VALUES
                (:as_of_date,:game_id,:season,:game_date,:game_datetime_utc,
                 :home_team_id,:away_team_id,:venue_id,:day_night,
                 :doubleheader_flag,:scheduled_innings,
                 :home_probable_pitcher_id,:away_probable_pitcher_id,
                 :status_code,:raw_payload_json,:load_timestamp_utc)
            ON CONFLICT (as_of_date, game_id) DO UPDATE SET
                season                   = EXCLUDED.season,
                game_datetime_utc        = EXCLUDED.game_datetime_utc,
                home_probable_pitcher_id = EXCLUDED.home_probable_pitcher_id,
                away_probable_pitcher_id = EXCLUDED.away_probable_pitcher_id,
                status_code              = EXCLUDED.status_code,
                raw_payload_json         = EXCLUDED.raw_payload_json,
                load_timestamp_utc       = EXCLUDED.load_timestamp_utc
        """
    return """
        INSERT OR REPLACE INTO stg_mlb_schedule_games
            (as_of_date, game_id, season, game_date, game_datetime_utc,
             home_team_id, away_team_id, venue_id, day_night,
             doubleheader_flag, scheduled_innings,
             home_probable_pitcher_id, away_probable_pitcher_id,
             status_code, raw_payload_json, load_timestamp_utc)
        VALUES
            (:as_of_date,:game_id,:season,:game_date,:game_datetime_utc,
             :home_team_id,:away_team_id,:venue_id,:day_night,
             :doubleheader_flag,:scheduled_innings,
             :home_probable_pitcher_id,:away_probable_pitcher_id,
             :status_code,:raw_payload_json,:load_timestamp_utc)
    """

def _upsert_fact_games_sql():
    if DB_BACKEND == "supabase":
        return """
            INSERT INTO fact_games
                (as_of_date, game_id, season, game_date, game_datetime_utc,
                 home_team_id, away_team_id, venue_id, day_night,
                 doubleheader_flag, scheduled_innings,
                 home_probable_pitcher_id, away_probable_pitcher_id,
                 confirmed_home_lineup_flag, confirmed_away_lineup_flag)
            VALUES
                (:as_of_date,:game_id,:season,:game_date,:game_datetime_utc,
                 :home_team_id,:away_team_id,:venue_id,:day_night,
                 :doubleheader_flag,:scheduled_innings,
                 :home_probable_pitcher_id,:away_probable_pitcher_id,
                 :confirmed_home_lineup_flag,:confirmed_away_lineup_flag)
            ON CONFLICT (as_of_date, game_id) DO UPDATE SET
                season                   = EXCLUDED.season,
                game_datetime_utc        = EXCLUDED.game_datetime_utc,
                home_probable_pitcher_id = EXCLUDED.home_probable_pitcher_id,
                away_probable_pitcher_id = EXCLUDED.away_probable_pitcher_id
        """
    return """
        INSERT OR REPLACE INTO fact_games
            (as_of_date, game_id, season, game_date, game_datetime_utc,
             home_team_id, away_team_id, venue_id, day_night,
             doubleheader_flag, scheduled_innings,
             home_probable_pitcher_id, away_probable_pitcher_id,
             confirmed_home_lineup_flag, confirmed_away_lineup_flag)
        VALUES
            (:as_of_date,:game_id,:season,:game_date,:game_datetime_utc,
             :home_team_id,:away_team_id,:venue_id,:day_night,
             :doubleheader_flag,:scheduled_innings,
             :home_probable_pitcher_id,:away_probable_pitcher_id,
             :confirmed_home_lineup_flag,:confirmed_away_lineup_flag)
    """

def _upsert_lineup_sql():
    if DB_BACKEND == "supabase":
        return """
            INSERT INTO fact_game_lineups
                (as_of_date, game_id, team_id, player_id, lineup_slot,
                 batting_order, starter_flag, confirmed_flag, projected_flag,
                 opponent_pitcher_id, opponent_pitcher_hand)
            VALUES
                (:as_of_date,:game_id,:team_id,:player_id,:lineup_slot,
                 :batting_order,:starter_flag,:confirmed_flag,:projected_flag,
                 :opponent_pitcher_id,:opponent_pitcher_hand)
            ON CONFLICT (as_of_date, game_id, team_id, player_id) DO UPDATE SET
                lineup_slot           = EXCLUDED.lineup_slot,
                batting_order         = EXCLUDED.batting_order,
                confirmed_flag        = EXCLUDED.confirmed_flag,
                projected_flag        = EXCLUDED.projected_flag,
                opponent_pitcher_id   = EXCLUDED.opponent_pitcher_id,
                opponent_pitcher_hand = EXCLUDED.opponent_pitcher_hand
        """
    return """
        INSERT OR REPLACE INTO fact_game_lineups
            (as_of_date, game_id, team_id, player_id, lineup_slot,
             batting_order, starter_flag, confirmed_flag, projected_flag,
             opponent_pitcher_id, opponent_pitcher_hand)
        VALUES
            (:as_of_date,:game_id,:team_id,:player_id,:lineup_slot,
             :batting_order,:starter_flag,:confirmed_flag,:projected_flag,
             :opponent_pitcher_id,:opponent_pitcher_hand)
    """


# ── Teams & Venues ─────────────────────────────────────────────────────────

def ingest_teams_and_venues(conn) -> None:
    log.info("Fetching teams and venues...")
    data = _get(f"{MLB_BASE}/teams", {"sportId": "1", "season": str(date.today().year)})
    venues_inserted = set()
    for team in data.get("teams", []):
        venue = team.get("venue", {})
        venue_id = venue.get("id")
        if venue_id and venue_id not in venues_inserted:
            try:
                vdata = _get(f"{MLB_BASE}/venues/{venue_id}", {"hydrate": "location"})
                v   = vdata.get("venues", [{}])[0]
                loc = v.get("location", {})
                conn.execute(_upsert_venue_sql(), {
                    "venue_id":       venue_id,
                    "venue_name":     v.get("name"),
                    "city":           loc.get("city"),
                    "state":          loc.get("stateAbbrev"),
                    "time_zone_name": v.get("timeZone", {}).get("id"),
                    "roof_type":      None,
                    "surface_type":   None,
                    "lat":            loc.get("defaultCoordinates", {}).get("latitude"),
                    "lon":            loc.get("defaultCoordinates", {}).get("longitude"),
                    "altitude_ft":    None,
                })
                venues_inserted.add(venue_id)
                time.sleep(0.15)
            except Exception as e:
                log.warning("Could not fetch venue %s: %s", venue_id, e)

        conn.execute(_upsert_team_sql(), {
            "team_id":      team.get("id"),
            "team_name":    team.get("name"),
            "team_abbr":    team.get("abbreviation"),
            "league_name":  team.get("league", {}).get("name"),
            "division_name":team.get("division", {}).get("name"),
            "venue_id":     venue_id,
            "active_flag":  1 if team.get("active") else 0,
        })
    conn.commit()
    log.info("Teams/venues done.")


# ── Players ────────────────────────────────────────────────────────────────

def ingest_roster(conn, team_id: int) -> None:
    data = _get(f"{MLB_BASE}/teams/{team_id}/roster", {"rosterType": "40Man"})
    for entry in data.get("roster", []):
        p   = entry.get("person", {})
        pid = p.get("id")
        if not pid:
            continue
        try:
            pdata  = _get(f"{MLB_BASE}/people/{pid}")
            player = pdata.get("people", [{}])[0]
            conn.execute(_upsert_player_sql(), {
                "player_id":        pid,
                "full_name":        player.get("fullName"),
                "first_name":       player.get("firstName"),
                "last_name":        player.get("lastName"),
                "birth_date":       player.get("birthDate"),
                "bats":             player.get("batSide", {}).get("code"),
                "throws":           player.get("pitchHand", {}).get("code"),
                "primary_position": player.get("primaryPosition", {}).get("abbreviation"),
                "current_team_id":  team_id,
                "active_flag":      1 if player.get("active") else 0,
                "mlb_debut_date":   player.get("mlbDebutDate"),
            })
            time.sleep(0.1)
        except Exception as e:
            log.warning("Could not fetch player %s: %s", pid, e)
    conn.commit()


def ingest_all_rosters(conn) -> None:
    log.info("Fetching rosters for all active teams...")
    cur = conn.cursor()
    cur.execute("SELECT team_id FROM dim_teams WHERE active_flag=1")
    teams = cur.fetchall()
    for (tid,) in teams:
        log.info("  Roster: team_id=%s", tid)
        ingest_roster(conn, tid)
        time.sleep(0.3)
    log.info("Rosters done.")


# ── Schedule & Probable Starters ───────────────────────────────────────────

def ingest_schedule(conn, game_date: str, as_of_date: str) -> list:
    log.info("Fetching schedule for %s (as_of=%s)...", game_date, as_of_date)
    data = _get(
        f"{MLB_BASE}/schedule",
        {
            "sportId": "1",
            "date":    game_date,
            "hydrate": "probablePitcher,lineupType,team",
        },
    )
    now_utc  = datetime.now(timezone.utc).isoformat()
    game_ids = []
    for day in data.get("dates", []):
        for game in day.get("games", []):
            gid = game.get("gamePk")
            if not gid:
                continue
            game_ids.append(gid)
            home   = game.get("teams", {}).get("home", {})
            away   = game.get("teams", {}).get("away", {})
            hp_pit = home.get("probablePitcher", {}).get("id")
            ap_pit = away.get("probablePitcher", {}).get("id")
            status = game.get("status", {}).get("statusCode")
            season = game.get("season")
            gdt    = game.get("gameDate")

            conn.execute(_upsert_schedule_sql(), {
                "as_of_date":               as_of_date,
                "game_id":                  gid,
                "season":                   int(season) if season else None,
                "game_date":                game_date,
                "game_datetime_utc":        gdt,
                "home_team_id":             home.get("team", {}).get("id"),
                "away_team_id":             away.get("team", {}).get("id"),
                "venue_id":                 game.get("venue", {}).get("id"),
                "day_night":                game.get("dayNight"),
                "doubleheader_flag":        1 if game.get("doubleHeader") in ("Y","S") else 0,
                "scheduled_innings":        game.get("scheduledInnings", 9),
                "home_probable_pitcher_id": hp_pit,
                "away_probable_pitcher_id": ap_pit,
                "status_code":              status,
                "raw_payload_json":         json.dumps(game),
                "load_timestamp_utc":       now_utc,
            })
            conn.execute(_upsert_fact_games_sql(), {
                "as_of_date":               as_of_date,
                "game_id":                  gid,
                "season":                   int(season) if season else None,
                "game_date":                game_date,
                "game_datetime_utc":        gdt,
                "home_team_id":             home.get("team", {}).get("id"),
                "away_team_id":             away.get("team", {}).get("id"),
                "venue_id":                 game.get("venue", {}).get("id"),
                "day_night":                game.get("dayNight"),
                "doubleheader_flag":        1 if game.get("doubleHeader") in ("Y","S") else 0,
                "scheduled_innings":        game.get("scheduledInnings", 9),
                "home_probable_pitcher_id": hp_pit,
                "away_probable_pitcher_id": ap_pit,
                "confirmed_home_lineup_flag": 0,
                "confirmed_away_lineup_flag": 0,
            })
    conn.commit()
    log.info("Schedule: %d games found.", len(game_ids))
    return game_ids


# ── Lineups ────────────────────────────────────────────────────────────────

def ingest_lineups(conn, game_id: int, as_of_date: str) -> None:
    try:
        bs = _get(f"{MLB_BASE}/game/{game_id}/boxscore")
    except Exception:
        bs = {}

    teams_data = bs.get("teams", {})

    cur = conn.cursor()
    cur.execute(
        "SELECT home_team_id, away_team_id, home_probable_pitcher_id, "
        "away_probable_pitcher_id FROM fact_games "
        "WHERE as_of_date=:aod AND game_id=:gid",
        {"aod": as_of_date, "gid": game_id},
    )
    row = cur.fetchone()
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

        opp_hand = None
        if opp_pid:
            cur.execute(
                "SELECT throws FROM dim_players WHERE player_id=:pid",
                {"pid": opp_pid},
            )
            res = cur.fetchone()
            opp_hand = res[0] if res else None

        for slot_idx, pid in enumerate(lineup or batters, start=1):
            if not pid:
                continue
            conn.execute(_upsert_lineup_sql(), {
                "as_of_date":           as_of_date,
                "game_id":              game_id,
                "team_id":              team_id,
                "player_id":            pid,
                "lineup_slot":          slot_idx,
                "batting_order":        str(slot_idx * 100),
                "starter_flag":         1,
                "confirmed_flag":       confirmed,
                "projected_flag":       1 - confirmed,
                "opponent_pitcher_id":  opp_pid,
                "opponent_pitcher_hand":opp_hand,
            })

        col = (
            "confirmed_home_lineup_flag"
            if side == "home"
            else "confirmed_away_lineup_flag"
        )
        conn.execute(
            f"UPDATE fact_games SET {col}=:val WHERE as_of_date=:aod AND game_id=:gid",
            {"val": confirmed, "aod": as_of_date, "gid": game_id},
        )
    conn.commit()


# ── Main ───────────────────────────────────────────────────────────────────

def run(game_date: str, as_of_date: str, seed_dimensions: bool = False) -> None:
    with get_connection() as conn:
        # SQLite-only PRAGMAs — skip on Supabase
        if DB_BACKEND == "sqlite":
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

    log.info("MLB Stats API ingestion complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",  help="Game date YYYY-MM-DD")
    parser.add_argument("--today", action="store_true", help="Use today's date")
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

    run(game_date=gdate, as_of_date=gdate, seed_dimensions=args.seed_dimensions)