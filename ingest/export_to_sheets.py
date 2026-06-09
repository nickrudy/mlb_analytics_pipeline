"""
export_to_sheets.py
-------------------
Exports today's matchup projections from SQLite to a Google Sheet,
replacing the manual CSV export workflow.

The sheet is structured identically to the CSV Tableau was reading,
so switching Tableau's data source from CSV to Google Sheets requires
no changes to any sheets or dashboards.

Reads credentials and sheet ID from .env via utils/config.py.

Usage:
    python ingest/export_to_sheets.py --today --db-path data/mlb_pregame.db
    python ingest/export_to_sheets.py --date 2026-04-27 --db-path data/mlb_pregame.db

Dependencies:
    pip install gspread python-dotenv
"""

import sqlite3
import logging
import argparse
import sys
from datetime import date
from pathlib import Path

# Add project root to path so utils.config is importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config import (
    DB_PATH,
    GOOGLE_SHEET_ID,
    GOOGLE_SHEETS_CREDENTIALS,
    DEFAULT_WINDOW,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# ── Query ──────────────────────────────────────────────────────────────────

EXPORT_QUERY = """
SELECT
    m.as_of_date,
    g.game_datetime_utc,
    t_home.team_abbr           AS home_team,
    t_away.team_abbr           AS away_team,
    t_batter.team_abbr         AS batter_team,
    p_batter.full_name         AS batter_name,
    p_batter.bats,
    p_pitcher.full_name        AS pitcher_name,
    p_pitcher.throws           AS pitcher_hand,
    m.window_code,
    m.batter_vs_hand_batting_avg   AS baseline_avg,
    m.pitch_type_match_score       AS pt_score,
    m.zone_match_score             AS zone_score,
    m.projected_batting_avg        AS final_projection,
    ROUND(m.projected_batting_avg - m.batter_vs_hand_batting_avg, 4) AS delta,
    m.park_adjustment_factor,
    m.weather_adjustment_factor,
    w.temperature_f,
    w.wind_speed_mph,
    w.wind_direction_deg,
    m.proj_at_bats_per_game,
    m.pt_slg_score,
    m.zone_slg_score,
    m.projected_slugging,
    m.projected_total_bases,
    m.projected_hr_probability,
    m.batter_barrel_rate,
    m.pitcher_barrel_rate_allowed
FROM fact_matchup_batter_pitcher m
JOIN fact_games g
    ON  g.game_id    = m.game_id
    AND g.as_of_date = m.as_of_date
JOIN dim_teams t_home    ON t_home.team_id   = g.home_team_id
JOIN dim_teams t_away    ON t_away.team_id   = g.away_team_id
JOIN dim_teams t_batter  ON t_batter.team_id = m.team_id
JOIN dim_players p_batter  ON p_batter.player_id  = m.batter_id
JOIN dim_players p_pitcher ON p_pitcher.player_id = m.pitcher_id
LEFT JOIN fact_game_weather w
    ON  w.game_id    = m.game_id
    AND w.as_of_date = m.as_of_date
WHERE m.as_of_date  = ?
  AND m.window_code = ?
ORDER BY m.projected_batting_avg DESC;
"""


# ── Data fetch ─────────────────────────────────────────────────────────────

def fetch_projections(db_path: str, as_of_date: str,
                      window_code: str = DEFAULT_WINDOW) -> tuple[list, list]:
    """
    Returns (headers, rows) for the export query.
    headers: list of column name strings
    rows:    list of lists (one per batter matchup row)
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")
    cursor = conn.execute(EXPORT_QUERY, (as_of_date, window_code))
    headers = [desc[0] for desc in cursor.description]
    rows    = [list(row) for row in cursor.fetchall()]
    conn.close()
    log.info("Fetched %d matchup rows for %s (window=%s).",
             len(rows), as_of_date, window_code)
    return headers, rows


# ── Google Sheets write ────────────────────────────────────────────────────

def write_to_sheets(headers: list, rows: list,
                    sheet_id: str, credentials_path: str) -> None:
    """
    Clears the Google Sheet and writes headers + rows.
    Uses batch update for efficiency.
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        log.error(
            "gspread or google-auth not installed.\n"
            "Run: pip install gspread google-auth"
        )
        raise

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds_path = Path(credentials_path)
    if not creds_path.exists():
        raise FileNotFoundError(
            f"Credentials file not found at {creds_path}. "
            "Check GOOGLE_SHEETS_CREDENTIALS_PATH in your .env file."
        )

    creds  = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
    client = gspread.authorize(creds)

    if not sheet_id:
        raise ValueError(
            "GOOGLE_SHEET_ID not set. Add it to your .env file."
        )

    sheet = client.open_by_key(sheet_id).sheet1
    log.info("Connected to Google Sheet: %s", sheet.spreadsheet.title)

    # Clear existing content
    sheet.clear()
    log.info("Sheet cleared.")

    # Write header row + data rows in one batch call
    all_rows = [headers] + rows

    # Convert None values to empty string for Sheets compatibility
    cleaned = [
        [("" if v is None else v) for v in row]
        for row in all_rows
    ]

    sheet.update(
        range_name="A1",
        values=cleaned,
    )
    log.info("Written %d rows (%d data + 1 header) to sheet.",
             len(all_rows), len(rows))


# ── Summary stats ──────────────────────────────────────────────────────────

def log_summary(headers: list, rows: list) -> None:
    """Log a quick summary of what was written."""
    if not rows:
        log.warning("No rows written — check that lineups are posted "
                    "and the pipeline has run for today.")
        return

    # Find column indices
    try:
        team_idx  = headers.index("batter_team")
        proj_idx  = headers.index("final_projection")
        delta_idx = headers.index("delta")
        hr_idx    = headers.index("projected_hr_probability")

        teams = sorted(set(r[team_idx] for r in rows if r[team_idx]))
        projs = [r[proj_idx] for r in rows if r[proj_idx] is not None]
        deltas= [r[delta_idx] for r in rows if r[delta_idx] is not None]
        hrs   = [r[hr_idx]   for r in rows if r[hr_idx]   is not None]

        log.info("Teams in export: %s", ", ".join(teams))
        if projs:
            log.info("Projection range: %.4f - %.4f (avg %.4f)",
                     min(projs), max(projs),
                     sum(projs) / len(projs))
        if deltas:
            log.info("Delta range: %.4f - %.4f",
                     min(deltas), max(deltas))
        if hrs:
            log.info("HR probability range: %.4f - %.4f (avg %.4f)",
                     min(hrs), max(hrs),
                     sum(hrs) / len(hrs))
    except (ValueError, TypeError):
        pass


# ── Entry point ────────────────────────────────────────────────────────────

def run(db_path: str, as_of_date: str, window_code: str = DEFAULT_WINDOW) -> None:
    headers, rows = fetch_projections(db_path, as_of_date, window_code)

    if not rows:
        log.warning("No data found for %s — sheet will not be updated.", as_of_date)
        return

    write_to_sheets(
        headers          = headers,
        rows             = rows,
        sheet_id         = GOOGLE_SHEET_ID,
        credentials_path = GOOGLE_SHEETS_CREDENTIALS,
    )
    log_summary(headers, rows)
    log.info("Google Sheets export complete for %s.", as_of_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export MLB matchup projections to Google Sheets"
    )
    parser.add_argument("--db-path", default=DB_PATH)
    parser.add_argument("--date",    help="Export date YYYY-MM-DD")
    parser.add_argument("--today",   action="store_true")
    parser.add_argument("--window",  default=DEFAULT_WINDOW,
                        help="Split window code (default: SEASON)")
    args = parser.parse_args()

    as_of = args.date if args.date else (
        date.today().isoformat() if args.today else None
    )
    if not as_of:
        parser.error("Provide --date YYYY-MM-DD or --today")

    run(db_path=args.db_path, as_of_date=as_of, window_code=args.window)
