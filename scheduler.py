"""
scheduler.py
------------
Game-aware daily pipeline scheduler. Run once each morning (manually or
via Task Scheduler at startup) and it handles all pipeline refreshes for
the day automatically.

Workflow:
  1. Runs a full pipeline refresh at startup (with Statcast)
  2. Queries today's game schedule from fact_games to find start times
     and expected teams
  3. Builds a dynamic refresh schedule:
       - 2 hours before first pitch of the day's earliest game
       - Every 30 minutes thereafter
       - Final refresh 30 minutes before the last game's first pitch
  4. At each scheduled interval: runs skip-statcast pipeline refresh
     (export_to_sheets runs inside run_pipeline.py — NOT called again
      here to avoid the double-export bug)
  5. Logs all activity including:
       - Teams expected at each refresh slot
       - Teams actually pulled in each run
       - FLAG if any expected team was missing from the export

Usage:
    python scheduler.py                    # runs for today
    python scheduler.py --date 2026-05-01  # runs for a specific date (testing)
    python scheduler.py --dry-run          # prints schedule without executing

Task Scheduler setup (Windows):
    Program:  C:\\Python310\\python.exe
    Arguments: C:\\Python310\\Projects\\mlb_model_2026\\scheduler.py
    Start in:  C:\\Python310\\Projects\\mlb_model_2026
    Trigger:   At log on (or daily at 8:00 AM)
"""

import sys
import time
import logging
import argparse
import sqlite3
import subprocess
import re
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# ── Project root setup ─────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# ── Logging ────────────────────────────────────────────────────────────────
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [scheduler]: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "scheduler.log"),
    ],
)
log = logging.getLogger("scheduler")

# ── Constants ──────────────────────────────────────────────────────────────
CT = ZoneInfo("America/Chicago")

# How many minutes before first pitch to begin lineup refreshes
LEAD_MINUTES_FIRST_GAME = 120   # 2 hours before earliest game

# How often to refresh during the game window (minutes)
REFRESH_INTERVAL_MINUTES = 30

# Stop refreshing this many minutes before last game first pitch
TRAIL_MINUTES_LAST_GAME  = 30   # 30 min before last game — catches late lineup posts

# Python executable — adjust if your environment differs
PYTHON = sys.executable


# ── Database helpers ───────────────────────────────────────────────────────

def get_game_times(db_path: str, game_date: str) -> list[datetime]:
    """
    Returns a list of game first-pitch datetimes (UTC, timezone-aware)
    for game_date, sorted ascending.
    """
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT game_datetime_utc
        FROM   fact_games
        WHERE  as_of_date = ?
          AND  game_datetime_utc IS NOT NULL
        ORDER  BY game_datetime_utc
        """,
        (game_date,),
    ).fetchall()
    conn.close()

    times = []
    for (dt_str,) in rows:
        if not dt_str:
            continue
        try:
            dt_str_clean = dt_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(dt_str_clean)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            times.append(dt)
        except ValueError:
            log.warning("Could not parse game time: %s", dt_str)

    return sorted(set(times))


def get_games_with_teams(db_path: str, game_date: str) -> list[dict]:
    """
    Returns all games for game_date with team abbreviations and start times.
    Used to build expected-team tracking per refresh slot.
    """
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT g.game_id,
               g.game_datetime_utc,
               th.team_abbr AS home_abbr,
               ta.team_abbr AS away_abbr
        FROM   fact_games g
        JOIN   dim_teams th ON th.team_id = g.home_team_id
        JOIN   dim_teams ta ON ta.team_id = g.away_team_id
        WHERE  g.as_of_date = ?
          AND  g.game_datetime_utc IS NOT NULL
        ORDER  BY g.game_datetime_utc
        """,
        (game_date,),
    ).fetchall()
    conn.close()

    games = []
    for game_id, dt_str, home, away in rows:
        if not dt_str:
            continue
        try:
            dt_str_clean = dt_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(dt_str_clean)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            games.append({
                "game_id":   game_id,
                "start_utc": dt,
                "home":      home,
                "away":      away,
            })
        except ValueError:
            pass
    return games


def get_expected_teams_at(games: list[dict], as_of_utc: datetime) -> set[str]:
    """
    Returns the set of team abbreviations whose games have a first pitch
    at or before as_of_utc + LEAD_MINUTES_FIRST_GAME.
    These are teams we'd expect to have lineups posted by this refresh time.
    We use a conservative window: teams whose game starts within 3 hours
    of the refresh time should have lineups available.
    """
    expected = set()
    window_end = as_of_utc + timedelta(hours=3)
    for g in games:
        if g["start_utc"] <= window_end:
            expected.add(g["home"])
            expected.add(g["away"])
    return expected


def parse_exported_teams_from_output(output: str) -> set[str]:
    """
    Parse the team abbreviations from the export_to_sheets log line:
      'Teams in export: BAL, CHC, CWS, ...'
    Returns a set of abbreviations, empty set if line not found.
    """
    match = re.search(r"Teams in export:\s*([A-Z, ]+)", output)
    if not match:
        return set()
    return {t.strip() for t in match.group(1).split(",") if t.strip()}


def build_refresh_schedule(game_times: list[datetime]) -> list[datetime]:
    """
    Given a list of game start times, returns a list of datetime moments
    when the pipeline refresh should fire.
    """
    if not game_times:
        log.warning("No game times found — no refresh schedule built.")
        return []

    window_start = game_times[0]  - timedelta(minutes=LEAD_MINUTES_FIRST_GAME)
    window_end   = game_times[-1] - timedelta(minutes=TRAIL_MINUTES_LAST_GAME)

    if window_end <= window_start:
        window_end = window_start + timedelta(minutes=REFRESH_INTERVAL_MINUTES)

    schedule = []
    current  = window_start
    while current <= window_end:
        schedule.append(current)
        current += timedelta(minutes=REFRESH_INTERVAL_MINUTES)

    return schedule


# ── Pipeline execution ─────────────────────────────────────────────────────

def run_command(cmd: list[str], label: str) -> tuple[bool, str]:
    """
    Run a subprocess command, log output, return (success, full_stdout).
    Returns full stdout so callers can parse team lists etc.
    """
    log.info("Running: %s", label)
    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=1800,
        )
        full_output = result.stdout or ""
        if result.returncode == 0:
            log.info("%s completed successfully.", label)
            if full_output.strip():
                for line in full_output.strip().split("\n")[-5:]:
                    log.info("  > %s", line)
            return True, full_output
        else:
            log.error("%s failed (exit %d).", label, result.returncode)
            if result.stderr.strip():
                log.error("  STDERR: %s", result.stderr.strip()[-500:])
            return False, full_output
    except subprocess.TimeoutExpired:
        log.error("%s timed out after 30 minutes.", label)
        return False, ""
    except Exception as e:
        log.error("%s error: %s", label, e)
        return False, ""


def run_full_pipeline(db_path: str, game_date: str) -> str:
    """Morning full refresh including Statcast. Returns stdout for team parsing."""
    log.info("=== FULL PIPELINE REFRESH (with Statcast) ===")
    _, output = run_command(
        [PYTHON, "run_pipeline.py", "--today", "--db-path", db_path],
        "Full pipeline"
    )
    return output


def run_lineup_refresh(db_path: str, game_date: str,
                       expected_teams: set[str] | None = None) -> None:
    """
    Lightweight lineup + match scores refresh (skip Statcast).

    NOTE: export_to_sheets runs inside run_pipeline.py as Step 7.
    We do NOT call it again here to avoid the double-export bug where
    the scheduler's standalone export overwrites the pipeline export
    with potentially stale data.

    expected_teams: set of team abbrs we expect to see in the export.
    If provided, any missing teams are flagged as warnings.
    """
    log.info("=== LINEUP REFRESH (skip Statcast) ===")

    if expected_teams:
        log.info("  Teams expected this run: %s",
                 ", ".join(sorted(expected_teams)))

    ok, output = run_command(
        [PYTHON, "run_pipeline.py", "--today", "--skip-statcast",
         "--db-path", db_path],
        "Pipeline (skip Statcast)"
    )

    if ok:
        # Parse which teams actually made it into the export
        exported_teams = parse_exported_teams_from_output(output)

        if exported_teams:
            log.info("  Teams pulled in this run: %s",
                     ", ".join(sorted(exported_teams)))

            # Flag any expected teams that didn't appear
            if expected_teams:
                missing = expected_teams - exported_teams
                if missing:
                    log.warning(
                        "  *** MISSING TEAMS (expected but not in export): %s ***",
                        ", ".join(sorted(missing))
                    )
                    log.warning(
                        "  Possible causes: lineup not yet posted, probable pitcher "
                        "missing, or matchup rows failed to build. "
                        "Will retry at next refresh interval."
                    )
                else:
                    log.info("  All expected teams present. Export complete.")
        else:
            log.warning("  Could not parse exported teams from pipeline output.")
    else:
        log.warning("Pipeline refresh failed — Sheets export may not have updated.")


# ── Schedule display ───────────────────────────────────────────────────────

def print_schedule(games: list[dict],
                   refresh_schedule: list[datetime],
                   game_date: str) -> None:
    """Print today's full schedule with team pairings and refresh slots."""
    log.info("=" * 60)
    log.info("SCHEDULER — %s", game_date)
    log.info("=" * 60)

    if games:
        log.info("Games today: %d", len(games))
        for g in games:
            ct_time = g["start_utc"].astimezone(CT).strftime("%I:%M %p CT")
            log.info("  %s  %s @ %s", ct_time, g["away"], g["home"])
    else:
        log.info("No games found for today.")

    if refresh_schedule:
        log.info("Refresh schedule (%d runs):", len(refresh_schedule))
        for rs in refresh_schedule:
            ct_time = rs.astimezone(CT).strftime("%I:%M %p CT")
            # Show which teams are expected to have lineups by this slot
            expected = get_expected_teams_at(games, rs)
            if expected:
                log.info("  Refresh at: %s  (expect: %s)",
                         ct_time, ", ".join(sorted(expected)))
            else:
                log.info("  Refresh at: %s", ct_time)
    log.info("=" * 60)


# ── Main scheduler loop ────────────────────────────────────────────────────

def run_scheduler(db_path: str, game_date: str, dry_run: bool = False) -> None:

    log.info("Scheduler starting for %s", game_date)
    log.info("DB path: %s", db_path)

    # ── Step 1: Full morning pipeline refresh ──────────────────────────────
    if not dry_run:
        output = run_full_pipeline(db_path, game_date)
        exported = parse_exported_teams_from_output(output)
        if exported:
            log.info("Initial pull — teams in export: %s",
                     ", ".join(sorted(exported)))
    else:
        log.info("[DRY RUN] Would run full pipeline now.")

    # ── Step 2: Query game schedule with team info ─────────────────────────
    games      = get_games_with_teams(db_path, game_date)
    game_times = sorted({g["start_utc"] for g in games})

    if not game_times:
        log.warning("No games found in database for %s.", game_date)
        log.warning("This may be an off-day, or the schedule hasn't been pulled yet.")
        log.info("Scheduler exiting — no refresh schedule to build.")
        return

    # ── Step 3: Build refresh schedule ────────────────────────────────────
    refresh_schedule = build_refresh_schedule(game_times)
    print_schedule(games, refresh_schedule, game_date)

    if dry_run:
        log.info("[DRY RUN] Exiting without executing refreshes.")
        return

    # ── Step 4: Wait and execute each scheduled refresh ───────────────────
    now_utc = datetime.now(timezone.utc)

    upcoming = [r for r in refresh_schedule if r > now_utc]
    skipped  = len(refresh_schedule) - len(upcoming)

    if skipped:
        log.info("Skipping %d past refresh times (scheduler started late).", skipped)

    if not upcoming:
        log.info("All scheduled refreshes are in the past. Running one refresh now.")
        expected = get_expected_teams_at(games, datetime.now(timezone.utc))
        run_lineup_refresh(db_path, game_date, expected_teams=expected)
        log.info("Scheduler complete.")
        return

    log.info("%d refreshes remaining today.", len(upcoming))

    # Log the next upcoming refresh for visibility
    next_ct = upcoming[0].astimezone(CT).strftime("%I:%M %p CT")
    next_expected = get_expected_teams_at(games, upcoming[0])
    log.info("Next refresh: %s — expecting teams: %s",
             next_ct, ", ".join(sorted(next_expected)) if next_expected else "TBD")

    for i, refresh_time in enumerate(upcoming, start=1):
        now_utc  = datetime.now(timezone.utc)
        wait_sec = (refresh_time - now_utc).total_seconds()
        ct_time  = refresh_time.astimezone(CT).strftime("%I:%M %p CT")

        # Compute expected teams for this slot
        expected_teams = get_expected_teams_at(games, refresh_time)

        if wait_sec > 0:
            wait_min = wait_sec / 60
            log.info(
                "Refresh %d/%d scheduled at %s — waiting %.1f minutes...",
                i, len(upcoming), ct_time, wait_min
            )
            # Preview next refresh slot while waiting
            if i < len(upcoming):
                next_slot    = upcoming[i]
                next_ct_str  = next_slot.astimezone(CT).strftime("%I:%M %p CT")
                next_exp     = get_expected_teams_at(games, next_slot)
                log.info("  After this: refresh %d/%d at %s (expect: %s)",
                         i + 1, len(upcoming), next_ct_str,
                         ", ".join(sorted(next_exp)) if next_exp else "TBD")

            # Sleep in 60-second chunks so Ctrl+C works responsively
            while True:
                now_utc   = datetime.now(timezone.utc)
                remaining = (refresh_time - now_utc).total_seconds()
                if remaining <= 0:
                    break
                time.sleep(min(60, remaining))

        log.info("Firing refresh %d/%d at %s", i, len(upcoming), ct_time)
        run_lineup_refresh(db_path, game_date, expected_teams=expected_teams)

    log.info("All scheduled refreshes complete for %s.", game_date)
    log.info("Scheduler exiting.")


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Game-aware daily MLB pipeline scheduler"
    )
    parser.add_argument(
        "--db-path", default="data/mlb_pregame.db",
        help="Path to SQLite database"
    )
    parser.add_argument(
        "--date", default=None,
        help="Game date YYYY-MM-DD (default: today)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print schedule without executing any pipeline commands"
    )
    args = parser.parse_args()

    game_date = args.date or date.today().isoformat()

    run_scheduler(
        db_path   = args.db_path,
        game_date = game_date,
        dry_run   = args.dry_run,
    )
