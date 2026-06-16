"""
export_to_daily_tables.py
-------------------------
Writes pre-computed leaderboard snapshots to three flat tables in Supabase:
  - daily_top_batting
  - daily_top_bases
  - daily_top_hrs

These tables are designed for direct Looker Studio consumption — simple
flat reads with no joins or window functions, eliminating query load
on the Nano instance and avoiding Looker Studio credential timeout issues.

Each run truncates and rewrites all three tables so Looker Studio always
reads fresh data. The refreshed_at column timestamps each write.

Usage:
    python ingest/export_to_daily_tables.py --today
    python ingest/export_to_daily_tables.py --date 2026-06-16
"""
import logging
import argparse
from datetime import date, datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.db import get_connection, DB_BACKEND
from utils.db_bulk import bulk_upsert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# Thresholds — match the view definitions
BATTING_MIN  = 0.22
BASES_MIN    = 1.5
HR_MIN       = 0.10


def _now_utc():
    return datetime.now(timezone.utc).isoformat()


def export_daily_tables(as_of_date: str) -> None:
    ct_date = f"(CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago')::date::text"

    with get_connection() as conn:
        if DB_BACKEND == "sqlite":
            conn.execute("PRAGMA foreign_keys=OFF;")

        cur = conn.cursor()
        now = _now_utc()

        # ── daily_top_batting ──────────────────────────────────────────────
        cur.execute(
            f"""
            WITH latest AS (
                SELECT
                    m.game_id, m.batter_id, m.team_id,
                    m.projected_batting_avg,
                    m.batter_vs_hand_batting_avg AS baseline_avg,
                    m.projected_batting_avg - m.batter_vs_hand_batting_avg AS delta,
                    m.as_of_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.game_id, m.batter_id, m.window_code
                        ORDER BY m.ingested_at DESC
                    ) AS rn
                FROM fact_matchup_batter_pitcher m
                WHERE m.window_code = 'SEASON'
                  AND m.projected_batting_avg >= :batting_min
                  AND m.as_of_date = :aod
            )
            SELECT
                pb.full_name AS batter_name,
                tb.team_abbr AS batter_team,
                (g.game_datetime_utc::timestamptz AT TIME ZONE 'America/Chicago') AS game_datetime_ct,
                ROUND(l.projected_batting_avg::numeric, 3) AS final_projection,
                ROUND(l.baseline_avg::numeric, 3)          AS baseline_avg,
                ROUND(l.delta::numeric, 3)                 AS delta
            FROM latest l
            JOIN dim_players pb ON pb.player_id = l.batter_id
            JOIN dim_teams   tb ON tb.team_id   = l.team_id
            JOIN fact_games  g  ON g.game_id    = l.game_id
                                AND g.as_of_date = l.as_of_date
            WHERE l.rn = 1
            ORDER BY l.projected_batting_avg DESC
            """,
            {"batting_min": BATTING_MIN, "aod": as_of_date},
        )
        batting_rows = cur.fetchall()
        batting_cols = [d[0] for d in cur.description]

        # ── daily_top_bases ────────────────────────────────────────────────
        cur.execute(
            """
            WITH latest AS (
                SELECT
                    m.game_id, m.batter_id, m.team_id,
                    m.projected_batting_avg,
                    m.batter_vs_hand_batting_avg AS baseline_avg,
                    m.projected_batting_avg - m.batter_vs_hand_batting_avg AS delta,
                    m.projected_total_bases,
                    m.as_of_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.game_id, m.batter_id, m.window_code
                        ORDER BY m.ingested_at DESC
                    ) AS rn
                FROM fact_matchup_batter_pitcher m
                WHERE m.window_code = 'SEASON'
                  AND m.projected_total_bases >= :bases_min
                  AND m.as_of_date = :aod
            )
            SELECT
                pb.full_name AS batter_name,
                tb.team_abbr AS batter_team,
                (g.game_datetime_utc::timestamptz AT TIME ZONE 'America/Chicago') AS game_datetime_ct,
                ROUND(l.projected_batting_avg::numeric, 3)   AS final_projection,
                ROUND(l.baseline_avg::numeric, 3)             AS baseline_avg,
                ROUND(l.delta::numeric, 3)                    AS delta,
                ROUND(l.projected_total_bases::numeric, 3)    AS projected_total_bases
            FROM latest l
            JOIN dim_players pb ON pb.player_id = l.batter_id
            JOIN dim_teams   tb ON tb.team_id   = l.team_id
            JOIN fact_games  g  ON g.game_id    = l.game_id
                                AND g.as_of_date = l.as_of_date
            WHERE l.rn = 1
            ORDER BY l.projected_total_bases DESC
            """,
            {"bases_min": BASES_MIN, "aod": as_of_date},
        )
        bases_rows = cur.fetchall()
        bases_cols = [d[0] for d in cur.description]

        # ── daily_top_hrs ──────────────────────────────────────────────────
        cur.execute(
            """
            WITH latest AS (
                SELECT
                    m.game_id, m.batter_id, m.team_id,
                    m.projected_batting_avg,
                    m.batter_vs_hand_batting_avg AS baseline_avg,
                    m.projected_batting_avg - m.batter_vs_hand_batting_avg AS delta,
                    m.projected_hr_probability,
                    m.as_of_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.game_id, m.batter_id, m.window_code
                        ORDER BY m.ingested_at DESC
                    ) AS rn
                FROM fact_matchup_batter_pitcher m
                WHERE m.window_code = 'SEASON'
                  AND m.projected_hr_probability >= :hr_min
                  AND m.as_of_date = :aod
            )
            SELECT
                pb.full_name AS batter_name,
                tb.team_abbr AS batter_team,
                (g.game_datetime_utc::timestamptz AT TIME ZONE 'America/Chicago') AS game_datetime_ct,
                ROUND(l.projected_batting_avg::numeric, 3)      AS final_projection,
                ROUND(l.baseline_avg::numeric, 3)                AS baseline_avg,
                ROUND(l.delta::numeric, 3)                       AS delta,
                ROUND(l.projected_hr_probability::numeric, 3)    AS projected_hr_probability
            FROM latest l
            JOIN dim_players pb ON pb.player_id = l.batter_id
            JOIN dim_teams   tb ON tb.team_id   = l.team_id
            JOIN fact_games  g  ON g.game_id    = l.game_id
                                AND g.as_of_date = l.as_of_date
            WHERE l.rn = 1
            ORDER BY l.projected_hr_probability DESC
            """,
            {"hr_min": HR_MIN, "aod": as_of_date},
        )
        hrs_rows = cur.fetchall()
        hrs_cols = [d[0] for d in cur.description]

        # ── Truncate and rewrite all three tables ──────────────────────────
        for table in ("daily_top_batting", "daily_top_bases", "daily_top_hrs"):
            conn.execute(f"TRUNCATE TABLE {table}")
        log.info("  Truncated daily_top_batting, daily_top_bases, daily_top_hrs.")

        def to_dicts(rows, cols, extra):
            return [{**dict(zip(cols, r)), **extra} for r in rows]

        extra = {"refreshed_at": now}

        if batting_rows:
            bulk_upsert(conn, "daily_top_batting", to_dicts(batting_rows, batting_cols, extra))
            log.info("  daily_top_batting: %d rows written.", len(batting_rows))
        else:
            log.warning("  daily_top_batting: 0 rows (no matchups above threshold).")

        if bases_rows:
            bulk_upsert(conn, "daily_top_bases", to_dicts(bases_rows, bases_cols, extra))
            log.info("  daily_top_bases: %d rows written.", len(bases_rows))
        else:
            log.warning("  daily_top_bases: 0 rows (no matchups above threshold).")

        if hrs_rows:
            bulk_upsert(conn, "daily_top_hrs", to_dicts(hrs_rows, hrs_cols, extra))
            log.info("  daily_top_hrs: %d rows written.", len(hrs_rows))
        else:
            log.warning("  daily_top_hrs: 0 rows (no matchups above threshold).")

        conn.commit()
        log.info("Daily table export complete for %s.", as_of_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",  help="as_of_date YYYY-MM-DD")
    parser.add_argument("--today", action="store_true")
    args = parser.parse_args()

    as_of = args.date if args.date else (
        date.today().isoformat() if args.today else None
    )
    if not as_of:
        parser.error("Provide --date YYYY-MM-DD or --today")

    export_daily_tables(as_of)
