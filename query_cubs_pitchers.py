"""
query_cubs_pitchers.py
-----------------------
Runs three Cubs pitcher analysis queries and exports results to CSV files
for use in building the Cubs pitching editorial document.

Usage:
    python query_cubs_pitchers.py --db-path data/mlb_pregame.db
"""

import sqlite3
import csv
import argparse
from pathlib import Path

QUERIES = {
    "cubs_pitcher_inventory": """
        SELECT
            p.player_id,
            p.full_name,
            COUNT(DISTINCT s.game_pk)      AS games_appeared,
            COUNT(*)                        AS total_pitches,
            MIN(s.at_bat_number)            AS min_at_bat_num,
            ROUND(AVG(s.release_speed), 1)  AS avg_velo
        FROM stg_statcast_pitches s
        JOIN dim_players p ON p.player_id = s.pitcher_id
        WHERE p.current_team_id = (
                SELECT team_id FROM dim_teams WHERE team_abbr = 'CHC'
              )
          AND s.game_date        >= '2026-03-01'
          AND p.primary_position  = 'P'
        GROUP BY p.player_id, p.full_name
        ORDER BY total_pitches DESC
    """,

    "cubs_pitch_type_breakdown": """
        SELECT
            p.full_name,
            s.pitch_type_code,
            COUNT(*) AS pitches,
            ROUND(
                COUNT(*) * 100.0 /
                SUM(COUNT(*)) OVER (PARTITION BY p.player_id),
                1
            ) AS usage_pct,
            ROUND(AVG(s.release_speed), 1) AS avg_velo,
            ROUND(
                SUM(CASE WHEN s.description IN (
                    'swinging_strike', 'swinging_strike_blocked'
                ) THEN 1.0 ELSE 0 END) /
                NULLIF(SUM(CASE WHEN s.description LIKE '%swing%'
                    THEN 1 ELSE 0 END), 0),
                3
            ) AS whiff_rate,
            ROUND(AVG(s.estimated_woba_using_speedangle), 3) AS xwoba_allowed
        FROM stg_statcast_pitches s
        JOIN dim_players p ON p.player_id = s.pitcher_id
        WHERE p.current_team_id = (
                SELECT team_id FROM dim_teams WHERE team_abbr = 'CHC'
              )
          AND s.game_date        >= '2026-03-01'
          AND p.primary_position  = 'P'
          AND s.pitch_type_code   IS NOT NULL
        GROUP BY p.player_id, p.full_name, s.pitch_type_code
        ORDER BY p.full_name, pitches DESC
    """,

    "cubs_zone_breakdown": """
        SELECT
            p.full_name,
            s.zone,
            COUNT(*) AS pitches,
            ROUND(
                COUNT(*) * 100.0 /
                SUM(COUNT(*)) OVER (PARTITION BY p.player_id),
                1
            ) AS zone_pct,
            ROUND(
                SUM(CASE WHEN s.description IN (
                    'swinging_strike', 'swinging_strike_blocked'
                ) THEN 1.0 ELSE 0 END) /
                NULLIF(SUM(CASE WHEN s.description LIKE '%swing%'
                    THEN 1 ELSE 0 END), 0),
                3
            ) AS whiff_rate
        FROM stg_statcast_pitches s
        JOIN dim_players p ON p.player_id = s.pitcher_id
        WHERE p.current_team_id = (
                SELECT team_id FROM dim_teams WHERE team_abbr = 'CHC'
              )
          AND s.game_date        >= '2026-03-01'
          AND p.primary_position  = 'P'
          AND s.zone              IS NOT NULL
        GROUP BY p.player_id, p.full_name, s.zone
        ORDER BY p.full_name, pitches DESC
    """,
}

OUTPUT_DIR = Path("data/cubs_analysis")


def run(db_path: str) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")

    for name, sql in QUERIES.items():
        print(f"Running: {name}...")
        cursor = conn.execute(sql)
        headers = [d[0] for d in cursor.description]
        rows    = cursor.fetchall()
        print(f"  {len(rows)} rows returned.")

        out_path = OUTPUT_DIR / f"{name}.csv"
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        print(f"  Saved to {out_path}")

    conn.close()
    print("\nAll queries complete. CSV files saved to data/cubs_analysis/")
    print("Upload all three CSV files to share results.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="data/mlb_pregame.db")
    args = parser.parse_args()
    run(args.db_path)
