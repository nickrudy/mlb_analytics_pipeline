"""
query_hr_prob_filtered.py
--------------------------
HR probability query filtered to specific teams.
Currently set to: COL vs LAD and SEA vs ATH (Athletics/Sacramento)

Usage:
    python query_hr_prob_filtered.py --db-path data/mlb_pregame.db
"""

import sqlite3
import argparse
from datetime import date

QUERY = """
SELECT
    p_batter.full_name              AS batter_name,
    p_batter.bats,
    t_batter.team_abbr              AS batter_team,
    t_home.team_abbr                AS home_team,
    t_away.team_abbr                AS away_team,
    p_pitcher.full_name             AS pitcher_name,
    p_pitcher.throws                AS pitcher_hand,
    g.game_datetime_utc,
    ROUND(pp.hr_per_pa, 4)          AS hr_per_pa,
    ROUND(pp.barrels_per_pa, 4)     AS batter_barrel_rate,
    ROUND(pp.avg_exit_velocity, 1)  AS avg_ev,
    ROUND(pv.hr_per_bf_allowed, 4)  AS pitcher_hr_rate_allowed,
    v.venue_name,
    v.park_hr_factor_rhb,
    v.park_hr_factor_lhb,
    ROUND(
        (
            (COALESCE(pp.hr_per_pa, 0.034) * 0.50)
            +
            (COALESCE(pv.hr_per_bf_allowed, 0.034) * 0.50)
        )
        *
        CASE p_batter.bats
            WHEN 'L' THEN COALESCE(v.park_hr_factor_lhb / 100.0, 1.0)
            WHEN 'R' THEN COALESCE(v.park_hr_factor_rhb / 100.0, 1.0)
            WHEN 'S' THEN
                CASE g_prob.pitcher_hand
                    WHEN 'L' THEN COALESCE(v.park_hr_factor_rhb / 100.0, 1.0)
                    ELSE COALESCE(v.park_hr_factor_lhb / 100.0, 1.0)
                END
            ELSE 1.0
        END
    , 4) AS estimated_hr_prob
FROM fact_games g
JOIN (
    SELECT
        fg.game_id,
        fg.as_of_date,
        fg.home_probable_pitcher_id   AS pitcher_id,
        fg.home_team_id               AS pitching_team_id,
        fg.away_team_id               AS batting_team_id,
        'away'                        AS batting_side,
        p.throws                      AS pitcher_hand
    FROM fact_games fg
    JOIN dim_players p ON p.player_id = fg.home_probable_pitcher_id
    WHERE fg.home_probable_pitcher_id IS NOT NULL

    UNION ALL

    SELECT
        fg.game_id,
        fg.as_of_date,
        fg.away_probable_pitcher_id,
        fg.away_team_id,
        fg.home_team_id,
        'home',
        p.throws
    FROM fact_games fg
    JOIN dim_players p ON p.player_id = fg.away_probable_pitcher_id
    WHERE fg.away_probable_pitcher_id IS NOT NULL
) g_prob
    ON  g_prob.game_id    = g.game_id
    AND g_prob.as_of_date = g.as_of_date
JOIN dim_players p_batter
    ON  p_batter.team_id     = g_prob.batting_team_id
    AND p_batter.position   != 'P'
    AND p_batter.active_flag = 1
JOIN dim_players p_pitcher
    ON  p_pitcher.player_id  = g_prob.pitcher_id
JOIN dim_teams t_batter
    ON  t_batter.team_id     = g_prob.batting_team_id
JOIN dim_teams t_home
    ON  t_home.team_id       = g.home_team_id
JOIN dim_teams t_away
    ON  t_away.team_id       = g.away_team_id
JOIN dim_venues v
    ON  v.venue_id           = g.venue_id
LEFT JOIN fact_batter_power_profile pp
    ON  pp.player_id         = p_batter.player_id
    AND pp.as_of_date        = g.as_of_date
    AND pp.window_code       = 'SEASON'
LEFT JOIN fact_pitcher_hr_vulnerability pv
    ON  pv.pitcher_id        = g_prob.pitcher_id
    AND pv.as_of_date        = g.as_of_date
    AND pv.window_code       = 'SEASON'
    AND pv.split_hand        = CASE p_batter.bats
                                   WHEN 'S' THEN
                                       CASE g_prob.pitcher_hand
                                           WHEN 'L' THEN 'R'
                                           ELSE 'L'
                                       END
                                   ELSE p_batter.bats
                               END
WHERE g.as_of_date = ?
  AND t_batter.team_abbr IN ('COL', 'LAD', 'SEA', 'ATH', 'OAK', 'SAC')
ORDER BY
    t_batter.team_abbr,
    estimated_hr_prob DESC
"""


def run(db_path: str, as_of_date: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")
    cursor = conn.execute(QUERY, (as_of_date,))
    headers = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print(f"No rows returned for {as_of_date}.")
        print("Check that probable pitchers are posted for these games.")
        return

    print(f"\nHR Probability — COL vs LAD | SEA vs Athletics — {as_of_date}")
    print(f"Rows returned: {len(rows)}")
    print("-" * 120)

    col_widths = [
        max(len(str(h)), max((len(str(r[i])) for r in rows), default=0))
        for i, h in enumerate(headers)
    ]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format(*headers))
    print("  ".join("-" * w for w in col_widths))

    current_team = None
    for row in rows:
        team = row[2]  # batter_team
        if team != current_team:
            if current_team is not None:
                print()
            print(f"--- {team} ---")
            current_team = team
        print(fmt.format(*[str(v) if v is not None else "" for v in row]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="HR probability for COL vs LAD and SEA vs Athletics"
    )
    parser.add_argument("--db-path", default="data/mlb_pregame.db")
    parser.add_argument("--date",    default=date.today().isoformat())
    args = parser.parse_args()
    run(db_path=args.db_path, as_of_date=args.date)
