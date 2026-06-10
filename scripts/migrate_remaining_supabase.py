#!/usr/bin/env python3
"""
scripts/migrate_remaining_supabase.py
--------------------------------------
Applies all remaining schema migrations to Supabase that were added
via incremental SQLite migration scripts after the initial init_db.py:

  - migrate_add_boxscore_table.py   → fact_player_game_results
  - migrate_add_total_bases.py      → new columns on several fact tables
  - migrate_add_power_profile.py    → already applied separately
  - migrate_seed_park_factors.py    → park factor data in dim_venues

Safe to re-run — uses IF NOT EXISTS and ON CONFLICT DO NOTHING.

Run:
    DB_BACKEND=supabase python scripts/migrate_remaining_supabase.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.db import execute, get_engine, DB_BACKEND, ping
from sqlalchemy import text

DDL = [
    # ── fact_player_game_results ───────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fact_player_game_results (
        game_date          TEXT    NOT NULL,
        game_id            INTEGER NOT NULL,
        player_id          INTEGER NOT NULL,
        team_id            INTEGER NOT NULL,
        at_bats            INTEGER,
        plate_appearances  INTEGER,
        hits               INTEGER,
        doubles            INTEGER,
        triples            INTEGER,
        home_runs          INTEGER,
        rbi                INTEGER,
        walks              INTEGER,
        strikeouts         INTEGER,
        hit_by_pitch       INTEGER,
        sac_flies          INTEGER,
        stolen_bases       INTEGER,
        total_bases        INTEGER,
        batting_avg        DOUBLE PRECISION,
        slugging_pct       DOUBLE PRECISION,
        hr_flag            INTEGER,
        lineup_slot        INTEGER,
        position           TEXT,
        load_timestamp_utc TEXT,
        PRIMARY KEY (game_date, game_id, player_id),
        FOREIGN KEY (player_id) REFERENCES dim_players(player_id),
        FOREIGN KEY (team_id)   REFERENCES dim_teams(team_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_boxscore_player_date ON fact_player_game_results(player_id, game_date)",
    "CREATE INDEX IF NOT EXISTS idx_boxscore_game ON fact_player_game_results(game_id, game_date)",

    # ── fact_batter_overall — add games_played, ab_per_game ───────────────
    "ALTER TABLE fact_batter_overall ADD COLUMN IF NOT EXISTS games_played INTEGER",
    "ALTER TABLE fact_batter_overall ADD COLUMN IF NOT EXISTS ab_per_game DOUBLE PRECISION",

    # ── fact_matchup_batter_pitcher — add total bases projection columns ───
    "ALTER TABLE fact_matchup_batter_pitcher ADD COLUMN IF NOT EXISTS proj_at_bats_per_game DOUBLE PRECISION",
    "ALTER TABLE fact_matchup_batter_pitcher ADD COLUMN IF NOT EXISTS pt_slg_score DOUBLE PRECISION",
    "ALTER TABLE fact_matchup_batter_pitcher ADD COLUMN IF NOT EXISTS zone_slg_score DOUBLE PRECISION",
    "ALTER TABLE fact_matchup_batter_pitcher ADD COLUMN IF NOT EXISTS projected_slugging DOUBLE PRECISION",
    "ALTER TABLE fact_matchup_batter_pitcher ADD COLUMN IF NOT EXISTS projected_total_bases DOUBLE PRECISION",
    "ALTER TABLE fact_matchup_batter_pitcher ADD COLUMN IF NOT EXISTS projected_hr_probability DOUBLE PRECISION",
    "ALTER TABLE fact_matchup_batter_pitcher ADD COLUMN IF NOT EXISTS batter_barrel_rate DOUBLE PRECISION",
    "ALTER TABLE fact_matchup_batter_pitcher ADD COLUMN IF NOT EXISTS pitcher_barrel_rate_allowed DOUBLE PRECISION",

    # ── fact_batter_pitch_type_splits — add slugging ──────────────────────
    "ALTER TABLE fact_batter_pitch_type_splits ADD COLUMN IF NOT EXISTS slugging_pct DOUBLE PRECISION",

    # ── fact_batter_zone_splits — add slugging ────────────────────────────
    "ALTER TABLE fact_batter_zone_splits ADD COLUMN IF NOT EXISTS slugging_pct DOUBLE PRECISION",
]

# Park factor seed data — matches migrate_seed_park_factors.py
# Format: (venue_name_fragment, hr_rhb, hr_lhb, run_factor)
PARK_FACTORS = [
    ("Coors Field",               123, 118, 1.15),
    ("Great American Ball Park",  118, 115, 1.10),
    ("Yankee Stadium",            120, 108, 1.08),
    ("Fenway Park",               105, 120, 1.06),
    ("Globe Life Field",          112, 110, 1.07),
    ("Camden Yards",              110, 108, 1.06),
    ("Citizens Bank Park",        110, 108, 1.06),
    ("Wrigley Field",             108, 110, 1.05),
    ("Rogers Centre",             108, 105, 1.06),
    ("Guaranteed Rate Field",     105, 103, 1.04),
    ("Chase Field",               105, 103, 1.04),
    ("Truist Park",               105, 105, 1.03),
    ("Nationals Park",            100, 100, 1.00),
    ("Minute Maid Park",          100, 103, 1.01),
    ("Dodger Stadium",             98, 100, 0.99),
    ("Angel Stadium",              97,  96, 0.98),
    ("Target Field",               97,  98, 0.98),
    ("Progressive Field",          97,  97, 0.98),
    ("American Family Field",     100,  98, 0.99),
    ("Kauffman Stadium",           95,  96, 0.97),
    ("Busch Stadium",              95,  97, 0.97),
    ("Comerica Park",              93,  95, 0.96),
    ("PNC Park",                   93,  95, 0.96),
    ("Tropicana Field",            93,  93, 0.96),
    ("loanDepot Park",             92,  92, 0.96),
    ("Oakland Coliseum",           92,  92, 0.96),
    ("Citi Field",                 96,  98, 0.97),
    ("T-Mobile Park",              90,  92, 0.95),
    ("Petco Park",                 88,  90, 0.94),
    ("Oracle Park",                85,  88, 0.93),
]


def seed_park_factors(engine):
    updated = not_found = 0
    with engine.begin() as conn:
        for fragment, hr_rhb, hr_lhb, run_factor in PARK_FACTORS:
            result = conn.execute(
                text("""
                    UPDATE dim_venues
                    SET park_hr_factor_rhb = :rhb,
                        park_hr_factor_lhb = :lhb,
                        park_run_factor    = :run
                    WHERE venue_name ILIKE :name
                """),
                {"rhb": hr_rhb, "lhb": hr_lhb,
                 "run": run_factor, "name": f"%{fragment}%"},
            )
            if result.rowcount > 0:
                updated += result.rowcount
                print(f"  ✓ {fragment}")
            else:
                not_found += 1
                print(f"  ~ not found: {fragment}")
    print(f"  Park factors: {updated} venues updated, {not_found} not matched")


def main():
    if DB_BACKEND != "supabase":
        print(f"[ERROR] DB_BACKEND={DB_BACKEND!r} — set DB_BACKEND=supabase first.")
        sys.exit(1)

    print("[migrate] Connecting to Supabase...")
    if not ping():
        print("[migrate] Cannot reach Supabase.")
        sys.exit(1)

    print(f"[migrate] Running {len(DDL)} DDL statements...")
    for stmt in DDL:
        stmt = stmt.strip()
        label = stmt.replace("\n", " ").strip()[:60]
        try:
            execute(stmt)
            print(f"  ✓ {label}")
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
            raise

    print("\n[migrate] Seeding park factors...")
    engine = get_engine()
    seed_park_factors(engine)

    print("\n[migrate] All remaining migrations complete ✓")


if __name__ == "__main__":
    main()
