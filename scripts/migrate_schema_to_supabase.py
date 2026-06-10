#!/usr/bin/env python3
"""
scripts/migrate_schema_to_supabase.py
--------------------------------------
Migrates the full MLB pre-game schema from SQLite DDL to Supabase (PostgreSQL).
Translated from db/init_db.py.

Run once:
    DB_BACKEND=supabase python scripts/migrate_schema_to_supabase.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.db import execute, DB_BACKEND, ping

# =============================================================================
# SQLite → PostgreSQL translation notes:
#   - INTEGER PRIMARY KEY / composite PKs → same syntax in PG ✓
#   - REAL                               → DOUBLE PRECISION
#   - TEXT                               → TEXT ✓
#   - FOREIGN KEY syntax                 → same ✓
#   - CREATE VIEW IF NOT EXISTS          → same ✓
#   - INSERT OR IGNORE                   → INSERT ... ON CONFLICT DO NOTHING
#   - No PRAGMA needed in PG
# =============================================================================

DDL_STATEMENTS = [

    # ── Dimension tables ───────────────────────────────────────────────────

    """
    CREATE TABLE IF NOT EXISTS dim_venues (
        venue_id              INTEGER NOT NULL,
        venue_name            TEXT,
        city                  TEXT,
        state                 TEXT,
        time_zone_name        TEXT,
        roof_type             TEXT,
        surface_type          TEXT,
        lat                   DOUBLE PRECISION,
        lon                   DOUBLE PRECISION,
        altitude_ft           INTEGER,
        park_run_factor       DOUBLE PRECISION,
        park_hr_factor_lhb    DOUBLE PRECISION,
        park_hr_factor_rhb    DOUBLE PRECISION,
        PRIMARY KEY (venue_id)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS dim_teams (
        team_id       INTEGER NOT NULL,
        team_name     TEXT,
        team_abbr     TEXT,
        league_name   TEXT,
        division_name TEXT,
        venue_id      INTEGER,
        active_flag   INTEGER,
        PRIMARY KEY (team_id),
        FOREIGN KEY (venue_id) REFERENCES dim_venues(venue_id)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS dim_players (
        player_id        INTEGER NOT NULL,
        full_name        TEXT,
        first_name       TEXT,
        last_name        TEXT,
        birth_date       TEXT,
        bats             TEXT,
        throws           TEXT,
        primary_position TEXT,
        current_team_id  INTEGER,
        active_flag      INTEGER,
        mlb_debut_date   TEXT,
        PRIMARY KEY (player_id),
        FOREIGN KEY (current_team_id) REFERENCES dim_teams(team_id)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_dim_players_team ON dim_players(current_team_id)",
    "CREATE INDEX IF NOT EXISTS idx_dim_players_name ON dim_players(full_name)",

    """
    CREATE TABLE IF NOT EXISTS dim_pitch_types (
        pitch_type_code TEXT NOT NULL,
        pitch_type_name TEXT,
        pitch_group     TEXT,
        velocity_band   TEXT,
        PRIMARY KEY (pitch_type_code)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS dim_zones (
        zone_code           TEXT NOT NULL,
        zone_bucket         TEXT,
        zone_group          TEXT,
        zone_row            INTEGER,
        zone_col            INTEGER,
        in_strike_zone_flag INTEGER,
        x_min               DOUBLE PRECISION,
        x_max               DOUBLE PRECISION,
        z_min               DOUBLE PRECISION,
        z_max               DOUBLE PRECISION,
        PRIMARY KEY (zone_code)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS dim_split_windows (
        window_code        TEXT NOT NULL,
        window_name        TEXT,
        window_start_rule  TEXT,
        window_end_rule    TEXT,
        min_pa_threshold   INTEGER,
        min_bf_threshold   INTEGER,
        regression_weight  DOUBLE PRECISION,
        PRIMARY KEY (window_code)
    )
    """,

    # ── Staging tables ─────────────────────────────────────────────────────

    """
    CREATE TABLE IF NOT EXISTS stg_mlb_schedule_games (
        as_of_date               TEXT NOT NULL,
        game_id                  INTEGER NOT NULL,
        season                   INTEGER,
        game_date                TEXT,
        game_datetime_utc        TEXT,
        home_team_id             INTEGER,
        away_team_id             INTEGER,
        venue_id                 INTEGER,
        day_night                TEXT,
        doubleheader_flag        INTEGER,
        scheduled_innings        INTEGER,
        home_probable_pitcher_id INTEGER,
        away_probable_pitcher_id INTEGER,
        status_code              TEXT,
        raw_payload_json         TEXT,
        load_timestamp_utc       TEXT,
        PRIMARY KEY (as_of_date, game_id)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_stg_schedule_game_date ON stg_mlb_schedule_games(game_date)",

    """
    CREATE TABLE IF NOT EXISTS stg_statcast_pitches (
        game_date                       TEXT NOT NULL,
        game_pk                         INTEGER NOT NULL,
        at_bat_number                   INTEGER NOT NULL,
        pitch_number                    INTEGER NOT NULL,
        pitcher_id                      INTEGER,
        batter_id                       INTEGER,
        pitch_type_code                 TEXT,
        stand                           TEXT,
        p_throws                        TEXT,
        balls                           INTEGER,
        strikes                         INTEGER,
        zone                            INTEGER,
        plate_x                         DOUBLE PRECISION,
        plate_z                         DOUBLE PRECISION,
        release_speed                   DOUBLE PRECISION,
        release_spin_rate               DOUBLE PRECISION,
        release_extension               DOUBLE PRECISION,
        release_pos_x                   DOUBLE PRECISION,
        release_pos_z                   DOUBLE PRECISION,
        pfx_x                           DOUBLE PRECISION,
        pfx_z                           DOUBLE PRECISION,
        description                     TEXT,
        events                          TEXT,
        bb_type                         TEXT,
        launch_speed                    DOUBLE PRECISION,
        launch_angle                    DOUBLE PRECISION,
        estimated_ba_using_speedangle   DOUBLE PRECISION,
        estimated_woba_using_speedangle DOUBLE PRECISION,
        raw_payload_json                TEXT,
        PRIMARY KEY (game_date, game_pk, at_bat_number, pitch_number)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_stg_statcast_pitcher_date ON stg_statcast_pitches(pitcher_id, game_date)",
    "CREATE INDEX IF NOT EXISTS idx_stg_statcast_batter_date  ON stg_statcast_pitches(batter_id, game_date)",
    "CREATE INDEX IF NOT EXISTS idx_stg_statcast_gamepk        ON stg_statcast_pitches(game_pk)",

    """
    CREATE TABLE IF NOT EXISTS stg_weather_hourly (
        as_of_date                    TEXT NOT NULL,
        venue_id                      INTEGER NOT NULL,
        forecast_timestamp_utc        TEXT NOT NULL,
        temperature_f                 DOUBLE PRECISION,
        wind_speed_mph                DOUBLE PRECISION,
        wind_direction_deg            DOUBLE PRECISION,
        humidity_pct                  DOUBLE PRECISION,
        precipitation_probability_pct DOUBLE PRECISION,
        pressure_hpa                  DOUBLE PRECISION,
        raw_payload_json              TEXT,
        PRIMARY KEY (as_of_date, venue_id, forecast_timestamp_utc)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_stg_weather_venue_time ON stg_weather_hourly(venue_id, forecast_timestamp_utc)",

    # ── Fact tables ────────────────────────────────────────────────────────

    """
    CREATE TABLE IF NOT EXISTS fact_games (
        as_of_date                 TEXT NOT NULL,
        game_id                    INTEGER NOT NULL,
        season                     INTEGER,
        game_date                  TEXT,
        game_datetime_utc          TEXT,
        home_team_id               INTEGER,
        away_team_id               INTEGER,
        venue_id                   INTEGER,
        day_night                  TEXT,
        doubleheader_flag          INTEGER,
        scheduled_innings          INTEGER,
        home_probable_pitcher_id   INTEGER,
        away_probable_pitcher_id   INTEGER,
        confirmed_home_lineup_flag INTEGER,
        confirmed_away_lineup_flag INTEGER,
        PRIMARY KEY (as_of_date, game_id),
        FOREIGN KEY (home_team_id)             REFERENCES dim_teams(team_id),
        FOREIGN KEY (away_team_id)             REFERENCES dim_teams(team_id),
        FOREIGN KEY (venue_id)                 REFERENCES dim_venues(venue_id),
        FOREIGN KEY (home_probable_pitcher_id) REFERENCES dim_players(player_id),
        FOREIGN KEY (away_probable_pitcher_id) REFERENCES dim_players(player_id)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_fact_games_date  ON fact_games(as_of_date, game_date)",
    "CREATE INDEX IF NOT EXISTS idx_fact_games_teams ON fact_games(as_of_date, home_team_id, away_team_id)",

    """
    CREATE TABLE IF NOT EXISTS fact_game_lineups (
        as_of_date            TEXT NOT NULL,
        game_id               INTEGER NOT NULL,
        team_id               INTEGER NOT NULL,
        player_id             INTEGER NOT NULL,
        lineup_slot           INTEGER,
        batting_order         TEXT,
        starter_flag          INTEGER,
        confirmed_flag        INTEGER,
        projected_flag        INTEGER,
        opponent_pitcher_id   INTEGER,
        opponent_pitcher_hand TEXT,
        PRIMARY KEY (as_of_date, game_id, team_id, player_id),
        FOREIGN KEY (opponent_pitcher_id) REFERENCES dim_players(player_id)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_lineups_game_team ON fact_game_lineups(as_of_date, game_id, team_id)",
    "CREATE INDEX IF NOT EXISTS idx_lineups_player     ON fact_game_lineups(as_of_date, player_id)",

    """
    CREATE TABLE IF NOT EXISTS fact_game_weather (
        as_of_date                    TEXT NOT NULL,
        game_id                       INTEGER NOT NULL,
        venue_id                      INTEGER,
        forecast_timestamp_utc        TEXT,
        temperature_f                 DOUBLE PRECISION,
        wind_speed_mph                DOUBLE PRECISION,
        wind_direction_deg            DOUBLE PRECISION,
        humidity_pct                  DOUBLE PRECISION,
        precipitation_probability_pct DOUBLE PRECISION,
        pressure_hpa                  DOUBLE PRECISION,
        PRIMARY KEY (as_of_date, game_id),
        FOREIGN KEY (venue_id) REFERENCES dim_venues(venue_id)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS fact_batter_overall (
        as_of_date          TEXT NOT NULL,
        player_id           INTEGER NOT NULL,
        season              INTEGER NOT NULL,
        window_code         TEXT NOT NULL,
        plate_appearances   INTEGER,
        at_bats             INTEGER,
        hits                INTEGER,
        doubles             INTEGER,
        triples             INTEGER,
        home_runs           INTEGER,
        walks               INTEGER,
        strikeouts          INTEGER,
        hit_by_pitch        INTEGER,
        sac_flies           INTEGER,
        stolen_bases        INTEGER,
        caught_stealing     INTEGER,
        batting_avg         DOUBLE PRECISION,
        on_base_pct         DOUBLE PRECISION,
        slugging_pct        DOUBLE PRECISION,
        ops                 DOUBLE PRECISION,
        iso                 DOUBLE PRECISION,
        babip               DOUBLE PRECISION,
        woba                DOUBLE PRECISION,
        xba                 DOUBLE PRECISION,
        xwoba               DOUBLE PRECISION,
        xslg                DOUBLE PRECISION,
        bb_rate             DOUBLE PRECISION,
        k_rate              DOUBLE PRECISION,
        swing_rate          DOUBLE PRECISION,
        zone_swing_rate     DOUBLE PRECISION,
        chase_rate          DOUBLE PRECISION,
        contact_rate        DOUBLE PRECISION,
        zone_contact_rate   DOUBLE PRECISION,
        whiff_rate          DOUBLE PRECISION,
        hard_hit_rate       DOUBLE PRECISION,
        barrel_rate         DOUBLE PRECISION,
        avg_exit_velocity   DOUBLE PRECISION,
        max_exit_velocity   DOUBLE PRECISION,
        sweet_spot_rate     DOUBLE PRECISION,
        ground_ball_rate    DOUBLE PRECISION,
        fly_ball_rate       DOUBLE PRECISION,
        line_drive_rate     DOUBLE PRECISION,
        pull_rate           DOUBLE PRECISION,
        opposite_field_rate DOUBLE PRECISION,
        PRIMARY KEY (as_of_date, player_id, season, window_code),
        FOREIGN KEY (player_id)    REFERENCES dim_players(player_id),
        FOREIGN KEY (window_code)  REFERENCES dim_split_windows(window_code)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_batter_overall_player_window ON fact_batter_overall(as_of_date, player_id, window_code)",

    """
    CREATE TABLE IF NOT EXISTS fact_batter_hand_splits (
        as_of_date        TEXT NOT NULL,
        player_id         INTEGER NOT NULL,
        season            INTEGER NOT NULL,
        split_hand        TEXT NOT NULL,
        window_code       TEXT NOT NULL,
        plate_appearances INTEGER,
        at_bats           INTEGER,
        hits              INTEGER,
        batting_avg       DOUBLE PRECISION,
        on_base_pct       DOUBLE PRECISION,
        slugging_pct      DOUBLE PRECISION,
        ops               DOUBLE PRECISION,
        iso               DOUBLE PRECISION,
        woba              DOUBLE PRECISION,
        xba               DOUBLE PRECISION,
        xwoba             DOUBLE PRECISION,
        bb_rate           DOUBLE PRECISION,
        k_rate            DOUBLE PRECISION,
        contact_rate      DOUBLE PRECISION,
        whiff_rate        DOUBLE PRECISION,
        hard_hit_rate     DOUBLE PRECISION,
        barrel_rate       DOUBLE PRECISION,
        PRIMARY KEY (as_of_date, player_id, season, split_hand, window_code),
        FOREIGN KEY (player_id)   REFERENCES dim_players(player_id),
        FOREIGN KEY (window_code) REFERENCES dim_split_windows(window_code)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_batter_hand_player_hand ON fact_batter_hand_splits(as_of_date, player_id, split_hand, window_code)",

    """
    CREATE TABLE IF NOT EXISTS fact_batter_pitch_type_splits (
        as_of_date        TEXT NOT NULL,
        player_id         INTEGER NOT NULL,
        season            INTEGER NOT NULL,
        split_hand        TEXT NOT NULL,
        pitch_type_code   TEXT NOT NULL,
        window_code       TEXT NOT NULL,
        pitches_seen      INTEGER,
        swings            INTEGER,
        contacts          INTEGER,
        whiffs            INTEGER,
        called_strikes    INTEGER,
        balls             INTEGER,
        in_play_events    INTEGER,
        at_bats           INTEGER,
        hits              INTEGER,
        total_bases       INTEGER,
        home_runs         INTEGER,
        batting_avg       DOUBLE PRECISION,
        slugging_pct      DOUBLE PRECISION,
        xba               DOUBLE PRECISION,
        xwoba             DOUBLE PRECISION,
        swing_rate        DOUBLE PRECISION,
        contact_rate      DOUBLE PRECISION,
        whiff_rate        DOUBLE PRECISION,
        csw_rate          DOUBLE PRECISION,
        chase_rate        DOUBLE PRECISION,
        hard_hit_rate     DOUBLE PRECISION,
        barrel_rate       DOUBLE PRECISION,
        avg_exit_velocity DOUBLE PRECISION,
        PRIMARY KEY (as_of_date, player_id, season, split_hand, pitch_type_code, window_code),
        FOREIGN KEY (player_id)       REFERENCES dim_players(player_id),
        FOREIGN KEY (pitch_type_code) REFERENCES dim_pitch_types(pitch_type_code),
        FOREIGN KEY (window_code)     REFERENCES dim_split_windows(window_code)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_batter_pt_player_pitch ON fact_batter_pitch_type_splits(as_of_date, player_id, split_hand, pitch_type_code, window_code)",

    """
    CREATE TABLE IF NOT EXISTS fact_batter_zone_splits (
        as_of_date     TEXT NOT NULL,
        player_id      INTEGER NOT NULL,
        season         INTEGER NOT NULL,
        split_hand     TEXT NOT NULL,
        zone_code      TEXT NOT NULL,
        window_code    TEXT NOT NULL,
        pitches_seen   INTEGER,
        swings         INTEGER,
        contacts       INTEGER,
        whiffs         INTEGER,
        called_strikes INTEGER,
        balls          INTEGER,
        in_play_events INTEGER,
        hits           INTEGER,
        total_bases    INTEGER,
        batting_avg    DOUBLE PRECISION,
        slugging_pct   DOUBLE PRECISION,
        xba            DOUBLE PRECISION,
        xwoba          DOUBLE PRECISION,
        swing_rate     DOUBLE PRECISION,
        chase_rate     DOUBLE PRECISION,
        contact_rate   DOUBLE PRECISION,
        whiff_rate     DOUBLE PRECISION,
        hard_hit_rate  DOUBLE PRECISION,
        barrel_rate    DOUBLE PRECISION,
        PRIMARY KEY (as_of_date, player_id, season, split_hand, zone_code, window_code),
        FOREIGN KEY (player_id)   REFERENCES dim_players(player_id),
        FOREIGN KEY (zone_code)   REFERENCES dim_zones(zone_code),
        FOREIGN KEY (window_code) REFERENCES dim_split_windows(window_code)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_batter_zone_player_zone ON fact_batter_zone_splits(as_of_date, player_id, split_hand, zone_code, window_code)",

    """
    CREATE TABLE IF NOT EXISTS fact_pitcher_overall (
        as_of_date                TEXT NOT NULL,
        pitcher_id                INTEGER NOT NULL,
        season                    INTEGER NOT NULL,
        window_code               TEXT NOT NULL,
        batters_faced             INTEGER,
        innings_pitched           DOUBLE PRECISION,
        hits_allowed              INTEGER,
        home_runs_allowed         INTEGER,
        walks                     INTEGER,
        strikeouts                INTEGER,
        hit_by_pitch              INTEGER,
        era                       DOUBLE PRECISION,
        whip                      DOUBLE PRECISION,
        fip                       DOUBLE PRECISION,
        xera                      DOUBLE PRECISION,
        xba_allowed               DOUBLE PRECISION,
        xwoba_allowed             DOUBLE PRECISION,
        xslg_allowed              DOUBLE PRECISION,
        bb_rate                   DOUBLE PRECISION,
        k_rate                    DOUBLE PRECISION,
        swing_rate_allowed        DOUBLE PRECISION,
        zone_rate                 DOUBLE PRECISION,
        first_pitch_strike_rate   DOUBLE PRECISION,
        contact_rate_allowed      DOUBLE PRECISION,
        whiff_rate                DOUBLE PRECISION,
        csw_rate                  DOUBLE PRECISION,
        chase_rate                DOUBLE PRECISION,
        hard_hit_rate_allowed     DOUBLE PRECISION,
        barrel_rate_allowed       DOUBLE PRECISION,
        avg_exit_velocity_allowed DOUBLE PRECISION,
        avg_launch_angle_allowed  DOUBLE PRECISION,
        ground_ball_rate          DOUBLE PRECISION,
        fly_ball_rate             DOUBLE PRECISION,
        line_drive_rate           DOUBLE PRECISION,
        PRIMARY KEY (as_of_date, pitcher_id, season, window_code),
        FOREIGN KEY (pitcher_id)  REFERENCES dim_players(player_id),
        FOREIGN KEY (window_code) REFERENCES dim_split_windows(window_code)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_pitcher_overall_pitcher_window ON fact_pitcher_overall(as_of_date, pitcher_id, window_code)",

    """
    CREATE TABLE IF NOT EXISTS fact_pitcher_hand_splits (
        as_of_date            TEXT NOT NULL,
        pitcher_id            INTEGER NOT NULL,
        season                INTEGER NOT NULL,
        split_hand            TEXT NOT NULL,
        window_code           TEXT NOT NULL,
        batters_faced         INTEGER,
        innings_pitched       DOUBLE PRECISION,
        batting_avg_allowed   DOUBLE PRECISION,
        on_base_pct_allowed   DOUBLE PRECISION,
        slugging_pct_allowed  DOUBLE PRECISION,
        ops_allowed           DOUBLE PRECISION,
        woba_allowed          DOUBLE PRECISION,
        xba_allowed           DOUBLE PRECISION,
        xwoba_allowed         DOUBLE PRECISION,
        bb_rate               DOUBLE PRECISION,
        k_rate                DOUBLE PRECISION,
        contact_rate_allowed  DOUBLE PRECISION,
        whiff_rate            DOUBLE PRECISION,
        csw_rate              DOUBLE PRECISION,
        chase_rate            DOUBLE PRECISION,
        zone_rate             DOUBLE PRECISION,
        hard_hit_rate_allowed DOUBLE PRECISION,
        barrel_rate_allowed   DOUBLE PRECISION,
        ground_ball_rate      DOUBLE PRECISION,
        PRIMARY KEY (as_of_date, pitcher_id, season, split_hand, window_code),
        FOREIGN KEY (pitcher_id)  REFERENCES dim_players(player_id),
        FOREIGN KEY (window_code) REFERENCES dim_split_windows(window_code)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_pitcher_hand_pitcher_hand ON fact_pitcher_hand_splits(as_of_date, pitcher_id, split_hand, window_code)",

    """
    CREATE TABLE IF NOT EXISTS fact_pitcher_pitch_mix (
        as_of_date                  TEXT NOT NULL,
        pitcher_id                  INTEGER NOT NULL,
        season                      INTEGER NOT NULL,
        split_hand                  TEXT NOT NULL,
        pitch_type_code             TEXT NOT NULL,
        window_code                 TEXT NOT NULL,
        pitches_thrown              INTEGER,
        usage_pct                   DOUBLE PRECISION,
        avg_velocity                DOUBLE PRECISION,
        max_velocity                DOUBLE PRECISION,
        avg_spin_rate               DOUBLE PRECISION,
        avg_extension               DOUBLE PRECISION,
        avg_release_height          DOUBLE PRECISION,
        avg_release_side            DOUBLE PRECISION,
        avg_horizontal_break        DOUBLE PRECISION,
        avg_vertical_break          DOUBLE PRECISION,
        avg_plate_x                 DOUBLE PRECISION,
        avg_plate_z                 DOUBLE PRECISION,
        swing_rate                  DOUBLE PRECISION,
        whiff_rate                  DOUBLE PRECISION,
        csw_rate                    DOUBLE PRECISION,
        chase_rate                  DOUBLE PRECISION,
        zone_rate                   DOUBLE PRECISION,
        first_pitch_usage_pct       DOUBLE PRECISION,
        putaway_rate                DOUBLE PRECISION,
        batting_avg_allowed         DOUBLE PRECISION,
        xwoba_allowed               DOUBLE PRECISION,
        slugging_allowed            DOUBLE PRECISION,
        hard_hit_rate_allowed       DOUBLE PRECISION,
        ground_ball_rate_on_contact DOUBLE PRECISION,
        PRIMARY KEY (as_of_date, pitcher_id, season, split_hand, pitch_type_code, window_code),
        FOREIGN KEY (pitcher_id)      REFERENCES dim_players(player_id),
        FOREIGN KEY (pitch_type_code) REFERENCES dim_pitch_types(pitch_type_code),
        FOREIGN KEY (window_code)     REFERENCES dim_split_windows(window_code)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_pitchmix_pitcher_pitch ON fact_pitcher_pitch_mix(as_of_date, pitcher_id, split_hand, pitch_type_code, window_code)",

    """
    CREATE TABLE IF NOT EXISTS fact_pitcher_zone_profile (
        as_of_date            TEXT NOT NULL,
        pitcher_id            INTEGER NOT NULL,
        season                INTEGER NOT NULL,
        split_hand            TEXT NOT NULL,
        zone_code             TEXT NOT NULL,
        pitch_type_code       TEXT NOT NULL,
        window_code           TEXT NOT NULL,
        pitches_thrown        INTEGER,
        usage_pct             DOUBLE PRECISION,
        avg_velocity          DOUBLE PRECISION,
        called_strike_rate    DOUBLE PRECISION,
        swing_rate            DOUBLE PRECISION,
        contact_rate          DOUBLE PRECISION,
        whiff_rate            DOUBLE PRECISION,
        batting_avg_allowed   DOUBLE PRECISION,
        xwoba_allowed         DOUBLE PRECISION,
        hard_hit_rate_allowed DOUBLE PRECISION,
        PRIMARY KEY (as_of_date, pitcher_id, season, split_hand, zone_code, pitch_type_code, window_code),
        FOREIGN KEY (pitcher_id)      REFERENCES dim_players(player_id),
        FOREIGN KEY (zone_code)       REFERENCES dim_zones(zone_code),
        FOREIGN KEY (pitch_type_code) REFERENCES dim_pitch_types(pitch_type_code),
        FOREIGN KEY (window_code)     REFERENCES dim_split_windows(window_code)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_pitchzone_pitcher_zone ON fact_pitcher_zone_profile(as_of_date, pitcher_id, split_hand, zone_code, pitch_type_code, window_code)",

    """
    CREATE TABLE IF NOT EXISTS fact_team_lineup_vs_hand (
        as_of_date             TEXT NOT NULL,
        game_id                INTEGER NOT NULL,
        team_id                INTEGER NOT NULL,
        split_hand             TEXT NOT NULL,
        window_code            TEXT NOT NULL,
        projected_lineup_spots INTEGER,
        projected_pa_weight    DOUBLE PRECISION,
        batting_avg_vs_hand    DOUBLE PRECISION,
        woba_vs_hand           DOUBLE PRECISION,
        k_rate_vs_hand         DOUBLE PRECISION,
        hard_hit_rate_vs_hand  DOUBLE PRECISION,
        barrel_rate_vs_hand    DOUBLE PRECISION,
        PRIMARY KEY (as_of_date, game_id, team_id, split_hand, window_code),
        FOREIGN KEY (team_id)     REFERENCES dim_teams(team_id),
        FOREIGN KEY (window_code) REFERENCES dim_split_windows(window_code)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS fact_matchup_batter_pitcher (
        as_of_date                          TEXT NOT NULL,
        game_id                             INTEGER NOT NULL,
        batter_id                           INTEGER NOT NULL,
        pitcher_id                          INTEGER NOT NULL,
        window_code                         TEXT NOT NULL,
        team_id                             INTEGER,
        opponent_team_id                    INTEGER,
        batter_vs_hand_batting_avg          DOUBLE PRECISION,
        batter_vs_hand_woba                 DOUBLE PRECISION,
        pitcher_vs_hand_batting_avg_allowed DOUBLE PRECISION,
        pitcher_vs_hand_k_rate              DOUBLE PRECISION,
        pitch_type_match_score              DOUBLE PRECISION,
        zone_match_score                    DOUBLE PRECISION,
        contact_match_score                 DOUBLE PRECISION,
        power_match_score                   DOUBLE PRECISION,
        park_adjustment_factor              DOUBLE PRECISION,
        weather_adjustment_factor           DOUBLE PRECISION,
        projected_batting_avg               DOUBLE PRECISION,
        projected_hit_probability           DOUBLE PRECISION,
        projected_total_bases_index         DOUBLE PRECISION,
        projected_strikeout_risk            DOUBLE PRECISION,
        PRIMARY KEY (as_of_date, game_id, batter_id, pitcher_id, window_code),
        FOREIGN KEY (batter_id)          REFERENCES dim_players(player_id),
        FOREIGN KEY (pitcher_id)         REFERENCES dim_players(player_id),
        FOREIGN KEY (window_code)        REFERENCES dim_split_windows(window_code),
        FOREIGN KEY (team_id)            REFERENCES dim_teams(team_id),
        FOREIGN KEY (opponent_team_id)   REFERENCES dim_teams(team_id)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_matchup_game    ON fact_matchup_batter_pitcher(as_of_date, game_id)",
    "CREATE INDEX IF NOT EXISTS idx_matchup_batter  ON fact_matchup_batter_pitcher(as_of_date, batter_id, window_code)",
    "CREATE INDEX IF NOT EXISTS idx_matchup_pitcher ON fact_matchup_batter_pitcher(as_of_date, pitcher_id, window_code)",

    # ── View ───────────────────────────────────────────────────────────────

    """
    CREATE OR REPLACE VIEW vw_projected_lineup_matchups AS
    SELECT
        l.as_of_date,
        l.game_id,
        l.team_id,
        l.player_id   AS batter_id,
        l.lineup_slot,
        l.confirmed_flag,
        l.projected_flag,
        l.opponent_pitcher_id AS pitcher_id,
        l.opponent_pitcher_hand,
        m.window_code,
        m.projected_batting_avg,
        m.projected_hit_probability,
        m.projected_total_bases_index,
        m.projected_strikeout_risk
    FROM fact_game_lineups l
    LEFT JOIN fact_matchup_batter_pitcher m
      ON  m.as_of_date  = l.as_of_date
      AND m.game_id     = l.game_id
      AND m.batter_id   = l.player_id
      AND m.pitcher_id  = l.opponent_pitcher_id
    """,

    # ── pipeline_runs audit table (new for cloud) ──────────────────────────

    """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        id              SERIAL PRIMARY KEY,
        run_at          TIMESTAMPTZ DEFAULT NOW(),
        games_fetched   INTEGER DEFAULT 0,
        games_predicted INTEGER DEFAULT 0,
        status          TEXT DEFAULT 'running',
        error_msg       TEXT,
        duration_s      DOUBLE PRECISION,
        git_sha         TEXT
    )
    """,
]

# =============================================================================
# Seed data  (INSERT ... ON CONFLICT DO NOTHING = idempotent, replaces
#             SQLite's INSERT OR IGNORE)
# =============================================================================

PITCH_TYPES = [
    ("FF",  "Four-Seam Fastball",    "fastball",  "hard"),
    ("SI",  "Sinker",                "fastball",  "hard"),
    ("FC",  "Cut Fastball (Cutter)", "fastball",  "hard"),
    ("SL",  "Slider",                "breaking",  "medium"),
    ("ST",  "Sweeper",               "breaking",  "medium"),
    ("CU",  "Curveball",             "breaking",  "medium"),
    ("KC",  "Knuckle Curve",         "breaking",  "medium"),
    ("CH",  "Changeup",              "offspeed",  "soft"),
    ("FS",  "Splitter",              "offspeed",  "soft"),
    ("SV",  "Screwball",             "offspeed",  "soft"),
    ("KN",  "Knuckleball",           "other",     "soft"),
    ("EP",  "Eephus",                "other",     "soft"),
    ("CS",  "Slow Curve",            "breaking",  "soft"),
    ("FO",  "Forkball",              "offspeed",  "soft"),
    ("PO",  "Pitch Out",             "other",     "hard"),
]

ZONES = [
    ("Z1",         "high-inside",   "IN_ZONE", 1, 1, 1, -0.83,  0.00, 3.00, 3.67),
    ("Z2",         "high-middle",   "IN_ZONE", 1, 2, 1, -0.28,  0.28, 3.00, 3.67),
    ("Z3",         "high-outside",  "IN_ZONE", 1, 3, 1,  0.00,  0.83, 3.00, 3.67),
    ("Z4",         "middle-inside", "IN_ZONE", 2, 1, 1, -0.83,  0.00, 2.33, 3.00),
    ("Z5",         "middle-middle", "IN_ZONE", 2, 2, 1, -0.28,  0.28, 2.33, 3.00),
    ("Z6",         "middle-outside","IN_ZONE", 2, 3, 1,  0.00,  0.83, 2.33, 3.00),
    ("Z7",         "low-inside",    "IN_ZONE", 3, 1, 1, -0.83,  0.00, 1.50, 2.33),
    ("Z8",         "low-middle",    "IN_ZONE", 3, 2, 1, -0.28,  0.28, 1.50, 2.33),
    ("Z9",         "low-outside",   "IN_ZONE", 3, 3, 1,  0.00,  0.83, 1.50, 2.33),
    ("CHASE_UP",   "chase-up",      "CHASE",   0, 0, 0, -1.50,  1.50, 3.67, 5.00),
    ("CHASE_DOWN", "chase-down",    "CHASE",   0, 0, 0, -1.50,  1.50, 0.50, 1.50),
    ("CHASE_IN",   "chase-inside",  "CHASE",   0, 0, 0, -1.50, -0.83, 1.50, 3.67),
    ("CHASE_OUT",  "chase-outside", "CHASE",   0, 0, 0,  0.83,  1.50, 1.50, 3.67),
]

SPLIT_WINDOWS = [
    ("SEASON", "Full Season",  "season_start",   "as_of_date", 50, 50, 1.0),
    ("L30D",   "Last 30 Days", "as_of_date-30d", "as_of_date", 20, 20, 0.85),
    ("L14D",   "Last 14 Days", "as_of_date-14d", "as_of_date", 10, 10, 0.70),
    ("L7D",    "Last 7 Days",  "as_of_date-7d",  "as_of_date",  5,  5, 0.50),
]


def seed(engine):
    from sqlalchemy import text
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO dim_pitch_types (pitch_type_code, pitch_type_name, pitch_group, velocity_band)
            VALUES (:a,:b,:c,:d)
            ON CONFLICT (pitch_type_code) DO NOTHING
        """), [dict(a=r[0], b=r[1], c=r[2], d=r[3]) for r in PITCH_TYPES])

        conn.execute(text("""
            INSERT INTO dim_zones
              (zone_code,zone_bucket,zone_group,zone_row,zone_col,
               in_strike_zone_flag,x_min,x_max,z_min,z_max)
            VALUES (:a,:b,:c,:d,:e,:f,:g,:h,:i,:j)
            ON CONFLICT (zone_code) DO NOTHING
        """), [dict(a=r[0],b=r[1],c=r[2],d=r[3],e=r[4],
                    f=r[5],g=r[6],h=r[7],i=r[8],j=r[9]) for r in ZONES])

        conn.execute(text("""
            INSERT INTO dim_split_windows
              (window_code,window_name,window_start_rule,window_end_rule,
               min_pa_threshold,min_bf_threshold,regression_weight)
            VALUES (:a,:b,:c,:d,:e,:f,:g)
            ON CONFLICT (window_code) DO NOTHING
        """), [dict(a=r[0],b=r[1],c=r[2],d=r[3],e=r[4],f=r[5],g=r[6])
               for r in SPLIT_WINDOWS])

    print("  ✓ seed data (pitch types, zones, split windows)")


def main():
    if DB_BACKEND != "supabase":
        print(f"[ERROR] DB_BACKEND={DB_BACKEND!r} — set DB_BACKEND=supabase first.")
        sys.exit(1)

    print("[migrate] Connecting to Supabase...")
    if not ping():
        print("[migrate] Cannot reach Supabase. Check SUPABASE_DB_URL in .env")
        sys.exit(1)

    print(f"[migrate] Running {len(DDL_STATEMENTS)} DDL statements...")
    for stmt in DDL_STATEMENTS:
        stmt = stmt.strip()
        if not stmt:
            continue
        # derive a label for logging
        first_line = stmt.splitlines()[0].upper()
        if "CREATE TABLE" in first_line or "CREATE OR REPLACE VIEW" in first_line or "CREATE VIEW" in first_line:
            label = stmt.split()[-1] if "VIEW" in first_line else [
                w for w in stmt.replace("\n", " ").split() if w not in
                ("CREATE","TABLE","IF","NOT","EXISTS")][0]
            print(f"  ✓ {label.strip('(')}")
        try:
            execute(stmt)
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
            raise

    from utils.db import get_engine
    print("[migrate] Seeding reference tables...")
    seed(get_engine())

    print("\n[migrate] Schema migration complete ✓")
    print("         Tables and seed data are live in Supabase.")


if __name__ == "__main__":
    main()
