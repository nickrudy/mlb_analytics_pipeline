"""
init_db.py
----------
Creates (or validates) the full MLB pre-game SQLite database.
Reflects the complete current schema including all columns added
via migration scripts throughout the 2026 season.

Run this once before any ingestion scripts:
    python db/init_db.py [--db-path path/to/mlb.db]
"""
import sqlite3
import argparse
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

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
        lat                   REAL,
        lon                   REAL,
        altitude_ft           INTEGER,
        park_run_factor       REAL,
        park_hr_factor_lhb    REAL,
        park_hr_factor_rhb    REAL,
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
        x_min               REAL,
        x_max               REAL,
        z_min               REAL,
        z_max               REAL,
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
        regression_weight  REAL,
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
        plate_x                         REAL,
        plate_z                         REAL,
        release_speed                   REAL,
        release_spin_rate               REAL,
        release_extension               REAL,
        release_pos_x                   REAL,
        release_pos_z                   REAL,
        pfx_x                           REAL,
        pfx_z                           REAL,
        description                     TEXT,
        events                          TEXT,
        bb_type                         TEXT,
        launch_speed                    REAL,
        launch_angle                    REAL,
        estimated_ba_using_speedangle   REAL,
        estimated_woba_using_speedangle REAL,
        raw_payload_json                TEXT,
        hc_x                            REAL,
        hc_y                            REAL,
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
        temperature_f                 REAL,
        wind_speed_mph                REAL,
        wind_direction_deg            REAL,
        humidity_pct                  REAL,
        precipitation_probability_pct REAL,
        pressure_hpa                  REAL,
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
        temperature_f                 REAL,
        wind_speed_mph                REAL,
        wind_direction_deg            REAL,
        humidity_pct                  REAL,
        precipitation_probability_pct REAL,
        pressure_hpa                  REAL,
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
        batting_avg         REAL,
        on_base_pct         REAL,
        slugging_pct        REAL,
        ops                 REAL,
        iso                 REAL,
        babip               REAL,
        woba                REAL,
        xba                 REAL,
        xwoba               REAL,
        xslg                REAL,
        bb_rate             REAL,
        k_rate              REAL,
        swing_rate          REAL,
        zone_swing_rate     REAL,
        chase_rate          REAL,
        contact_rate        REAL,
        zone_contact_rate   REAL,
        whiff_rate          REAL,
        hard_hit_rate       REAL,
        barrel_rate         REAL,
        avg_exit_velocity   REAL,
        max_exit_velocity   REAL,
        sweet_spot_rate     REAL,
        ground_ball_rate    REAL,
        fly_ball_rate       REAL,
        line_drive_rate     REAL,
        pull_rate           REAL,
        opposite_field_rate REAL,
        games_played        INTEGER,
        ab_per_game         REAL,
        PRIMARY KEY (as_of_date, player_id, season, window_code),
        FOREIGN KEY (player_id)   REFERENCES dim_players(player_id),
        FOREIGN KEY (window_code) REFERENCES dim_split_windows(window_code)
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
        batting_avg       REAL,
        on_base_pct       REAL,
        slugging_pct      REAL,
        ops               REAL,
        iso               REAL,
        woba              REAL,
        xba               REAL,
        xwoba             REAL,
        bb_rate           REAL,
        k_rate            REAL,
        contact_rate      REAL,
        whiff_rate        REAL,
        hard_hit_rate     REAL,
        barrel_rate       REAL,
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
        batting_avg       REAL,
        slugging_pct      REAL,
        xba               REAL,
        xwoba             REAL,
        swing_rate        REAL,
        contact_rate      REAL,
        whiff_rate        REAL,
        csw_rate          REAL,
        chase_rate        REAL,
        hard_hit_rate     REAL,
        barrel_rate       REAL,
        avg_exit_velocity REAL,
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
        batting_avg    REAL,
        slugging_pct   REAL,
        xba            REAL,
        xwoba          REAL,
        swing_rate     REAL,
        chase_rate     REAL,
        contact_rate   REAL,
        whiff_rate     REAL,
        hard_hit_rate  REAL,
        barrel_rate    REAL,
        PRIMARY KEY (as_of_date, player_id, season, split_hand, zone_code, window_code),
        FOREIGN KEY (player_id)   REFERENCES dim_players(player_id),
        FOREIGN KEY (zone_code)   REFERENCES dim_zones(zone_code),
        FOREIGN KEY (window_code) REFERENCES dim_split_windows(window_code)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_batter_zone_player_zone ON fact_batter_zone_splits(as_of_date, player_id, split_hand, zone_code, window_code)",

    """
    CREATE TABLE IF NOT EXISTS fact_batter_power_profile (
        as_of_date            TEXT    NOT NULL,
        player_id             INTEGER NOT NULL,
        season                INTEGER NOT NULL,
        window_code           TEXT    NOT NULL,
        batted_ball_events    INTEGER,
        plate_appearances     INTEGER,
        at_bats               INTEGER,
        barrels               INTEGER,
        barrels_per_pa        REAL,
        barrels_per_bbe       REAL,
        hard_hit_count        INTEGER,
        hard_hit_rate         REAL,
        avg_exit_velocity     REAL,
        max_exit_velocity     REAL,
        avg_launch_angle      REAL,
        xba                   REAL,
        xwoba                 REAL,
        home_runs             INTEGER,
        hr_per_pa             REAL,
        hr_per_bbe            REAL,
        fly_ball_rate         REAL,
        ground_ball_rate      REAL,
        line_drive_rate       REAL,
        pull_rate             REAL,
        oppo_rate             REAL,
        barrels_per_pa_vs_rhp REAL,
        barrels_per_pa_vs_lhp REAL,
        hard_hit_rate_vs_rhp  REAL,
        hard_hit_rate_vs_lhp  REAL,
        avg_ev_vs_rhp         REAL,
        avg_ev_vs_lhp         REAL,
        hard_hit_rate_last10  REAL,
        hard_hit_rate_prior   REAL,
        recency_raw_diff      REAL,
        PRIMARY KEY (as_of_date, player_id, season, window_code),
        FOREIGN KEY (player_id)   REFERENCES dim_players(player_id),
        FOREIGN KEY (window_code) REFERENCES dim_split_windows(window_code)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_power_profile_player ON fact_batter_power_profile(as_of_date, player_id, window_code)",

    """
    CREATE TABLE IF NOT EXISTS fact_pitcher_overall (
        as_of_date                TEXT NOT NULL,
        pitcher_id                INTEGER NOT NULL,
        season                    INTEGER NOT NULL,
        window_code               TEXT NOT NULL,
        batters_faced             INTEGER,
        innings_pitched           REAL,
        hits_allowed              INTEGER,
        home_runs_allowed         INTEGER,
        walks                     INTEGER,
        strikeouts                INTEGER,
        hit_by_pitch              INTEGER,
        era                       REAL,
        whip                      REAL,
        fip                       REAL,
        xera                      REAL,
        xba_allowed               REAL,
        xwoba_allowed             REAL,
        xslg_allowed              REAL,
        bb_rate                   REAL,
        k_rate                    REAL,
        swing_rate_allowed        REAL,
        zone_rate                 REAL,
        first_pitch_strike_rate   REAL,
        contact_rate_allowed      REAL,
        whiff_rate                REAL,
        csw_rate                  REAL,
        chase_rate                REAL,
        hard_hit_rate_allowed     REAL,
        barrel_rate_allowed       REAL,
        avg_exit_velocity_allowed REAL,
        avg_launch_angle_allowed  REAL,
        ground_ball_rate          REAL,
        fly_ball_rate             REAL,
        line_drive_rate           REAL,
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
        innings_pitched       REAL,
        batting_avg_allowed   REAL,
        on_base_pct_allowed   REAL,
        slugging_pct_allowed  REAL,
        ops_allowed           REAL,
        woba_allowed          REAL,
        xba_allowed           REAL,
        xwoba_allowed         REAL,
        bb_rate               REAL,
        k_rate                REAL,
        contact_rate_allowed  REAL,
        whiff_rate            REAL,
        csw_rate              REAL,
        chase_rate            REAL,
        zone_rate             REAL,
        hard_hit_rate_allowed REAL,
        barrel_rate_allowed   REAL,
        ground_ball_rate      REAL,
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
        usage_pct                   REAL,
        avg_velocity                REAL,
        max_velocity                REAL,
        avg_spin_rate               REAL,
        avg_extension               REAL,
        avg_release_height          REAL,
        avg_release_side            REAL,
        avg_horizontal_break        REAL,
        avg_vertical_break          REAL,
        avg_plate_x                 REAL,
        avg_plate_z                 REAL,
        swing_rate                  REAL,
        whiff_rate                  REAL,
        csw_rate                    REAL,
        chase_rate                  REAL,
        zone_rate                   REAL,
        first_pitch_usage_pct       REAL,
        putaway_rate                REAL,
        batting_avg_allowed         REAL,
        xwoba_allowed               REAL,
        slugging_allowed            REAL,
        hard_hit_rate_allowed       REAL,
        ground_ball_rate_on_contact REAL,
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
        usage_pct             REAL,
        avg_velocity          REAL,
        called_strike_rate    REAL,
        swing_rate            REAL,
        contact_rate          REAL,
        whiff_rate            REAL,
        batting_avg_allowed   REAL,
        xwoba_allowed         REAL,
        hard_hit_rate_allowed REAL,
        PRIMARY KEY (as_of_date, pitcher_id, season, split_hand, zone_code, pitch_type_code, window_code),
        FOREIGN KEY (pitcher_id)      REFERENCES dim_players(player_id),
        FOREIGN KEY (zone_code)       REFERENCES dim_zones(zone_code),
        FOREIGN KEY (pitch_type_code) REFERENCES dim_pitch_types(pitch_type_code),
        FOREIGN KEY (window_code)     REFERENCES dim_split_windows(window_code)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_pitchzone_pitcher_zone ON fact_pitcher_zone_profile(as_of_date, pitcher_id, split_hand, zone_code, pitch_type_code, window_code)",

    """
    CREATE TABLE IF NOT EXISTS fact_pitcher_hr_vulnerability (
        as_of_date                  TEXT    NOT NULL,
        pitcher_id                  INTEGER NOT NULL,
        season                      INTEGER NOT NULL,
        split_hand                  TEXT    NOT NULL,
        window_code                 TEXT    NOT NULL,
        batted_ball_events          INTEGER,
        batters_faced               INTEGER,
        barrels_allowed             INTEGER,
        barrel_rate_allowed         REAL,
        hard_hit_rate_allowed       REAL,
        avg_exit_velocity_allowed   REAL,
        max_exit_velocity_allowed   REAL,
        home_runs_allowed           INTEGER,
        hr_per_bbe_allowed          REAL,
        hr_per_bf_allowed           REAL,
        xwoba_allowed               REAL,
        fly_ball_rate_allowed       REAL,
        ground_ball_rate_allowed    REAL,
        line_drive_rate_allowed     REAL,
        barrel_rate_on_fastballs    REAL,
        barrel_rate_on_breaking     REAL,
        barrel_rate_on_offspeed     REAL,
        hr_rate_on_fastballs        REAL,
        hr_rate_on_breaking         REAL,
        hr_rate_on_offspeed         REAL,
        PRIMARY KEY (as_of_date, pitcher_id, season, split_hand, window_code),
        FOREIGN KEY (pitcher_id)  REFERENCES dim_players(player_id),
        FOREIGN KEY (window_code) REFERENCES dim_split_windows(window_code)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_pitcher_hr_vuln_pitcher ON fact_pitcher_hr_vulnerability(as_of_date, pitcher_id, split_hand, window_code)",

    """
    CREATE TABLE IF NOT EXISTS fact_team_lineup_vs_hand (
        as_of_date             TEXT NOT NULL,
        game_id                INTEGER NOT NULL,
        team_id                INTEGER NOT NULL,
        split_hand             TEXT NOT NULL,
        window_code            TEXT NOT NULL,
        projected_lineup_spots INTEGER,
        projected_pa_weight    REAL,
        batting_avg_vs_hand    REAL,
        woba_vs_hand           REAL,
        k_rate_vs_hand         REAL,
        hard_hit_rate_vs_hand  REAL,
        barrel_rate_vs_hand    REAL,
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
        batter_vs_hand_batting_avg          REAL,
        batter_vs_hand_woba                 REAL,
        pitcher_vs_hand_batting_avg_allowed REAL,
        pitcher_vs_hand_k_rate              REAL,
        pitch_type_match_score              REAL,
        zone_match_score                    REAL,
        contact_match_score                 REAL,
        power_match_score                   REAL,
        park_adjustment_factor              REAL,
        weather_adjustment_factor           REAL,
        projected_batting_avg               REAL,
        projected_hit_probability           REAL,
        projected_total_bases_index         REAL,
        projected_strikeout_risk            REAL,
        proj_at_bats_per_game               REAL,
        pt_slg_score                        REAL,
        zone_slg_score                      REAL,
        projected_slugging                  REAL,
        projected_total_bases               REAL,
        projected_hr_probability            REAL,
        batter_barrel_rate                  REAL,
        pitcher_barrel_rate_allowed         REAL,
        recency_raw_diff                    REAL,
        batter_vs_hand_on_base_pct          REAL,
        pitcher_vs_hand_on_base_pct_allowed REAL,
        projected_on_base_pct               REAL,
        proj_plate_appearances_per_game     REAL,
        projected_times_on_base             REAL,
        ingested_at                         TEXT,
        PRIMARY KEY (as_of_date, game_id, batter_id, pitcher_id, window_code),
        FOREIGN KEY (batter_id)        REFERENCES dim_players(player_id),
        FOREIGN KEY (pitcher_id)       REFERENCES dim_players(player_id),
        FOREIGN KEY (window_code)      REFERENCES dim_split_windows(window_code),
        FOREIGN KEY (team_id)          REFERENCES dim_teams(team_id),
        FOREIGN KEY (opponent_team_id) REFERENCES dim_teams(team_id)
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_matchup_game    ON fact_matchup_batter_pitcher(as_of_date, game_id)",
    "CREATE INDEX IF NOT EXISTS idx_matchup_batter  ON fact_matchup_batter_pitcher(as_of_date, batter_id, window_code)",
    "CREATE INDEX IF NOT EXISTS idx_matchup_pitcher ON fact_matchup_batter_pitcher(as_of_date, pitcher_id, window_code)",

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
        batting_avg        REAL,
        slugging_pct       REAL,
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
    "CREATE INDEX IF NOT EXISTS idx_boxscore_game        ON fact_player_game_results(game_id, game_date)",

    # ── View ───────────────────────────────────────────────────────────────

    """
    CREATE VIEW IF NOT EXISTS vw_projected_lineup_matchups AS
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

    # ── Looker Studio flat export tables ──────────────────────────────────
    # Truncated and rewritten each pipeline run by export_to_daily_tables.py.
    # Simple flat selects — no joins or window functions at read time.

    """
    CREATE TABLE IF NOT EXISTS daily_top_batting (
        batter_name         TEXT,
        batter_team         TEXT,
        game_datetime_ct    TEXT,
        final_projection    REAL,
        baseline_avg        REAL,
        delta               REAL,
        projected_on_base_pct              REAL,
        proj_plate_appearances_per_game    REAL,
        projected_times_on_base            REAL,
        refreshed_at        TEXT
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS daily_top_bases (
        batter_name             TEXT,
        batter_team             TEXT,
        game_datetime_ct        TEXT,
        final_projection        REAL,
        baseline_avg            REAL,
        delta                   REAL,
        projected_total_bases   REAL,
        refreshed_at            TEXT
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS daily_top_hrs (
        batter_name                 TEXT,
        batter_team                 TEXT,
        game_datetime_ct            TEXT,
        final_projection            REAL,
        baseline_avg                REAL,
        delta                       REAL,
        projected_hr_probability    REAL,
        refreshed_at                TEXT
    )
    """,
]

# ── Seed data ──────────────────────────────────────────────────────────────

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


def init_db(db_path: str) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=OFF;")
    with conn:
        for stmt in DDL_STATEMENTS:
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)
        conn.executemany(
            "INSERT OR IGNORE INTO dim_pitch_types VALUES (?,?,?,?)",
            PITCH_TYPES,
        )
        conn.executemany(
            "INSERT OR IGNORE INTO dim_zones VALUES (?,?,?,?,?,?,?,?,?,?)",
            ZONES,
        )
        conn.executemany(
            "INSERT OR IGNORE INTO dim_split_windows VALUES (?,?,?,?,?,?,?)",
            SPLIT_WINDOWS,
        )
    conn.close()
    log.info("Database initialised at %s", path)
    conn = sqlite3.connect(str(path))
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    views = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
    ).fetchall()
    conn.close()
    log.info("Tables created: %s", [t[0] for t in tables])
    log.info("Views created:  %s", [v[0] for v in views])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialise MLB pre-game SQLite database")
    parser.add_argument("--db-path", default="data/mlb_pregame.db")
    args = parser.parse_args()
    init_db(args.db_path)
