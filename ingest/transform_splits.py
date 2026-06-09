"""
transform_splits.py
--------------------
Reads raw pitch-level data from stg_statcast_pitches and computes all
split fact tables:
    - fact_batter_overall
    - fact_batter_hand_splits
    - fact_batter_pitch_type_splits
    - fact_batter_zone_splits
    - fact_pitcher_overall
    - fact_pitcher_hand_splits
    - fact_pitcher_pitch_mix
    - fact_pitcher_zone_profile

Also computes fact_matchup_batter_pitcher for games in fact_game_lineups.

Usage:
    pip install pandas numpy
    python ingest/transform_splits.py --date 2025-04-15 --db-path data/mlb_pregame.db
    python ingest/transform_splits.py --season 2025 --db-path data/mlb_pregame.db
"""

import sqlite3
import logging
import argparse
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

try:
    import pandas as pd
    import numpy as np
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    log.error("pandas / numpy not installed. Run: pip install pandas numpy")


# ── Zone classification ────────────────────────────────────────────────────

def _assign_zone(plate_x, plate_z):
    """Map plate coordinates to normalized zone_code."""
    if plate_x is None or plate_z is None:
        return None
    x, z = float(plate_x), float(plate_z)
    # Strike zone boundaries (feet): x ±0.83, z 1.50-3.67
    in_x   = -0.83 <= x <= 0.83
    in_z   =  1.50 <= z <= 3.67
    if in_x and in_z:
        # 3x3 grid
        col = 1 if x < -0.28 else (2 if x <= 0.28 else 3)
        row = 1 if z >= 3.00 else (2 if z >= 2.33 else 3)
        return f"Z{(row-1)*3 + col}"
    else:
        if z > 3.67:                 return "CHASE_UP"
        if z < 1.50:                 return "CHASE_DOWN"
        if x < -0.83 and in_z:      return "CHASE_IN"
        if x >  0.83 and in_z:      return "CHASE_OUT"
        return None


# ── Hit classification helpers ─────────────────────────────────────────────

HIT_EVENTS     = {"single", "double", "triple", "home_run"}
EXTRA_BASE     = {"double": 2, "triple": 3, "home_run": 4}
SWING_DESCS    = {"swinging_strike", "swinging_strike_blocked", "foul",
                  "foul_tip", "hit_into_play", "foul_bunt", "missed_bunt",
                  "in_play_out", "in_play_score", "in_play_no_out"}
WHIFF_DESCS    = {"swinging_strike", "swinging_strike_blocked"}
CONTACT_DESCS  = {"foul", "foul_tip", "hit_into_play",
                  "in_play_out", "in_play_score", "in_play_no_out"}
IN_PLAY_DESCS  = {"hit_into_play", "in_play_out", "in_play_score", "in_play_no_out"}
CALLED_STRIKE  = {"called_strike"}

AT_BAT_END     = {"single","double","triple","home_run","strikeout","strikeout_double_play",
                  "field_out","force_out","grounded_into_double_play","double_play",
                  "triple_play","field_error","fielders_choice","fielders_choice_out",
                  "hit_by_pitch","sac_fly","sac_bunt","sac_fly_double_play"}

HARD_HIT_MPH   = 95.0


# ── Per-pitch flag columns ─────────────────────────────────────────────────

def _enrich(df: "pd.DataFrame") -> "pd.DataFrame":
    """Add boolean flag columns to a pitch DataFrame."""
    desc = df["description"].fillna("").str.lower()
    evts = df["events"].fillna("").str.lower()

    df["is_swing"]      = desc.isin(SWING_DESCS)
    df["is_whiff"]      = desc.isin(WHIFF_DESCS)
    df["is_contact"]    = desc.isin(CONTACT_DESCS)
    df["is_in_play"]    = desc.isin(IN_PLAY_DESCS)
    df["is_called_str"] = desc.isin(CALLED_STRIKE)
    df["is_ball"]       = desc == "ball"
    df["is_hit"]        = evts.isin(HIT_EVENTS)
    df["is_ab_end"]     = evts.isin(AT_BAT_END)
    df["is_hard_hit"]   = (df["launch_speed"] >= HARD_HIT_MPH)
    df["is_barrel"]     = (
        (df["launch_speed"] >= 98.0) &
        (df["launch_angle"].between(26, 30))
    )
    df["total_bases"] = evts.map(
        lambda e: 1 if e == "single" else EXTRA_BASE.get(e, 0)
    )
    df["is_home_run"]   = evts == "home_run"
    df["is_in_zone"]    = df["zone_code"].str.startswith("Z", na=False)
    df["is_chase"]      = df["zone_code"].str.startswith("CHASE", na=False)
    df["is_chase_swing"]= df["is_swing"] & df["is_chase"]
    df["is_zone_swing"] = df["is_swing"] & df["is_in_zone"]
    df["is_zone_contact"]= df["is_contact"] & df["is_in_zone"]
    return df


# ── Aggregate helpers ──────────────────────────────────────────────────────

def _safe_div(num, denom):
    return round(float(num) / float(denom), 6) if denom and denom > 0 else None


def _batter_agg(grp: "pd.DataFrame") -> dict:
    pa  = grp["is_ab_end"].sum()           # approximation; real PA needs HBP/SF
    ab  = grp["is_ab_end"].sum()
    h   = grp["is_hit"].sum()
    hr  = grp["is_home_run"].sum()
    swings   = grp["is_swing"].sum()
    contacts = grp["is_contact"].sum()
    whiffs   = grp["is_whiff"].sum()
    in_zone  = grp["is_in_zone"].sum()
    z_swing  = grp["is_zone_swing"].sum()
    z_contact= grp["is_zone_contact"].sum()
    chase    = grp["is_chase"].sum()
    c_swing  = grp["is_chase_swing"].sum()
    in_play  = grp["is_in_play"].sum()
    hard_hit = grp["is_hard_hit"].sum()
    barrels  = grp["is_barrel"].sum()
    tb       = grp["total_bases"].sum()
    pitches  = len(grp)

    avg = _safe_div(h, ab)
    slg = _safe_div(tb, ab)

    # Games played — count distinct game_pks if available, else estimate from PA
    if "game_pk" in grp.columns:
        games = grp["game_pk"].nunique()
    else:
        games = max(1, int(pa / 4))   # rough fallback: ~4 PA per game
    ab_per_game = _safe_div(ab, games)

    return {
        "plate_appearances": int(pa),
        "at_bats":           int(ab),
        "hits":              int(h),
        "home_runs":         int(hr),
        "batting_avg":       avg,
        "slugging_pct":      slg,
        "games_played":      int(games),
        "ab_per_game":       ab_per_game,
        "xba":  grp["estimated_ba_using_speedangle"].mean()  if "estimated_ba_using_speedangle"  in grp else None,
        "xwoba":grp["estimated_woba_using_speedangle"].mean() if "estimated_woba_using_speedangle" in grp else None,
        "swing_rate":        _safe_div(swings, pitches),
        "zone_swing_rate":   _safe_div(z_swing, in_zone),
        "chase_rate":        _safe_div(c_swing, chase),
        "contact_rate":      _safe_div(contacts, swings),
        "zone_contact_rate": _safe_div(z_contact, z_swing),
        "whiff_rate":        _safe_div(whiffs, swings),
        "hard_hit_rate":     _safe_div(hard_hit, in_play),
        "barrel_rate":       _safe_div(barrels, in_play),
        "avg_exit_velocity": grp["launch_speed"].mean() if "launch_speed" in grp else None,
    }


def _pitch_type_agg(grp: "pd.DataFrame") -> dict:
    pitches  = len(grp)
    swings   = grp["is_swing"].sum()
    contacts = grp["is_contact"].sum()
    whiffs   = grp["is_whiff"].sum()
    c_strikes= grp["is_called_str"].sum()
    balls    = grp["is_ball"].sum()
    in_play  = grp["is_in_play"].sum()
    ab       = grp["is_ab_end"].sum()
    h        = grp["is_hit"].sum()
    tb       = grp["total_bases"].sum()
    hr       = grp["is_home_run"].sum()
    hard_hit = grp["is_hard_hit"].sum()
    barrels  = grp["is_barrel"].sum()
    chase    = grp["is_chase"].sum()
    c_swing  = grp["is_chase_swing"].sum()

    return {
        "pitches_seen":   int(pitches),
        "swings":         int(swings),
        "contacts":       int(contacts),
        "whiffs":         int(whiffs),
        "called_strikes": int(c_strikes),
        "balls":          int(balls),
        "in_play_events": int(in_play),
        "at_bats":        int(ab),
        "hits":           int(h),
        "total_bases":    int(tb),
        "home_runs":      int(hr),
        "batting_avg":    _safe_div(h, ab),
        "slugging_pct":   _safe_div(tb, ab),
        "xba":   grp["estimated_ba_using_speedangle"].mean()  if "estimated_ba_using_speedangle"  in grp else None,
        "xwoba": grp["estimated_woba_using_speedangle"].mean() if "estimated_woba_using_speedangle" in grp else None,
        "swing_rate":   _safe_div(swings, pitches),
        "contact_rate": _safe_div(contacts, swings),
        "whiff_rate":   _safe_div(whiffs, swings),
        "csw_rate":     _safe_div(whiffs + c_strikes, pitches),
        "chase_rate":   _safe_div(c_swing, chase),
        "hard_hit_rate":_safe_div(hard_hit, in_play),
        "barrel_rate":  _safe_div(barrels, in_play),
        "avg_exit_velocity": grp["launch_speed"].mean() if "launch_speed" in grp else None,
    }


def _zone_agg(grp: "pd.DataFrame") -> dict:
    pitches  = len(grp)
    swings   = grp["is_swing"].sum()
    contacts = grp["is_contact"].sum()
    whiffs   = grp["is_whiff"].sum()
    c_strikes= grp["is_called_str"].sum()
    balls    = grp["is_ball"].sum()
    in_play  = grp["is_in_play"].sum()
    h        = grp["is_hit"].sum()
    tb       = grp["total_bases"].sum()
    hard_hit = grp["is_hard_hit"].sum()
    barrels  = grp["is_barrel"].sum()
    chase    = grp["is_chase"].sum()
    c_swing  = grp["is_chase_swing"].sum()

    return {
        "pitches_seen":   int(pitches),
        "swings":         int(swings),
        "contacts":       int(contacts),
        "whiffs":         int(whiffs),
        "called_strikes": int(c_strikes),
        "balls":          int(balls),
        "in_play_events": int(in_play),
        "hits":           int(h),
        "total_bases":    int(tb),
        "batting_avg":    _safe_div(h, in_play),
        "slugging_pct":   _safe_div(tb, in_play),
        "xba":   grp["estimated_ba_using_speedangle"].mean()  if "estimated_ba_using_speedangle"  in grp else None,
        "xwoba": grp["estimated_woba_using_speedangle"].mean() if "estimated_woba_using_speedangle" in grp else None,
        "swing_rate":   _safe_div(swings, pitches),
        "chase_rate":   _safe_div(c_swing, chase),
        "contact_rate": _safe_div(contacts, swings),
        "whiff_rate":   _safe_div(whiffs, swings),
        "hard_hit_rate":_safe_div(hard_hit, in_play),
        "barrel_rate":  _safe_div(barrels, in_play),
    }


# ── Main transform ─────────────────────────────────────────────────────────

def transform_splits(conn: sqlite3.Connection, as_of_date: str,
                     window_code: str, start_date: str, end_date: str) -> None:
    log.info("Loading pitches: %s -> %s (window=%s)", start_date, end_date, window_code)

    df = pd.read_sql_query(
        """
        SELECT * FROM stg_statcast_pitches
        WHERE game_date >= ? AND game_date <= ?
          AND pitcher_id IS NOT NULL AND batter_id IS NOT NULL
        """,
        conn,
        params=(start_date, end_date),
    )

    if df.empty:
        log.warning("No pitches found for window %s %s→%s.", window_code, start_date, end_date)
        return

    log.info("  %d pitch events loaded.", len(df))

    # Assign zone codes
    df["zone_code"] = df.apply(
        lambda r: _assign_zone(r.get("plate_x"), r.get("plate_z")), axis=1
    )

    # Infer season from game dates
    df["season"] = pd.to_datetime(df["game_date"]).dt.year

    df = _enrich(df)

    season = int(df["season"].mode()[0])

    _build_batter_overall(conn, df, as_of_date, season, window_code)
    _build_batter_hand_splits(conn, df, as_of_date, season, window_code)
    _build_batter_pitch_type_splits(conn, df, as_of_date, season, window_code)
    _build_batter_zone_splits(conn, df, as_of_date, season, window_code)
    _build_pitcher_overall(conn, df, as_of_date, season, window_code)
    _build_pitcher_hand_splits(conn, df, as_of_date, season, window_code)
    _build_pitcher_pitch_mix(conn, df, as_of_date, season, window_code)
    _build_pitcher_zone_profile(conn, df, as_of_date, season, window_code)
    conn.commit()


def _build_batter_overall(conn, df, as_of_date, season, window_code):
    for player_id, grp in df.groupby("batter_id"):
        a = _batter_agg(grp)
        conn.execute(
            """
            INSERT OR REPLACE INTO fact_batter_overall
                (as_of_date,player_id,season,window_code,
                 plate_appearances,at_bats,hits,home_runs,
                 batting_avg,slugging_pct,xba,xwoba,
                 swing_rate,zone_swing_rate,chase_rate,contact_rate,
                 zone_contact_rate,whiff_rate,hard_hit_rate,barrel_rate,
                 avg_exit_velocity,games_played,ab_per_game)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (as_of_date, int(player_id), season, window_code,
             a["plate_appearances"], a["at_bats"], a["hits"], a["home_runs"],
             a["batting_avg"], a["slugging_pct"], _nan(a["xba"]), _nan(a["xwoba"]),
             a["swing_rate"], a["zone_swing_rate"], a["chase_rate"], a["contact_rate"],
             a["zone_contact_rate"], a["whiff_rate"], a["hard_hit_rate"], a["barrel_rate"],
             _nan(a["avg_exit_velocity"]), a["games_played"], a["ab_per_game"]),
        )


def _build_batter_hand_splits(conn, df, as_of_date, season, window_code):
    for (pid, hand), grp in df.groupby(["batter_id", "p_throws"]):
        if not hand:
            continue
        a = _batter_agg(grp)
        conn.execute(
            """
            INSERT OR REPLACE INTO fact_batter_hand_splits
                (as_of_date,player_id,season,split_hand,window_code,
                 plate_appearances,at_bats,hits,
                 batting_avg,slugging_pct,xba,xwoba,
                 contact_rate,whiff_rate,hard_hit_rate,barrel_rate)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (as_of_date, int(pid), season, hand, window_code,
             a["plate_appearances"], a["at_bats"], a["hits"],
             a["batting_avg"], a["slugging_pct"], _nan(a["xba"]), _nan(a["xwoba"]),
             a["contact_rate"], a["whiff_rate"], a["hard_hit_rate"], a["barrel_rate"]),
        )


def _build_batter_pitch_type_splits(conn, df, as_of_date, season, window_code):
    for (pid, hand, pt), grp in df.groupby(["batter_id", "p_throws", "pitch_type_code"]):
        if not hand or not pt:
            continue
        a = _pitch_type_agg(grp)
        conn.execute(
            """
            INSERT OR REPLACE INTO fact_batter_pitch_type_splits
                (as_of_date,player_id,season,split_hand,pitch_type_code,window_code,
                 pitches_seen,swings,contacts,whiffs,called_strikes,balls,
                 in_play_events,at_bats,hits,total_bases,home_runs,
                 batting_avg,slugging_pct,xba,xwoba,
                 swing_rate,contact_rate,whiff_rate,csw_rate,chase_rate,
                 hard_hit_rate,barrel_rate,avg_exit_velocity)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (as_of_date, int(pid), season, hand, pt, window_code,
             a["pitches_seen"], a["swings"], a["contacts"], a["whiffs"],
             a["called_strikes"], a["balls"], a["in_play_events"],
             a["at_bats"], a["hits"], a["total_bases"], a["home_runs"],
             a["batting_avg"], a["slugging_pct"], _nan(a["xba"]), _nan(a["xwoba"]),
             a["swing_rate"], a["contact_rate"], a["whiff_rate"],
             a["csw_rate"], a["chase_rate"],
             a["hard_hit_rate"], a["barrel_rate"], _nan(a["avg_exit_velocity"])),
        )


def _build_batter_zone_splits(conn, df, as_of_date, season, window_code):
    df_z = df[df["zone_code"].notna()]
    for (pid, hand, zc), grp in df_z.groupby(["batter_id", "p_throws", "zone_code"]):
        if not hand or not zc:
            continue
        a = _zone_agg(grp)
        conn.execute(
            """
            INSERT OR REPLACE INTO fact_batter_zone_splits
                (as_of_date,player_id,season,split_hand,zone_code,window_code,
                 pitches_seen,swings,contacts,whiffs,called_strikes,balls,
                 in_play_events,hits,total_bases,
                 batting_avg,slugging_pct,xba,xwoba,
                 swing_rate,chase_rate,contact_rate,whiff_rate,
                 hard_hit_rate,barrel_rate)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (as_of_date, int(pid), season, hand, zc, window_code,
             a["pitches_seen"], a["swings"], a["contacts"], a["whiffs"],
             a["called_strikes"], a["balls"], a["in_play_events"],
             a["hits"], a["total_bases"],
             a["batting_avg"], a["slugging_pct"], _nan(a["xba"]), _nan(a["xwoba"]),
             a["swing_rate"], a["chase_rate"], a["contact_rate"], a["whiff_rate"],
             a["hard_hit_rate"], a["barrel_rate"]),
        )


def _build_pitcher_overall(conn, df, as_of_date, season, window_code):
    for pitcher_id, grp in df.groupby("pitcher_id"):
        pitches  = len(grp)
        swings   = grp["is_swing"].sum()
        whiffs   = grp["is_whiff"].sum()
        contacts = grp["is_contact"].sum()
        c_str    = grp["is_called_str"].sum()
        in_play  = grp["is_in_play"].sum()
        hard_hit = grp["is_hard_hit"].sum()
        barrels  = grp["is_barrel"].sum()
        in_zone  = grp["is_in_zone"].sum()
        chase    = grp["is_chase"].sum()
        c_swing  = grp["is_chase_swing"].sum()
        hits_a   = grp["is_hit"].sum()
        hr_a     = grp["is_home_run"].sum()
        ab       = grp["is_ab_end"].sum()
        tb       = grp["total_bases"].sum()

        conn.execute(
            """
            INSERT OR REPLACE INTO fact_pitcher_overall
                (as_of_date,pitcher_id,season,window_code,
                 hits_allowed,home_runs_allowed,
                 xba_allowed,xwoba_allowed,
                 swing_rate_allowed,zone_rate,contact_rate_allowed,
                 whiff_rate,csw_rate,chase_rate,
                 hard_hit_rate_allowed,barrel_rate_allowed,
                 avg_exit_velocity_allowed,avg_launch_angle_allowed)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (as_of_date, int(pitcher_id), season, window_code,
             int(hits_a), int(hr_a),
             _nan(grp["estimated_ba_using_speedangle"].mean()),
             _nan(grp["estimated_woba_using_speedangle"].mean()),
             _safe_div(swings, pitches), _safe_div(in_zone, pitches),
             _safe_div(contacts, swings), _safe_div(whiffs, swings),
             _safe_div(whiffs + c_str, pitches), _safe_div(c_swing, chase),
             _safe_div(hard_hit, in_play), _safe_div(barrels, in_play),
             _nan(grp["launch_speed"].mean()), _nan(grp["launch_angle"].mean())),
        )


def _build_pitcher_hand_splits(conn, df, as_of_date, season, window_code):
    for (pid, hand), grp in df.groupby(["pitcher_id", "stand"]):
        if not hand:
            continue
        pitches  = len(grp)
        swings   = grp["is_swing"].sum()
        whiffs   = grp["is_whiff"].sum()
        contacts = grp["is_contact"].sum()
        c_str    = grp["is_called_str"].sum()
        in_play  = grp["is_in_play"].sum()
        hard_hit = grp["is_hard_hit"].sum()
        barrels  = grp["is_barrel"].sum()
        in_zone  = grp["is_in_zone"].sum()
        chase    = grp["is_chase"].sum()
        c_swing  = grp["is_chase_swing"].sum()
        hits_a   = grp["is_hit"].sum()
        ab       = grp["is_ab_end"].sum()
        tb       = grp["total_bases"].sum()

        conn.execute(
            """
            INSERT OR REPLACE INTO fact_pitcher_hand_splits
                (as_of_date,pitcher_id,season,split_hand,window_code,
                 batters_faced,batting_avg_allowed,xba_allowed,xwoba_allowed,
                 contact_rate_allowed,whiff_rate,csw_rate,chase_rate,zone_rate,
                 hard_hit_rate_allowed,barrel_rate_allowed)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (as_of_date, int(pid), season, hand, window_code,
             int(ab),
             _safe_div(hits_a, ab),
             _nan(grp["estimated_ba_using_speedangle"].mean()),
             _nan(grp["estimated_woba_using_speedangle"].mean()),
             _safe_div(contacts, swings), _safe_div(whiffs, swings),
             _safe_div(whiffs + c_str, pitches), _safe_div(c_swing, chase),
             _safe_div(in_zone, pitches),
             _safe_div(hard_hit, in_play), _safe_div(barrels, in_play)),
        )


def _build_pitcher_pitch_mix(conn, df, as_of_date, season, window_code):
    total_by_pitcher = df.groupby("pitcher_id").size()
    for (pid, hand, pt), grp in df.groupby(["pitcher_id", "stand", "pitch_type_code"]):
        if not hand or not pt:
            continue
        pitches  = len(grp)
        swings   = grp["is_swing"].sum()
        whiffs   = grp["is_whiff"].sum()
        contacts = grp["is_contact"].sum()
        c_str    = grp["is_called_str"].sum()
        in_play  = grp["is_in_play"].sum()
        chase    = grp["is_chase"].sum()
        c_swing  = grp["is_chase_swing"].sum()
        in_zone  = grp["is_in_zone"].sum()
        hard_hit = grp["is_hard_hit"].sum()
        hits_a   = grp["is_hit"].sum()
        ab       = grp["is_ab_end"].sum()
        tb       = grp["total_bases"].sum()
        total    = total_by_pitcher.get(pid, 1)

        conn.execute(
            """
            INSERT OR REPLACE INTO fact_pitcher_pitch_mix
                (as_of_date,pitcher_id,season,split_hand,pitch_type_code,window_code,
                 pitches_thrown,usage_pct,
                 avg_velocity,max_velocity,avg_spin_rate,avg_extension,
                 avg_release_height,avg_release_side,
                 avg_horizontal_break,avg_vertical_break,
                 avg_plate_x,avg_plate_z,
                 swing_rate,whiff_rate,csw_rate,chase_rate,zone_rate,
                 batting_avg_allowed,xwoba_allowed,
                 hard_hit_rate_allowed)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (as_of_date, int(pid), season, hand, pt, window_code,
             int(pitches), _safe_div(pitches, total),
             _nan(grp["release_speed"].mean()), _nan(grp["release_speed"].max()),
             _nan(grp["release_spin_rate"].mean()), _nan(grp["release_extension"].mean()),
             _nan(grp["release_pos_z"].mean()), _nan(grp["release_pos_x"].mean()),
             _nan(grp["pfx_x"].mean()), _nan(grp["pfx_z"].mean()),
             _nan(grp["plate_x"].mean()), _nan(grp["plate_z"].mean()),
             _safe_div(swings, pitches), _safe_div(whiffs, swings),
             _safe_div(whiffs + c_str, pitches), _safe_div(c_swing, chase),
             _safe_div(in_zone, pitches),
             _safe_div(hits_a, ab),
             _nan(grp["estimated_woba_using_speedangle"].mean()),
             _safe_div(hard_hit, in_play)),
        )


def _build_pitcher_zone_profile(conn, df, as_of_date, season, window_code):
    df_z = df[df["zone_code"].notna()]
    for (pid, hand, zc, pt), grp in df_z.groupby(
        ["pitcher_id", "stand", "zone_code", "pitch_type_code"]
    ):
        if not hand or not zc or not pt:
            continue
        pitches  = len(grp)
        swings   = grp["is_swing"].sum()
        whiffs   = grp["is_whiff"].sum()
        contacts = grp["is_contact"].sum()
        c_str    = grp["is_called_str"].sum()
        in_play  = grp["is_in_play"].sum()
        hits_a   = grp["is_hit"].sum()
        hard_hit = grp["is_hard_hit"].sum()

        conn.execute(
            """
            INSERT OR REPLACE INTO fact_pitcher_zone_profile
                (as_of_date,pitcher_id,season,split_hand,zone_code,pitch_type_code,window_code,
                 pitches_thrown,
                 avg_velocity,called_strike_rate,swing_rate,contact_rate,whiff_rate,
                 batting_avg_allowed,xwoba_allowed,hard_hit_rate_allowed)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (as_of_date, int(pid), season, hand, zc, pt, window_code,
             int(pitches),
             _nan(grp["release_speed"].mean()),
             _safe_div(c_str, pitches), _safe_div(swings, pitches),
             _safe_div(contacts, swings), _safe_div(whiffs, swings),
             _safe_div(hits_a, in_play),
             _nan(grp["estimated_woba_using_speedangle"].mean()),
             _safe_div(hard_hit, in_play)),
        )


def _nan(v):
    """Convert NaN / None to None for SQLite."""
    if v is None:
        return None
    try:
        import math
        return None if math.isnan(v) else float(v)
    except (TypeError, ValueError):
        return None


# ── Matchup fact table ─────────────────────────────────────────────────────

def build_matchups(conn: sqlite3.Connection, as_of_date: str, window_code: str = "SEASON") -> None:
    """
    Join fact_game_lineups + batter/pitcher split facts to populate
    fact_matchup_batter_pitcher for a given as_of_date.

    Fallback hierarchy — a projection is always produced, degrading gracefully:
      1. Full data: baseline + pt_score + zone_score (70/20/10 blend in compute_match_scores)
      2. No zone data: baseline + pt_score (80/20)
      3. No pitch mix data: baseline + zone_score (90/10)
      4. No pitcher Statcast data at all: baseline only (100%)
      5. No pitcher hand splits: batter baseline only, park/weather adjusted
      6. No batter data either: regression target (0.22) used, park/weather adjusted

    A game is NEVER dropped because the opposing pitcher lacks data.
    pt_score and zone_score are populated by compute_match_scores.py later;
    build_matchups() only needs to establish the matchup row with baseline values.
    """
    log.info("Building matchups for %s (window=%s)...", as_of_date, window_code)

    lineups = conn.execute(
        """
        SELECT l.game_id, l.team_id, l.player_id AS batter_id,
               l.opponent_pitcher_id AS pitcher_id, l.opponent_pitcher_hand,
               g.away_team_id AS opponent_team_id,
               g.venue_id
        FROM fact_game_lineups l
        JOIN fact_games g ON g.as_of_date=l.as_of_date AND g.game_id=l.game_id
        WHERE l.as_of_date=?
        """,
        (as_of_date,),
    ).fetchall()

    skipped_no_batter  = 0
    skipped_no_pitcher = 0
    written            = 0

    for gid, tid, batter_id, pitcher_id, p_hand, opp_tid, venue_id in lineups:

        # Batter ID is required — can't build a matchup without knowing who's batting
        if not batter_id:
            skipped_no_batter += 1
            continue

        # Pitcher ID missing — no probable pitcher posted yet for this game.
        # Skip for now; the next pipeline refresh will populate it once posted.
        if not pitcher_id:
            skipped_no_pitcher += 1
            continue

        # ── Resolve pitcher hand ───────────────────────────────────────────
        # p_hand from lineup ingestion may be NULL for rookies/call-ups not yet
        # in dim_players. Fall back to a direct dim_players lookup.
        resolved_p_hand = p_hand
        if not resolved_p_hand:
            ph_row = conn.execute(
                "SELECT throws FROM dim_players WHERE player_id=?", (pitcher_id,)
            ).fetchone()
            resolved_p_hand = ph_row[0] if ph_row and ph_row[0] else None

        # ── Resolve batter hand ────────────────────────────────────────────
        batter_hand_row = conn.execute(
            "SELECT bats FROM dim_players WHERE player_id=?", (batter_id,)
        ).fetchone()
        b_hand = batter_hand_row[0] if batter_hand_row else None

        # For switch hitters, resolve effective hand vs this pitcher
        if b_hand == "S" and resolved_p_hand:
            effective_b_hand = "L" if resolved_p_hand == "R" else "R"
        else:
            effective_b_hand = b_hand

        # ── Batter vs pitcher hand split ───────────────────────────────────
        bvh = None
        if resolved_p_hand:
            bvh = conn.execute(
                """
                SELECT batting_avg, woba FROM fact_batter_hand_splits
                WHERE as_of_date=? AND player_id=? AND split_hand=? AND window_code=?
                """,
                (as_of_date, batter_id, resolved_p_hand, window_code),
            ).fetchone()

        # ── Pitcher vs batter hand split ───────────────────────────────────
        pvh = None
        if effective_b_hand:
            pvh = conn.execute(
                """
                SELECT batting_avg_allowed, k_rate FROM fact_pitcher_hand_splits
                WHERE as_of_date=? AND pitcher_id=? AND split_hand=? AND window_code=?
                """,
                (as_of_date, pitcher_id, effective_b_hand, window_code),
            ).fetchone()

        # ── Park factor ────────────────────────────────────────────────────
        park = conn.execute(
            "SELECT park_run_factor FROM dim_venues WHERE venue_id=?", (venue_id,)
        ).fetchone()
        park_adj = park[0] if park and park[0] else 1.0

        # ── Weather adjustment ─────────────────────────────────────────────
        import math as _math

        weather = conn.execute(
            """
            SELECT temperature_f, wind_speed_mph, wind_direction_deg
            FROM   fact_game_weather
            WHERE  as_of_date = ? AND game_id = ?
            """,
            (as_of_date, gid),
        ).fetchone()

        weather_adj = 1.0
        if weather and weather[0] is not None:
            temp_f     = weather[0]
            wind_speed = weather[1] if weather[1] is not None else 0.0
            wind_dir   = weather[2] if weather[2] is not None else None

            temp_delta = max(-0.05, min(0.05, (temp_f - 70.0) * 0.001))
            temp_adj   = 1.0 + temp_delta

            wind_adj = 1.0
            if wind_dir is not None and wind_speed > 0:
                effective_speed = min(wind_speed, 25.0)
                out_component   = _math.cos(_math.radians(wind_dir - 180.0))
                WIND_SCALE      = 0.02 / 15.0
                wind_effect     = out_component * effective_speed * WIND_SCALE
                wind_effect     = max(-0.03, min(0.03, wind_effect))
                wind_adj        = 1.0 + wind_effect

            weather_adj = round(temp_adj * wind_adj, 4)

        # ── Projected batting average (baseline blend) ─────────────────────
        # Compute a simple initial projection here. compute_match_scores.py
        # will later overwrite this with the full 70/20/10 weighted blend
        # including pt_score and zone_score.
        # Using 30/70 batter/pitcher weighting per backtesting results.
        REGRESSION_TARGET = 0.22
        bvh_avg = bvh[0] if bvh and bvh[0] is not None else None
        pvh_avg = pvh[0] if pvh and pvh[0] is not None else None

        b_component = bvh_avg if bvh_avg is not None else REGRESSION_TARGET
        p_component = pvh_avg if pvh_avg is not None else REGRESSION_TARGET

        projected_avg = round(
            ((b_component * 0.30) + (p_component * 0.70)) * park_adj * weather_adj,
            4
        )

        conn.execute(
            """
            INSERT OR REPLACE INTO fact_matchup_batter_pitcher
                (as_of_date,game_id,batter_id,pitcher_id,window_code,
                 team_id,opponent_team_id,
                 batter_vs_hand_batting_avg,batter_vs_hand_woba,
                 pitcher_vs_hand_batting_avg_allowed,pitcher_vs_hand_k_rate,
                 park_adjustment_factor,weather_adjustment_factor,
                 projected_batting_avg)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                as_of_date, gid, batter_id, pitcher_id, window_code,
                tid, opp_tid,
                bvh_avg,
                bvh[1] if bvh and len(bvh) > 1 else None,
                pvh_avg,
                pvh[1] if pvh and len(pvh) > 1 else None,
                park_adj, weather_adj,
                projected_avg,
            ),
        )
        written += 1

    conn.commit()
    log.info("Matchups built: %d rows written, %d skipped (no pitcher posted), "
             "%d skipped (no batter id).",
             written, skipped_no_pitcher, skipped_no_batter)


# ── Entry point ────────────────────────────────────────────────────────────

def _window_dates(window_code: str, as_of_date: str):
    as_of = date.fromisoformat(as_of_date)
    if window_code == "SEASON":
        return date(as_of.year, 1, 1).isoformat(), as_of_date
    days = {"L30D": 30, "L14D": 14, "L7D": 7}.get(window_code)
    if days:
        return (as_of - timedelta(days=days)).isoformat(), as_of_date
    raise ValueError(f"Unknown window_code: {window_code}")


if __name__ == "__main__":
    if not HAS_PANDAS:
        exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path",    default="data/mlb_pregame.db")
    parser.add_argument("--date",       help="as_of_date YYYY-MM-DD (default: today)")
    parser.add_argument("--windows",    default="SEASON,L30D,L14D,L7D",
                        help="Comma-separated window codes to compute")
    parser.add_argument("--matchups",   action="store_true",
                        help="Also build fact_matchup_batter_pitcher")
    args = parser.parse_args()

    as_of = args.date or date.today().isoformat()
    windows = [w.strip() for w in args.windows.split(",")]

    conn = sqlite3.connect(args.db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    for wc in windows:
        start, end = _window_dates(wc, as_of)
        transform_splits(conn, as_of_date=as_of, window_code=wc,
                         start_date=start, end_date=end)

    if args.matchups:
        build_matchups(conn, as_of_date=as_of)

    conn.close()
    log.info("Transform complete.")
