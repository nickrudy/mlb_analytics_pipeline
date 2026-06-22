"""
transform_splits.py
--------------------
Reads raw pitch-level data from stg_statcast_pitches and computes all
split fact tables and fact_matchup_batter_pitcher.

Works with both SQLite (local) and Supabase (cloud) via utils/db.py.

Key optimization: uses bulk_upsert() which calls psycopg2.extras.execute_values()
for Supabase — sends all rows in a single SQL statement instead of individual
round-trips. Reduces pitcher_zone_profile insert from 20 min to ~10 sec.
"""
import math
import logging
import argparse
from datetime import date, timedelta, datetime, timezone

# -- CT date helper (avoids UTC-date bug after ~7 PM CT) --
from zoneinfo import ZoneInfo as _ZI
def _today_ct(): return __import__("datetime").datetime.now(_ZI("America/Chicago")).date().isoformat()
from utils.db import get_connection, get_engine, DB_BACKEND
from utils.db_bulk import bulk_upsert

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
    if plate_x is None or plate_z is None:
        return None
    x, z = float(plate_x), float(plate_z)
    in_x = -0.83 <= x <= 0.83
    in_z =  1.50 <= z <= 3.67
    if in_x and in_z:
        col = 1 if x < -0.28 else (2 if x <= 0.28 else 3)
        row = 1 if z >= 3.00 else (2 if z >= 2.33 else 3)
        return f"Z{(row-1)*3 + col}"
    if z > 3.67:            return "CHASE_UP"
    if z < 1.50:            return "CHASE_DOWN"
    if x < -0.83 and in_z: return "CHASE_IN"
    if x >  0.83 and in_z: return "CHASE_OUT"
    return None


# ── Hit classification helpers ─────────────────────────────────────────────

HIT_EVENTS    = {"single", "double", "triple", "home_run"}
EXTRA_BASE    = {"double": 2, "triple": 3, "home_run": 4}
SWING_DESCS   = {"swinging_strike","swinging_strike_blocked","foul","foul_tip",
                  "hit_into_play","foul_bunt","missed_bunt",
                  "in_play_out","in_play_score","in_play_no_out"}
WHIFF_DESCS   = {"swinging_strike","swinging_strike_blocked"}
CONTACT_DESCS = {"foul","foul_tip","hit_into_play",
                  "in_play_out","in_play_score","in_play_no_out"}
IN_PLAY_DESCS = {"hit_into_play","in_play_out","in_play_score","in_play_no_out"}
CALLED_STRIKE = {"called_strike"}
AT_BAT_END    = {"single","double","triple","home_run","strikeout",
                  "strikeout_double_play","field_out","force_out",
                  "grounded_into_double_play","double_play","triple_play",
                  "field_error","fielders_choice","fielders_choice_out",
                  "hit_by_pitch","sac_fly","sac_bunt","sac_fly_double_play"}
HARD_HIT_MPH  = 95.0


def _enrich(df):
    desc = df["description"].fillna("").str.lower()
    evts = df["events"].fillna("").str.lower()
    df["is_swing"]       = desc.isin(SWING_DESCS)
    df["is_whiff"]       = desc.isin(WHIFF_DESCS)
    df["is_contact"]     = desc.isin(CONTACT_DESCS)
    df["is_in_play"]     = desc.isin(IN_PLAY_DESCS)
    df["is_called_str"]  = desc.isin(CALLED_STRIKE)
    df["is_ball"]        = desc == "ball"
    df["is_hit"]         = evts.isin(HIT_EVENTS)
    df["is_ab_end"]      = evts.isin(AT_BAT_END)
    df["is_hard_hit"]    = df["launch_speed"] >= HARD_HIT_MPH
    df["is_barrel"]      = (df["launch_speed"] >= 98.0) & (df["launch_angle"].between(26, 30))
    df["total_bases"]    = evts.map(lambda e: 1 if e == "single" else EXTRA_BASE.get(e, 0))
    df["is_home_run"]    = evts == "home_run"
    df["is_in_zone"]     = df["zone_code"].str.startswith("Z", na=False)
    df["is_chase"]       = df["zone_code"].str.startswith("CHASE", na=False)
    df["is_chase_swing"] = df["is_swing"] & df["is_chase"]
    df["is_zone_swing"]  = df["is_swing"] & df["is_in_zone"]
    df["is_zone_contact"]= df["is_contact"] & df["is_in_zone"]
    return df


# ── Aggregate helpers ──────────────────────────────────────────────────────

def _safe_div(num, denom):
    return round(float(num) / float(denom), 6) if denom and denom > 0 else None

def _nan(v):
    if v is None:
        return None
    try:
        return None if math.isnan(float(v)) else float(v)
    except (TypeError, ValueError):
        return None

def _batter_agg(grp):
    pa = ab = int(grp["is_ab_end"].sum())
    h       = int(grp["is_hit"].sum())
    hr      = int(grp["is_home_run"].sum())
    swings  = int(grp["is_swing"].sum())
    contacts= int(grp["is_contact"].sum())
    whiffs  = int(grp["is_whiff"].sum())
    in_zone = int(grp["is_in_zone"].sum())
    z_swing = int(grp["is_zone_swing"].sum())
    z_cont  = int(grp["is_zone_contact"].sum())
    chase   = int(grp["is_chase"].sum())
    c_swing = int(grp["is_chase_swing"].sum())
    in_play = int(grp["is_in_play"].sum())
    hard_hit= int(grp["is_hard_hit"].sum())
    barrels = int(grp["is_barrel"].sum())
    tb      = int(grp["total_bases"].sum())
    pitches = len(grp)
    games   = grp["game_pk"].nunique() if "game_pk" in grp.columns else max(1, int(pa / 4))
    return {
        "plate_appearances": pa, "at_bats": ab, "hits": h, "home_runs": hr,
        "batting_avg": _safe_div(h, ab), "slugging_pct": _safe_div(tb, ab),
        "games_played": int(games), "ab_per_game": _safe_div(ab, games),
        "xba":   _nan(grp["estimated_ba_using_speedangle"].mean())  if "estimated_ba_using_speedangle"  in grp else None,
        "xwoba": _nan(grp["estimated_woba_using_speedangle"].mean()) if "estimated_woba_using_speedangle" in grp else None,
        "swing_rate":        _safe_div(swings, pitches),
        "zone_swing_rate":   _safe_div(z_swing, in_zone),
        "chase_rate":        _safe_div(c_swing, chase),
        "contact_rate":      _safe_div(contacts, swings),
        "zone_contact_rate": _safe_div(z_cont, z_swing),
        "whiff_rate":        _safe_div(whiffs, swings),
        "hard_hit_rate":     _safe_div(hard_hit, in_play),
        "barrel_rate":       _safe_div(barrels, in_play),
        "avg_exit_velocity": _nan(grp["launch_speed"].mean()) if "launch_speed" in grp else None,
    }

def _pitch_type_agg(grp):
    pitches  = len(grp)
    swings   = int(grp["is_swing"].sum())
    contacts = int(grp["is_contact"].sum())
    whiffs   = int(grp["is_whiff"].sum())
    c_str    = int(grp["is_called_str"].sum())
    balls    = int(grp["is_ball"].sum())
    in_play  = int(grp["is_in_play"].sum())
    ab       = int(grp["is_ab_end"].sum())
    h        = int(grp["is_hit"].sum())
    tb       = int(grp["total_bases"].sum())
    hr       = int(grp["is_home_run"].sum())
    hard_hit = int(grp["is_hard_hit"].sum())
    barrels  = int(grp["is_barrel"].sum())
    chase    = int(grp["is_chase"].sum())
    c_swing  = int(grp["is_chase_swing"].sum())
    return {
        "pitches_seen": pitches, "swings": swings, "contacts": contacts,
        "whiffs": whiffs, "called_strikes": c_str, "balls": balls,
        "in_play_events": in_play, "at_bats": ab, "hits": h,
        "total_bases": tb, "home_runs": hr,
        "batting_avg": _safe_div(h, ab), "slugging_pct": _safe_div(tb, ab),
        "xba":   _nan(grp["estimated_ba_using_speedangle"].mean())  if "estimated_ba_using_speedangle"  in grp else None,
        "xwoba": _nan(grp["estimated_woba_using_speedangle"].mean()) if "estimated_woba_using_speedangle" in grp else None,
        "swing_rate": _safe_div(swings, pitches), "contact_rate": _safe_div(contacts, swings),
        "whiff_rate": _safe_div(whiffs, swings), "csw_rate": _safe_div(whiffs + c_str, pitches),
        "chase_rate": _safe_div(c_swing, chase), "hard_hit_rate": _safe_div(hard_hit, in_play),
        "barrel_rate": _safe_div(barrels, in_play),
        "avg_exit_velocity": _nan(grp["launch_speed"].mean()) if "launch_speed" in grp else None,
    }

def _zone_agg(grp):
    pitches  = len(grp)
    swings   = int(grp["is_swing"].sum())
    contacts = int(grp["is_contact"].sum())
    whiffs   = int(grp["is_whiff"].sum())
    c_str    = int(grp["is_called_str"].sum())
    balls    = int(grp["is_ball"].sum())
    in_play  = int(grp["is_in_play"].sum())
    h        = int(grp["is_hit"].sum())
    tb       = int(grp["total_bases"].sum())
    hard_hit = int(grp["is_hard_hit"].sum())
    barrels  = int(grp["is_barrel"].sum())
    chase    = int(grp["is_chase"].sum())
    c_swing  = int(grp["is_chase_swing"].sum())
    return {
        "pitches_seen": pitches, "swings": swings, "contacts": contacts,
        "whiffs": whiffs, "called_strikes": c_str, "balls": balls,
        "in_play_events": in_play, "hits": h, "total_bases": tb,
        "batting_avg": _safe_div(h, in_play), "slugging_pct": _safe_div(tb, in_play),
        "xba":   _nan(grp["estimated_ba_using_speedangle"].mean())  if "estimated_ba_using_speedangle"  in grp else None,
        "xwoba": _nan(grp["estimated_woba_using_speedangle"].mean()) if "estimated_woba_using_speedangle" in grp else None,
        "swing_rate": _safe_div(swings, pitches), "chase_rate": _safe_div(c_swing, chase),
        "contact_rate": _safe_div(contacts, swings), "whiff_rate": _safe_div(whiffs, swings),
        "hard_hit_rate": _safe_div(hard_hit, in_play), "barrel_rate": _safe_div(barrels, in_play),
    }


# ── Fact table builders — all use bulk_upsert() ───────────────────────────

def _build_batter_overall(conn, df, as_of_date, season, window_code):
    rows = []
    for player_id, grp in df.groupby("batter_id"):
        a = _batter_agg(grp)
        rows.append({
            "as_of_date": as_of_date, "player_id": int(player_id),
            "season": season, "window_code": window_code,
            "plate_appearances": a["plate_appearances"], "at_bats": a["at_bats"],
            "hits": a["hits"], "home_runs": a["home_runs"],
            "batting_avg": a["batting_avg"], "slugging_pct": a["slugging_pct"],
            "xba": a["xba"], "xwoba": a["xwoba"],
            "swing_rate": a["swing_rate"], "zone_swing_rate": a["zone_swing_rate"],
            "chase_rate": a["chase_rate"], "contact_rate": a["contact_rate"],
            "zone_contact_rate": a["zone_contact_rate"], "whiff_rate": a["whiff_rate"],
            "hard_hit_rate": a["hard_hit_rate"], "barrel_rate": a["barrel_rate"],
            "avg_exit_velocity": a["avg_exit_velocity"],
            "games_played": a["games_played"], "ab_per_game": a["ab_per_game"],
        })
    n = bulk_upsert(conn, "fact_batter_overall", rows,
        conflict_cols="as_of_date,player_id,season,window_code",
        update_cols=["plate_appearances","at_bats","hits","batting_avg","slugging_pct",
                     "swing_rate","whiff_rate","hard_hit_rate","barrel_rate",
                     "ab_per_game","games_played","avg_exit_velocity"])
    log.info("    batter_overall: %d rows", n)


def _build_batter_hand_splits(conn, df, as_of_date, season, window_code):
    rows = []
    for (pid, hand), grp in df.groupby(["batter_id", "p_throws"]):
        if not hand:
            continue
        a = _batter_agg(grp)
        rows.append({
            "as_of_date": as_of_date, "player_id": int(pid),
            "season": season, "split_hand": hand, "window_code": window_code,
            "plate_appearances": a["plate_appearances"], "at_bats": a["at_bats"],
            "hits": a["hits"], "batting_avg": a["batting_avg"],
            "slugging_pct": a["slugging_pct"], "xba": a["xba"], "xwoba": a["xwoba"],
            "contact_rate": a["contact_rate"], "whiff_rate": a["whiff_rate"],
            "hard_hit_rate": a["hard_hit_rate"], "barrel_rate": a["barrel_rate"],
        })
    n = bulk_upsert(conn, "fact_batter_hand_splits", rows,
        conflict_cols="as_of_date,player_id,season,split_hand,window_code",
        update_cols=["plate_appearances","batting_avg","slugging_pct","whiff_rate",
                     "hard_hit_rate","barrel_rate"])
    log.info("    batter_hand_splits: %d rows", n)


def _build_batter_pitch_type_splits(conn, df, as_of_date, season, window_code):
    rows = []
    for (pid, hand, pt), grp in df.groupby(["batter_id", "p_throws", "pitch_type_code"]):
        if not hand or not pt:
            continue
        a = _pitch_type_agg(grp)
        rows.append({
            "as_of_date": as_of_date, "player_id": int(pid),
            "season": season, "split_hand": hand, "pitch_type_code": pt,
            "window_code": window_code,
            "pitches_seen": a["pitches_seen"], "swings": a["swings"],
            "contacts": a["contacts"], "whiffs": a["whiffs"],
            "called_strikes": a["called_strikes"], "balls": a["balls"],
            "in_play_events": a["in_play_events"], "at_bats": a["at_bats"],
            "hits": a["hits"], "total_bases": a["total_bases"],
            "home_runs": a["home_runs"], "batting_avg": a["batting_avg"],
            "slugging_pct": a["slugging_pct"], "xba": a["xba"], "xwoba": a["xwoba"],
            "swing_rate": a["swing_rate"], "contact_rate": a["contact_rate"],
            "whiff_rate": a["whiff_rate"], "csw_rate": a["csw_rate"],
            "chase_rate": a["chase_rate"], "hard_hit_rate": a["hard_hit_rate"],
            "barrel_rate": a["barrel_rate"], "avg_exit_velocity": a["avg_exit_velocity"],
        })
    n = bulk_upsert(conn, "fact_batter_pitch_type_splits", rows,
        conflict_cols="as_of_date,player_id,season,split_hand,pitch_type_code,window_code",
        update_cols=["pitches_seen","batting_avg","slugging_pct","whiff_rate","barrel_rate"])
    log.info("    batter_pitch_type_splits: %d rows", n)


def _build_batter_zone_splits(conn, df, as_of_date, season, window_code):
    rows = []
    df_z = df[df["zone_code"].notna()]
    for (pid, hand, zc), grp in df_z.groupby(["batter_id", "p_throws", "zone_code"]):
        if not hand or not zc:
            continue
        a = _zone_agg(grp)
        rows.append({
            "as_of_date": as_of_date, "player_id": int(pid),
            "season": season, "split_hand": hand, "zone_code": zc,
            "window_code": window_code,
            "pitches_seen": a["pitches_seen"], "swings": a["swings"],
            "contacts": a["contacts"], "whiffs": a["whiffs"],
            "called_strikes": a["called_strikes"], "balls": a["balls"],
            "in_play_events": a["in_play_events"], "hits": a["hits"],
            "total_bases": a["total_bases"], "batting_avg": a["batting_avg"],
            "slugging_pct": a["slugging_pct"], "xba": a["xba"], "xwoba": a["xwoba"],
            "swing_rate": a["swing_rate"], "chase_rate": a["chase_rate"],
            "contact_rate": a["contact_rate"], "whiff_rate": a["whiff_rate"],
            "hard_hit_rate": a["hard_hit_rate"], "barrel_rate": a["barrel_rate"],
        })
    n = bulk_upsert(conn, "fact_batter_zone_splits", rows,
        conflict_cols="as_of_date,player_id,season,split_hand,zone_code,window_code",
        update_cols=["pitches_seen","batting_avg","slugging_pct"])
    log.info("    batter_zone_splits: %d rows", n)


def _build_pitcher_overall(conn, df, as_of_date, season, window_code):
    rows = []
    for pitcher_id, grp in df.groupby("pitcher_id"):
        pitches  = len(grp)
        swings   = int(grp["is_swing"].sum())
        whiffs   = int(grp["is_whiff"].sum())
        contacts = int(grp["is_contact"].sum())
        c_str    = int(grp["is_called_str"].sum())
        in_play  = int(grp["is_in_play"].sum())
        hard_hit = int(grp["is_hard_hit"].sum())
        barrels  = int(grp["is_barrel"].sum())
        in_zone  = int(grp["is_in_zone"].sum())
        chase    = int(grp["is_chase"].sum())
        c_swing  = int(grp["is_chase_swing"].sum())
        hits_a   = int(grp["is_hit"].sum())
        hr_a     = int(grp["is_home_run"].sum())
        rows.append({
            "as_of_date": as_of_date, "pitcher_id": int(pitcher_id),
            "season": season, "window_code": window_code,
            "hits_allowed": hits_a, "home_runs_allowed": hr_a,
            "xba_allowed": _nan(grp["estimated_ba_using_speedangle"].mean()),
            "xwoba_allowed": _nan(grp["estimated_woba_using_speedangle"].mean()),
            "swing_rate_allowed": _safe_div(swings, pitches),
            "zone_rate": _safe_div(in_zone, pitches),
            "contact_rate_allowed": _safe_div(contacts, swings),
            "whiff_rate": _safe_div(whiffs, swings),
            "csw_rate": _safe_div(whiffs + c_str, pitches),
            "chase_rate": _safe_div(c_swing, chase),
            "hard_hit_rate_allowed": _safe_div(hard_hit, in_play),
            "barrel_rate_allowed": _safe_div(barrels, in_play),
            "avg_exit_velocity_allowed": _nan(grp["launch_speed"].mean()),
            "avg_launch_angle_allowed": _nan(grp["launch_angle"].mean()),
        })
    n = bulk_upsert(conn, "fact_pitcher_overall", rows,
        conflict_cols="as_of_date,pitcher_id,season,window_code",
        update_cols=["hits_allowed","whiff_rate","hard_hit_rate_allowed","barrel_rate_allowed"])
    log.info("    pitcher_overall: %d rows", n)


def _build_pitcher_hand_splits(conn, df, as_of_date, season, window_code):
    rows = []
    for (pid, hand), grp in df.groupby(["pitcher_id", "stand"]):
        if not hand:
            continue
        pitches  = len(grp)
        swings   = int(grp["is_swing"].sum())
        whiffs   = int(grp["is_whiff"].sum())
        contacts = int(grp["is_contact"].sum())
        c_str    = int(grp["is_called_str"].sum())
        in_play  = int(grp["is_in_play"].sum())
        hard_hit = int(grp["is_hard_hit"].sum())
        barrels  = int(grp["is_barrel"].sum())
        in_zone  = int(grp["is_in_zone"].sum())
        chase    = int(grp["is_chase"].sum())
        c_swing  = int(grp["is_chase_swing"].sum())
        hits_a   = int(grp["is_hit"].sum())
        ab       = int(grp["is_ab_end"].sum())
        rows.append({
            "as_of_date": as_of_date, "pitcher_id": int(pid),
            "season": season, "split_hand": hand, "window_code": window_code,
            "batters_faced": ab,
            "batting_avg_allowed": _safe_div(hits_a, ab),
            "xba_allowed": _nan(grp["estimated_ba_using_speedangle"].mean()),
            "xwoba_allowed": _nan(grp["estimated_woba_using_speedangle"].mean()),
            "contact_rate_allowed": _safe_div(contacts, swings),
            "whiff_rate": _safe_div(whiffs, swings),
            "csw_rate": _safe_div(whiffs + c_str, pitches),
            "chase_rate": _safe_div(c_swing, chase),
            "zone_rate": _safe_div(in_zone, pitches),
            "hard_hit_rate_allowed": _safe_div(hard_hit, in_play),
            "barrel_rate_allowed": _safe_div(barrels, in_play),
        })
    n = bulk_upsert(conn, "fact_pitcher_hand_splits", rows,
        conflict_cols="as_of_date,pitcher_id,season,split_hand,window_code",
        update_cols=["batters_faced","whiff_rate","hard_hit_rate_allowed"])
    log.info("    pitcher_hand_splits: %d rows", n)


def _build_pitcher_pitch_mix(conn, df, as_of_date, season, window_code):
    total_by_pitcher = df.groupby("pitcher_id").size()
    rows = []
    for (pid, hand, pt), grp in df.groupby(["pitcher_id", "stand", "pitch_type_code"]):
        if not hand or not pt:
            continue
        pitches  = len(grp)
        swings   = int(grp["is_swing"].sum())
        whiffs   = int(grp["is_whiff"].sum())
        c_str    = int(grp["is_called_str"].sum())
        in_play  = int(grp["is_in_play"].sum())
        in_zone  = int(grp["is_in_zone"].sum())
        chase    = int(grp["is_chase"].sum())
        c_swing  = int(grp["is_chase_swing"].sum())
        hard_hit = int(grp["is_hard_hit"].sum())
        hits_a   = int(grp["is_hit"].sum())
        ab       = int(grp["is_ab_end"].sum())
        total    = total_by_pitcher.get(pid, 1)
        rows.append({
            "as_of_date": as_of_date, "pitcher_id": int(pid),
            "season": season, "split_hand": hand, "pitch_type_code": pt,
            "window_code": window_code,
            "pitches_thrown": pitches, "usage_pct": _safe_div(pitches, total),
            "avg_velocity": _nan(grp["release_speed"].mean()),
            "max_velocity": _nan(grp["release_speed"].max()),
            "avg_spin_rate": _nan(grp["release_spin_rate"].mean()),
            "avg_extension": _nan(grp["release_extension"].mean()),
            "avg_release_height": _nan(grp["release_pos_z"].mean()),
            "avg_release_side": _nan(grp["release_pos_x"].mean()),
            "avg_horizontal_break": _nan(grp["pfx_x"].mean()),
            "avg_vertical_break": _nan(grp["pfx_z"].mean()),
            "avg_plate_x": _nan(grp["plate_x"].mean()),
            "avg_plate_z": _nan(grp["plate_z"].mean()),
            "swing_rate": _safe_div(swings, pitches),
            "whiff_rate": _safe_div(whiffs, swings),
            "csw_rate": _safe_div(whiffs + c_str, pitches),
            "chase_rate": _safe_div(c_swing, chase),
            "zone_rate": _safe_div(in_zone, pitches),
            "batting_avg_allowed": _safe_div(hits_a, ab),
            "xwoba_allowed": _nan(grp["estimated_woba_using_speedangle"].mean()),
            "hard_hit_rate_allowed": _safe_div(hard_hit, in_play),
        })
    n = bulk_upsert(conn, "fact_pitcher_pitch_mix", rows,
        conflict_cols="as_of_date,pitcher_id,season,split_hand,pitch_type_code,window_code",
        update_cols=["pitches_thrown","usage_pct","whiff_rate","avg_velocity"])
    log.info("    pitcher_pitch_mix: %d rows", n)


def _build_pitcher_zone_profile(conn, df, as_of_date, season, window_code):
    rows = []
    df_z = df[df["zone_code"].notna()]
    for (pid, hand, zc, pt), grp in df_z.groupby(
            ["pitcher_id", "stand", "zone_code", "pitch_type_code"]):
        if not hand or not zc or not pt:
            continue
        pitches  = len(grp)
        swings   = int(grp["is_swing"].sum())
        whiffs   = int(grp["is_whiff"].sum())
        contacts = int(grp["is_contact"].sum())
        c_str    = int(grp["is_called_str"].sum())
        in_play  = int(grp["is_in_play"].sum())
        hits_a   = int(grp["is_hit"].sum())
        hard_hit = int(grp["is_hard_hit"].sum())
        rows.append({
            "as_of_date": as_of_date, "pitcher_id": int(pid),
            "season": season, "split_hand": hand, "zone_code": zc,
            "pitch_type_code": pt, "window_code": window_code,
            "pitches_thrown": pitches,
            "avg_velocity": _nan(grp["release_speed"].mean()),
            "called_strike_rate": _safe_div(c_str, pitches),
            "swing_rate": _safe_div(swings, pitches),
            "contact_rate": _safe_div(contacts, swings),
            "whiff_rate": _safe_div(whiffs, swings),
            "batting_avg_allowed": _safe_div(hits_a, in_play),
            "xwoba_allowed": _nan(grp["estimated_woba_using_speedangle"].mean()),
            "hard_hit_rate_allowed": _safe_div(hard_hit, in_play),
        })
    n = bulk_upsert(conn, "fact_pitcher_zone_profile", rows,
        conflict_cols="as_of_date,pitcher_id,season,split_hand,zone_code,pitch_type_code,window_code",
        update_cols=["pitches_thrown","whiff_rate"])
    log.info("    pitcher_zone_profile: %d rows", n)


# ── Power profile builders ─────────────────────────────────────────────────

def _build_batter_power_profile(conn, df, as_of_date, season, window_code):
    rows = []
    in_play_df = df[df["is_in_play"]]
    for bid, grp in df.groupby("batter_id"):
        ip_grp   = in_play_df[in_play_df["batter_id"] == bid]
        bbe      = len(ip_grp)
        pa       = int(grp["is_ab_end"].sum())
        hr       = int(grp["is_home_run"].sum())
        barrels  = int(ip_grp["is_barrel"].sum())
        hard_hit = int(ip_grp["is_hard_hit"].sum())

        # vs RHP / LHP splits
        grp_rhp  = grp[grp["p_throws"] == "R"]   if "p_throws" in grp.columns else grp.iloc[0:0]
        grp_lhp  = grp[grp["p_throws"] == "L"]   if "p_throws" in grp.columns else grp.iloc[0:0]
        ip_rhp   = ip_grp[ip_grp["p_throws"] == "R"] if "p_throws" in ip_grp.columns else ip_grp.iloc[0:0]
        ip_lhp   = ip_grp[ip_grp["p_throws"] == "L"] if "p_throws" in ip_grp.columns else ip_grp.iloc[0:0]

        pa_rhp   = int(grp_rhp["is_ab_end"].sum())
        pa_lhp   = int(grp_lhp["is_ab_end"].sum())
        bbe_rhp  = len(ip_rhp)
        bbe_lhp  = len(ip_lhp)

        rows.append({
            "as_of_date":           as_of_date,
            "player_id":            int(bid),
            "season":               season,
            "window_code":          window_code,
            "hr_per_pa":            _safe_div(hr, pa),
            "barrels_per_pa":       _safe_div(barrels, bbe),
            "barrels_per_pa_vs_rhp":_safe_div(int(ip_rhp["is_barrel"].sum()), bbe_rhp),
            "barrels_per_pa_vs_lhp":_safe_div(int(ip_lhp["is_barrel"].sum()), bbe_lhp),
            "hard_hit_rate_vs_rhp": _safe_div(int(ip_rhp["is_hard_hit"].sum()), bbe_rhp),
            "hard_hit_rate_vs_lhp": _safe_div(int(ip_lhp["is_hard_hit"].sum()), bbe_lhp),
            "batted_ball_events":   bbe,
        })
    n = bulk_upsert(conn, "fact_batter_power_profile", rows,
        conflict_cols="as_of_date,player_id,season,window_code",
        update_cols=["hr_per_pa","barrels_per_pa","barrels_per_pa_vs_rhp",
                     "barrels_per_pa_vs_lhp","hard_hit_rate_vs_rhp",
                     "hard_hit_rate_vs_lhp","batted_ball_events"])
    log.info("    batter_power_profile: %d rows", n)


def _build_pitcher_hr_vulnerability(conn, df, as_of_date, season, window_code):
    rows = []
    in_play_df = df[df["is_in_play"]]
    for (pid, hand), grp in df.groupby(["pitcher_id", "stand"]):
        if not hand:
            continue
        ip_grp  = in_play_df[(in_play_df["pitcher_id"] == pid) &
                              (in_play_df["stand"] == hand)]
        bbe     = len(ip_grp)
        bf      = int(grp["is_ab_end"].sum())
        hr_a    = int(grp["is_home_run"].sum())
        barrels = int(ip_grp["is_barrel"].sum())

        rows.append({
            "as_of_date":          as_of_date,
            "pitcher_id":          int(pid),
            "season":              season,
            "split_hand":          hand,
            "window_code":         window_code,
            "hr_per_bf_allowed":   _safe_div(hr_a, bf),
            "barrel_rate_allowed": _safe_div(barrels, bbe),
            "batted_ball_events":  bbe,
        })
    n = bulk_upsert(conn, "fact_pitcher_hr_vulnerability", rows,
        conflict_cols="as_of_date,pitcher_id,season,split_hand,window_code",
        update_cols=["hr_per_bf_allowed","barrel_rate_allowed","batted_ball_events"])
    log.info("    pitcher_hr_vulnerability: %d rows", n)


# ── Main transform ─────────────────────────────────────────────────────────

def transform_splits(conn, as_of_date, window_code, start_date, end_date):
    log.info("Loading pitches: %s -> %s (window=%s)", start_date, end_date, window_code)
    engine = get_engine()
    if DB_BACKEND == "supabase":
        sql = """
            SELECT * FROM stg_statcast_pitches
            WHERE game_date >= %(start)s AND game_date <= %(end)s
              AND pitcher_id IS NOT NULL AND batter_id IS NOT NULL
        """
    else:
        sql = """
            SELECT * FROM stg_statcast_pitches
            WHERE game_date >= :start AND game_date <= :end
              AND pitcher_id IS NOT NULL AND batter_id IS NOT NULL
        """
    df = pd.read_sql_query(sql, engine, params={"start": start_date, "end": end_date})

    if df.empty:
        log.warning("No pitches for %s %s→%s.", window_code, start_date, end_date)
        return
    log.info("  %d pitch events loaded.", len(df))
    df["zone_code"] = df.apply(
        lambda r: _assign_zone(r.get("plate_x"), r.get("plate_z")), axis=1
    )
    df["season"] = pd.to_datetime(df["game_date"]).dt.year
    df = _enrich(df)
    season = int(df["season"].mode()[0])

    _build_batter_overall(conn, df, as_of_date, season, window_code)
    _build_batter_hand_splits(conn, df, as_of_date, season, window_code)
    _build_batter_pitch_type_splits(conn, df, as_of_date, season, window_code)
    _build_batter_zone_splits(conn, df, as_of_date, season, window_code)
    _build_batter_power_profile(conn, df, as_of_date, season, window_code)
    _build_pitcher_overall(conn, df, as_of_date, season, window_code)
    _build_pitcher_hand_splits(conn, df, as_of_date, season, window_code)
    _build_pitcher_pitch_mix(conn, df, as_of_date, season, window_code)
    _build_pitcher_zone_profile(conn, df, as_of_date, season, window_code)
    _build_pitcher_hr_vulnerability(conn, df, as_of_date, season, window_code)
    conn.commit()
    log.info("  Transform complete for window=%s.", window_code)


# ── Matchup builder ────────────────────────────────────────────────────────

def build_matchups(conn, as_of_date, window_code="SEASON"):
    log.info("Building matchups for %s (window=%s)...", as_of_date, window_code)
    REGRESSION_TARGET = 0.22
    cur = conn.cursor()
    cur.execute(
        """
        SELECT l.game_id, l.team_id, l.player_id AS batter_id,
               l.opponent_pitcher_id, l.opponent_pitcher_hand,
               g.away_team_id, g.venue_id
        FROM fact_game_lineups l
        JOIN fact_games g ON g.as_of_date=l.as_of_date AND g.game_id=l.game_id
        WHERE l.as_of_date=:aod
        """,
        {"aod": as_of_date},
    )
    lineups = cur.fetchall()
    rows = []
    skipped_pitcher = skipped_batter = 0

    for gid, tid, batter_id, pitcher_id, p_hand, opp_tid, venue_id in lineups:
        if not batter_id:
            skipped_batter += 1
            continue
        if not pitcher_id:
            skipped_pitcher += 1
            continue
        resolved_p_hand = p_hand
        if not resolved_p_hand:
            cur.execute("SELECT throws FROM dim_players WHERE player_id=:pid", {"pid": pitcher_id})
            ph = cur.fetchone()
            resolved_p_hand = ph[0] if ph else None
        cur.execute("SELECT bats FROM dim_players WHERE player_id=:pid", {"pid": batter_id})
        bh = cur.fetchone()
        b_hand = bh[0] if bh else None
        effective_b_hand = (
            ("L" if resolved_p_hand == "R" else "R") if b_hand == "S" and resolved_p_hand
            else b_hand
        )
        bvh = None
        if resolved_p_hand:
            cur.execute(
                "SELECT batting_avg, woba FROM fact_batter_hand_splits "
                "WHERE as_of_date=:aod AND player_id=:bid AND split_hand=:hand AND window_code=:wc",
                {"aod": as_of_date, "bid": batter_id, "hand": resolved_p_hand, "wc": window_code},
            )
            bvh = cur.fetchone()
        pvh = None
        if effective_b_hand:
            cur.execute(
                "SELECT batting_avg_allowed, k_rate FROM fact_pitcher_hand_splits "
                "WHERE as_of_date=:aod AND pitcher_id=:pid AND split_hand=:hand AND window_code=:wc",
                {"aod": as_of_date, "pid": pitcher_id, "hand": effective_b_hand, "wc": window_code},
            )
            pvh = cur.fetchone()
        park = None
        if venue_id:
            cur.execute("SELECT park_run_factor FROM dim_venues WHERE venue_id=:vid", {"vid": venue_id})
            park = cur.fetchone()
        park_adj = park[0] if park and park[0] else 1.0
        cur.execute(
            "SELECT temperature_f, wind_speed_mph, wind_direction_deg "
            "FROM fact_game_weather WHERE as_of_date=:aod AND game_id=:gid",
            {"aod": as_of_date, "gid": gid},
        )
        weather = cur.fetchone()
        weather_adj = 1.0
        if weather and weather[0] is not None:
            temp_f = weather[0]
            wind_speed = weather[1] or 0.0
            wind_dir   = weather[2]
            temp_adj = 1.0 + max(-0.05, min(0.05, (temp_f - 70.0) * 0.001))
            wind_adj = 1.0
            if wind_dir is not None and wind_speed > 0:
                import math as _math
                eff_speed     = min(wind_speed, 25.0)
                out_component = _math.cos(_math.radians(wind_dir - 180.0))
                wind_effect   = max(-0.03, min(0.03, out_component * eff_speed * (0.02/15.0)))
                wind_adj      = 1.0 + wind_effect
            weather_adj = round(temp_adj * wind_adj, 4)
        b_avg = (bvh[0] if bvh and bvh[0] is not None else None)
        p_avg = (pvh[0] if pvh and pvh[0] is not None else None)
        projected_avg = round(
            (((b_avg or REGRESSION_TARGET) * 0.30) + ((p_avg or REGRESSION_TARGET) * 0.70))
            * park_adj * weather_adj, 4
        )
        rows.append({
            "as_of_date": as_of_date, "game_id": gid,
            "batter_id": batter_id, "pitcher_id": pitcher_id,
            "window_code": window_code, "team_id": tid, "opponent_team_id": opp_tid,
            "batter_vs_hand_batting_avg": b_avg,
            "batter_vs_hand_woba": (bvh[1] if bvh and len(bvh) > 1 else None),
            "pitcher_vs_hand_batting_avg_allowed": p_avg,
            "pitcher_vs_hand_k_rate": (pvh[1] if pvh and len(pvh) > 1 else None),
            "park_adjustment_factor": park_adj,
            "weather_adjustment_factor": weather_adj,
            "projected_batting_avg": projected_avg,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        })

    n = bulk_upsert(conn, "fact_matchup_batter_pitcher", rows,
        conflict_cols="as_of_date,game_id,batter_id,pitcher_id,window_code",
        update_cols=["batter_vs_hand_batting_avg","projected_batting_avg",
                     "park_adjustment_factor","weather_adjustment_factor","ingested_at"])
    conn.commit()
    log.info("Matchups built: %d written, %d skipped (no pitcher), %d skipped (no batter).",
             n, skipped_pitcher, skipped_batter)


# ── Window date helper ─────────────────────────────────────────────────────

def _window_dates(window_code, as_of_date):
    as_of = date.fromisoformat(as_of_date)
    if window_code == "SEASON":
        return date(as_of.year, 1, 1).isoformat(), as_of_date
    days = {"L30D": 30, "L14D": 14, "L7D": 7}.get(window_code)
    if days:
        return (as_of - timedelta(days=days)).isoformat(), as_of_date
    raise ValueError(f"Unknown window_code: {window_code}")


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not HAS_PANDAS:
        exit(1)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",     help="as_of_date YYYY-MM-DD (default: today)")
    parser.add_argument("--windows",  default="SEASON,L30D,L14D,L7D")
    parser.add_argument("--matchups", action="store_true")
    args = parser.parse_args()
    as_of   = args.date or _today_ct()
    windows = [w.strip() for w in args.windows.split(",")]
    with get_connection() as conn:
        if DB_BACKEND == "sqlite":
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        for wc in windows:
            start, end = _window_dates(wc, as_of)
            transform_splits(conn, as_of_date=as_of, window_code=wc,
                             start_date=start, end_date=end)
        if args.matchups:
            build_matchups(conn, as_of_date=as_of)
    log.info("Transform complete.")