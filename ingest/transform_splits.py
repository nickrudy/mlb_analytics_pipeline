"""
transform_splits.py
--------------------
Reads raw pitch-level data from stg_statcast_pitches and computes all
split fact tables and fact_matchup_batter_pitcher.

Works with both SQLite (local) and Supabase (cloud) via utils/db.py.

Usage:
    python ingest/transform_splits.py --date 2025-04-15
    python ingest/transform_splits.py --season 2025
"""
import math
import logging
import argparse
from datetime import date, timedelta

from utils.db import get_connection, get_engine, DB_BACKEND

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
    if z > 3.67:               return "CHASE_UP"
    if z < 1.50:               return "CHASE_DOWN"
    if x < -0.83 and in_z:    return "CHASE_IN"
    if x >  0.83 and in_z:    return "CHASE_OUT"
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
    h      = int(grp["is_hit"].sum())
    hr     = int(grp["is_home_run"].sum())
    swings = int(grp["is_swing"].sum())
    contacts= int(grp["is_contact"].sum())
    whiffs = int(grp["is_whiff"].sum())
    in_zone= int(grp["is_in_zone"].sum())
    z_swing= int(grp["is_zone_swing"].sum())
    z_cont = int(grp["is_zone_contact"].sum())
    chase  = int(grp["is_chase"].sum())
    c_swing= int(grp["is_chase_swing"].sum())
    in_play= int(grp["is_in_play"].sum())
    hard_hit= int(grp["is_hard_hit"].sum())
    barrels= int(grp["is_barrel"].sum())
    tb     = int(grp["total_bases"].sum())
    pitches= len(grp)
    games  = grp["game_pk"].nunique() if "game_pk" in grp.columns else max(1, int(pa / 4))
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


# ── SQL upsert helpers ─────────────────────────────────────────────────────

def _conflict(unique_cols: str) -> str:
    """Return the ON CONFLICT clause for Supabase, empty string for SQLite
    (SQLite uses INSERT OR REPLACE instead)."""
    if DB_BACKEND == "supabase":
        return f"ON CONFLICT ({unique_cols}) DO UPDATE SET "
    return ""

def _insert_prefix(table: str) -> str:
    return "INSERT INTO" if DB_BACKEND == "supabase" else "INSERT OR REPLACE INTO"


# ── Fact table builders ────────────────────────────────────────────────────
# Each uses named :param style throughout so both backends work identically.

def _build_batter_overall(conn, df, as_of_date, season, window_code):
    ins = _insert_prefix("fact_batter_overall")
    conflict = (
        "ON CONFLICT (as_of_date,player_id,season,window_code) DO UPDATE SET "
        "plate_appearances=EXCLUDED.plate_appearances, at_bats=EXCLUDED.at_bats, "
        "hits=EXCLUDED.hits, batting_avg=EXCLUDED.batting_avg, "
        "slugging_pct=EXCLUDED.slugging_pct, swing_rate=EXCLUDED.swing_rate, "
        "whiff_rate=EXCLUDED.whiff_rate, hard_hit_rate=EXCLUDED.hard_hit_rate, "
        "barrel_rate=EXCLUDED.barrel_rate, ab_per_game=EXCLUDED.ab_per_game"
        if DB_BACKEND == "supabase" else ""
    )
    for player_id, grp in df.groupby("batter_id"):
        a = _batter_agg(grp)
        conn.execute(
            f"""
            {ins} fact_batter_overall
                (as_of_date,player_id,season,window_code,
                 plate_appearances,at_bats,hits,home_runs,
                 batting_avg,slugging_pct,xba,xwoba,
                 swing_rate,zone_swing_rate,chase_rate,contact_rate,
                 zone_contact_rate,whiff_rate,hard_hit_rate,barrel_rate,
                 avg_exit_velocity,games_played,ab_per_game)
            VALUES
                (:aod,:pid,:season,:wc,
                 :pa,:ab,:h,:hr,
                 :avg,:slg,:xba,:xwoba,
                 :sw,:zsw,:ch,:ct,
                 :zct,:wh,:hh,:br,
                 :ev,:gp,:apg)
            {conflict}
            """,
            {"aod": as_of_date, "pid": int(player_id), "season": season, "wc": window_code,
             "pa": a["plate_appearances"], "ab": a["at_bats"], "h": a["hits"], "hr": a["home_runs"],
             "avg": a["batting_avg"], "slg": a["slugging_pct"], "xba": a["xba"], "xwoba": a["xwoba"],
             "sw": a["swing_rate"], "zsw": a["zone_swing_rate"], "ch": a["chase_rate"],
             "ct": a["contact_rate"], "zct": a["zone_contact_rate"], "wh": a["whiff_rate"],
             "hh": a["hard_hit_rate"], "br": a["barrel_rate"], "ev": a["avg_exit_velocity"],
             "gp": a["games_played"], "apg": a["ab_per_game"]},
        )

def _build_batter_hand_splits(conn, df, as_of_date, season, window_code):
    ins = _insert_prefix("fact_batter_hand_splits")
    conflict = (
        "ON CONFLICT (as_of_date,player_id,season,split_hand,window_code) DO UPDATE SET "
        "plate_appearances=EXCLUDED.plate_appearances, batting_avg=EXCLUDED.batting_avg, "
        "slugging_pct=EXCLUDED.slugging_pct, whiff_rate=EXCLUDED.whiff_rate"
        if DB_BACKEND == "supabase" else ""
    )
    for (pid, hand), grp in df.groupby(["batter_id", "p_throws"]):
        if not hand:
            continue
        a = _batter_agg(grp)
        conn.execute(
            f"""
            {ins} fact_batter_hand_splits
                (as_of_date,player_id,season,split_hand,window_code,
                 plate_appearances,at_bats,hits,
                 batting_avg,slugging_pct,xba,xwoba,
                 contact_rate,whiff_rate,hard_hit_rate,barrel_rate)
            VALUES
                (:aod,:pid,:season,:hand,:wc,
                 :pa,:ab,:h,
                 :avg,:slg,:xba,:xwoba,
                 :ct,:wh,:hh,:br)
            {conflict}
            """,
            {"aod": as_of_date, "pid": int(pid), "season": season, "hand": hand, "wc": window_code,
             "pa": a["plate_appearances"], "ab": a["at_bats"], "h": a["hits"],
             "avg": a["batting_avg"], "slg": a["slugging_pct"], "xba": a["xba"], "xwoba": a["xwoba"],
             "ct": a["contact_rate"], "wh": a["whiff_rate"], "hh": a["hard_hit_rate"], "br": a["barrel_rate"]},
        )

def _build_batter_pitch_type_splits(conn, df, as_of_date, season, window_code):
    ins = _insert_prefix("fact_batter_pitch_type_splits")
    conflict = (
        "ON CONFLICT (as_of_date,player_id,season,split_hand,pitch_type_code,window_code) "
        "DO UPDATE SET pitches_seen=EXCLUDED.pitches_seen, batting_avg=EXCLUDED.batting_avg, "
        "whiff_rate=EXCLUDED.whiff_rate, barrel_rate=EXCLUDED.barrel_rate"
        if DB_BACKEND == "supabase" else ""
    )
    for (pid, hand, pt), grp in df.groupby(["batter_id", "p_throws", "pitch_type_code"]):
        if not hand or not pt:
            continue
        a = _pitch_type_agg(grp)
        conn.execute(
            f"""
            {ins} fact_batter_pitch_type_splits
                (as_of_date,player_id,season,split_hand,pitch_type_code,window_code,
                 pitches_seen,swings,contacts,whiffs,called_strikes,balls,
                 in_play_events,at_bats,hits,total_bases,home_runs,
                 batting_avg,slugging_pct,xba,xwoba,
                 swing_rate,contact_rate,whiff_rate,csw_rate,chase_rate,
                 hard_hit_rate,barrel_rate,avg_exit_velocity)
            VALUES
                (:aod,:pid,:season,:hand,:pt,:wc,
                 :ps,:sw,:ct,:wh,:cs,:bl,
                 :ip,:ab,:h,:tb,:hr,
                 :avg,:slg,:xba,:xwoba,
                 :swr,:ctr,:whr,:csw,:chr,
                 :hh,:br,:ev)
            {conflict}
            """,
            {"aod": as_of_date, "pid": int(pid), "season": season, "hand": hand,
             "pt": pt, "wc": window_code,
             "ps": a["pitches_seen"], "sw": a["swings"], "ct": a["contacts"],
             "wh": a["whiffs"], "cs": a["called_strikes"], "bl": a["balls"],
             "ip": a["in_play_events"], "ab": a["at_bats"], "h": a["hits"],
             "tb": a["total_bases"], "hr": a["home_runs"],
             "avg": a["batting_avg"], "slg": a["slugging_pct"],
             "xba": a["xba"], "xwoba": a["xwoba"],
             "swr": a["swing_rate"], "ctr": a["contact_rate"], "whr": a["whiff_rate"],
             "csw": a["csw_rate"], "chr": a["chase_rate"],
             "hh": a["hard_hit_rate"], "br": a["barrel_rate"], "ev": a["avg_exit_velocity"]},
        )

def _build_batter_zone_splits(conn, df, as_of_date, season, window_code):
    ins = _insert_prefix("fact_batter_zone_splits")
    conflict = (
        "ON CONFLICT (as_of_date,player_id,season,split_hand,zone_code,window_code) "
        "DO UPDATE SET pitches_seen=EXCLUDED.pitches_seen, batting_avg=EXCLUDED.batting_avg"
        if DB_BACKEND == "supabase" else ""
    )
    df_z = df[df["zone_code"].notna()]
    for (pid, hand, zc), grp in df_z.groupby(["batter_id", "p_throws", "zone_code"]):
        if not hand or not zc:
            continue
        a = _zone_agg(grp)
        conn.execute(
            f"""
            {ins} fact_batter_zone_splits
                (as_of_date,player_id,season,split_hand,zone_code,window_code,
                 pitches_seen,swings,contacts,whiffs,called_strikes,balls,
                 in_play_events,hits,total_bases,
                 batting_avg,slugging_pct,xba,xwoba,
                 swing_rate,chase_rate,contact_rate,whiff_rate,
                 hard_hit_rate,barrel_rate)
            VALUES
                (:aod,:pid,:season,:hand,:zc,:wc,
                 :ps,:sw,:ct,:wh,:cs,:bl,
                 :ip,:h,:tb,
                 :avg,:slg,:xba,:xwoba,
                 :swr,:chr,:ctr,:whr,
                 :hh,:br)
            {conflict}
            """,
            {"aod": as_of_date, "pid": int(pid), "season": season, "hand": hand,
             "zc": zc, "wc": window_code,
             "ps": a["pitches_seen"], "sw": a["swings"], "ct": a["contacts"],
             "wh": a["whiffs"], "cs": a["called_strikes"], "bl": a["balls"],
             "ip": a["in_play_events"], "h": a["hits"], "tb": a["total_bases"],
             "avg": a["batting_avg"], "slg": a["slugging_pct"],
             "xba": a["xba"], "xwoba": a["xwoba"],
             "swr": a["swing_rate"], "chr": a["chase_rate"],
             "ctr": a["contact_rate"], "whr": a["whiff_rate"],
             "hh": a["hard_hit_rate"], "br": a["barrel_rate"]},
        )

def _build_pitcher_overall(conn, df, as_of_date, season, window_code):
    ins = _insert_prefix("fact_pitcher_overall")
    conflict = (
        "ON CONFLICT (as_of_date,pitcher_id,season,window_code) DO UPDATE SET "
        "hits_allowed=EXCLUDED.hits_allowed, whiff_rate=EXCLUDED.whiff_rate, "
        "hard_hit_rate_allowed=EXCLUDED.hard_hit_rate_allowed"
        if DB_BACKEND == "supabase" else ""
    )
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
        conn.execute(
            f"""
            {ins} fact_pitcher_overall
                (as_of_date,pitcher_id,season,window_code,
                 hits_allowed,home_runs_allowed,
                 xba_allowed,xwoba_allowed,
                 swing_rate_allowed,zone_rate,contact_rate_allowed,
                 whiff_rate,csw_rate,chase_rate,
                 hard_hit_rate_allowed,barrel_rate_allowed,
                 avg_exit_velocity_allowed,avg_launch_angle_allowed)
            VALUES
                (:aod,:pid,:season,:wc,
                 :ha,:hra,
                 :xba,:xwoba,
                 :sw,:zr,:ct,
                 :wh,:csw,:chr,
                 :hh,:br,
                 :ev,:la)
            {conflict}
            """,
            {"aod": as_of_date, "pid": int(pitcher_id), "season": season, "wc": window_code,
             "ha": hits_a, "hra": hr_a,
             "xba": _nan(grp["estimated_ba_using_speedangle"].mean()),
             "xwoba": _nan(grp["estimated_woba_using_speedangle"].mean()),
             "sw": _safe_div(swings, pitches), "zr": _safe_div(in_zone, pitches),
             "ct": _safe_div(contacts, swings), "wh": _safe_div(whiffs, swings),
             "csw": _safe_div(whiffs + c_str, pitches), "chr": _safe_div(c_swing, chase),
             "hh": _safe_div(hard_hit, in_play), "br": _safe_div(barrels, in_play),
             "ev": _nan(grp["launch_speed"].mean()), "la": _nan(grp["launch_angle"].mean())},
        )

def _build_pitcher_hand_splits(conn, df, as_of_date, season, window_code):
    ins = _insert_prefix("fact_pitcher_hand_splits")
    conflict = (
        "ON CONFLICT (as_of_date,pitcher_id,season,split_hand,window_code) DO UPDATE SET "
        "batters_faced=EXCLUDED.batters_faced, whiff_rate=EXCLUDED.whiff_rate"
        if DB_BACKEND == "supabase" else ""
    )
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
        conn.execute(
            f"""
            {ins} fact_pitcher_hand_splits
                (as_of_date,pitcher_id,season,split_hand,window_code,
                 batters_faced,batting_avg_allowed,xba_allowed,xwoba_allowed,
                 contact_rate_allowed,whiff_rate,csw_rate,chase_rate,zone_rate,
                 hard_hit_rate_allowed,barrel_rate_allowed)
            VALUES
                (:aod,:pid,:season,:hand,:wc,
                 :bf,:avg,:xba,:xwoba,
                 :ct,:wh,:csw,:chr,:zr,
                 :hh,:br)
            {conflict}
            """,
            {"aod": as_of_date, "pid": int(pid), "season": season, "hand": hand, "wc": window_code,
             "bf": ab, "avg": _safe_div(hits_a, ab),
             "xba": _nan(grp["estimated_ba_using_speedangle"].mean()),
             "xwoba": _nan(grp["estimated_woba_using_speedangle"].mean()),
             "ct": _safe_div(contacts, swings), "wh": _safe_div(whiffs, swings),
             "csw": _safe_div(whiffs + c_str, pitches), "chr": _safe_div(c_swing, chase),
             "zr": _safe_div(in_zone, pitches),
             "hh": _safe_div(hard_hit, in_play), "br": _safe_div(barrels, in_play)},
        )

def _build_pitcher_pitch_mix(conn, df, as_of_date, season, window_code):
    ins = _insert_prefix("fact_pitcher_pitch_mix")
    conflict = (
        "ON CONFLICT (as_of_date,pitcher_id,season,split_hand,pitch_type_code,window_code) "
        "DO UPDATE SET pitches_thrown=EXCLUDED.pitches_thrown, usage_pct=EXCLUDED.usage_pct, "
        "whiff_rate=EXCLUDED.whiff_rate"
        if DB_BACKEND == "supabase" else ""
    )
    total_by_pitcher = df.groupby("pitcher_id").size()
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
        conn.execute(
            f"""
            {ins} fact_pitcher_pitch_mix
                (as_of_date,pitcher_id,season,split_hand,pitch_type_code,window_code,
                 pitches_thrown,usage_pct,
                 avg_velocity,max_velocity,avg_spin_rate,avg_extension,
                 avg_release_height,avg_release_side,
                 avg_horizontal_break,avg_vertical_break,
                 avg_plate_x,avg_plate_z,
                 swing_rate,whiff_rate,csw_rate,chase_rate,zone_rate,
                 batting_avg_allowed,xwoba_allowed,hard_hit_rate_allowed)
            VALUES
                (:aod,:pid,:season,:hand,:pt,:wc,
                 :pthr,:usg,
                 :vel,:mvel,:spin,:ext,
                 :rh,:rs,:hbrk,:vbrk,
                 :px,:pz,
                 :sw,:wh,:csw,:chr,:zr,
                 :avg,:xwoba,:hh)
            {conflict}
            """,
            {"aod": as_of_date, "pid": int(pid), "season": season, "hand": hand,
             "pt": pt, "wc": window_code,
             "pthr": pitches, "usg": _safe_div(pitches, total),
             "vel": _nan(grp["release_speed"].mean()), "mvel": _nan(grp["release_speed"].max()),
             "spin": _nan(grp["release_spin_rate"].mean()), "ext": _nan(grp["release_extension"].mean()),
             "rh": _nan(grp["release_pos_z"].mean()), "rs": _nan(grp["release_pos_x"].mean()),
             "hbrk": _nan(grp["pfx_x"].mean()), "vbrk": _nan(grp["pfx_z"].mean()),
             "px": _nan(grp["plate_x"].mean()), "pz": _nan(grp["plate_z"].mean()),
             "sw": _safe_div(swings, pitches), "wh": _safe_div(whiffs, swings),
             "csw": _safe_div(whiffs + c_str, pitches), "chr": _safe_div(c_swing, chase),
             "zr": _safe_div(in_zone, pitches),
             "avg": _safe_div(hits_a, ab),
             "xwoba": _nan(grp["estimated_woba_using_speedangle"].mean()),
             "hh": _safe_div(hard_hit, in_play)},
        )

def _build_pitcher_zone_profile(conn, df, as_of_date, season, window_code):
    ins = _insert_prefix("fact_pitcher_zone_profile")
    conflict = (
        "ON CONFLICT (as_of_date,pitcher_id,season,split_hand,zone_code,pitch_type_code,window_code) "
        "DO UPDATE SET pitches_thrown=EXCLUDED.pitches_thrown, whiff_rate=EXCLUDED.whiff_rate"
        if DB_BACKEND == "supabase" else ""
    )
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
        conn.execute(
            f"""
            {ins} fact_pitcher_zone_profile
                (as_of_date,pitcher_id,season,split_hand,zone_code,pitch_type_code,window_code,
                 pitches_thrown,avg_velocity,called_strike_rate,swing_rate,
                 contact_rate,whiff_rate,batting_avg_allowed,xwoba_allowed,
                 hard_hit_rate_allowed)
            VALUES
                (:aod,:pid,:season,:hand,:zc,:pt,:wc,
                 :pthr,:vel,:csr,:sw,
                 :ct,:wh,:avg,:xwoba,
                 :hh)
            {conflict}
            """,
            {"aod": as_of_date, "pid": int(pid), "season": season, "hand": hand,
             "zc": zc, "pt": pt, "wc": window_code,
             "pthr": pitches, "vel": _nan(grp["release_speed"].mean()),
             "csr": _safe_div(c_str, pitches), "sw": _safe_div(swings, pitches),
             "ct": _safe_div(contacts, swings), "wh": _safe_div(whiffs, swings),
             "avg": _safe_div(hits_a, in_play),
             "xwoba": _nan(grp["estimated_woba_using_speedangle"].mean()),
             "hh": _safe_div(hard_hit, in_play)},
        )


# ── Main transform ─────────────────────────────────────────────────────────

def transform_splits(conn, as_of_date, window_code, start_date, end_date):
    log.info("Loading pitches: %s -> %s (window=%s)", start_date, end_date, window_code)

    # pd.read_sql_query needs a SQLAlchemy engine for Supabase (psycopg2
    # connections don't support the pandas read path directly)
    engine = get_engine()
    df = pd.read_sql_query(
        """
        SELECT * FROM stg_statcast_pitches
        WHERE game_date >= :start AND game_date <= :end
          AND pitcher_id IS NOT NULL AND batter_id IS NOT NULL
        """,
        engine,
        params={"start": start_date, "end": end_date},
    )
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
    _build_pitcher_overall(conn, df, as_of_date, season, window_code)
    _build_pitcher_hand_splits(conn, df, as_of_date, season, window_code)
    _build_pitcher_pitch_mix(conn, df, as_of_date, season, window_code)
    _build_pitcher_zone_profile(conn, df, as_of_date, season, window_code)
    conn.commit()
    log.info("  Transform complete for window=%s.", window_code)


# ── Matchup builder ────────────────────────────────────────────────────────

def build_matchups(conn, as_of_date, window_code="SEASON"):
    log.info("Building matchups for %s (window=%s)...", as_of_date, window_code)
    ins = _insert_prefix("fact_matchup_batter_pitcher")
    conflict = (
        "ON CONFLICT (as_of_date,game_id,batter_id,pitcher_id,window_code) DO UPDATE SET "
        "batter_vs_hand_batting_avg=EXCLUDED.batter_vs_hand_batting_avg, "
        "projected_batting_avg=EXCLUDED.projected_batting_avg"
        if DB_BACKEND == "supabase" else ""
    )
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
    written = skipped_pitcher = skipped_batter = 0

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
                eff_speed    = min(wind_speed, 25.0)
                out_component= math.cos(math.radians(wind_dir - 180.0))
                wind_effect  = max(-0.03, min(0.03, out_component * eff_speed * (0.02/15.0)))
                wind_adj     = 1.0 + wind_effect
            weather_adj = round(temp_adj * wind_adj, 4)

        b_avg = (bvh[0] if bvh and bvh[0] is not None else None)
        p_avg = (pvh[0] if pvh and pvh[0] is not None else None)
        projected_avg = round(
            (((b_avg or REGRESSION_TARGET) * 0.30) + ((p_avg or REGRESSION_TARGET) * 0.70))
            * park_adj * weather_adj, 4
        )

        conn.execute(
            f"""
            {ins} fact_matchup_batter_pitcher
                (as_of_date,game_id,batter_id,pitcher_id,window_code,
                 team_id,opponent_team_id,
                 batter_vs_hand_batting_avg,batter_vs_hand_woba,
                 pitcher_vs_hand_batting_avg_allowed,pitcher_vs_hand_k_rate,
                 park_adjustment_factor,weather_adjustment_factor,
                 projected_batting_avg)
            VALUES
                (:aod,:gid,:bid,:pid,:wc,
                 :tid,:otid,
                 :bavg,:bwoba,
                 :pavg,:pkr,
                 :park,:weather,:proj)
            {conflict}
            """,
            {"aod": as_of_date, "gid": gid, "bid": batter_id, "pid": pitcher_id, "wc": window_code,
             "tid": tid, "otid": opp_tid,
             "bavg": b_avg, "bwoba": (bvh[1] if bvh and len(bvh) > 1 else None),
             "pavg": p_avg, "pkr": (pvh[1] if pvh and len(pvh) > 1 else None),
             "park": park_adj, "weather": weather_adj, "proj": projected_avg},
        )
        written += 1

    conn.commit()
    log.info("Matchups built: %d written, %d skipped (no pitcher), %d skipped (no batter).",
             written, skipped_pitcher, skipped_batter)


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
    as_of   = args.date or date.today().isoformat()
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