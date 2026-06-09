"""
compute_match_scores.py
-----------------------
Computes pitch_type_match_score and zone_match_score for every row in
fact_matchup_batter_pitcher, then writes those scores back to the table.

Works with both SQLite (local) and Supabase (cloud) via utils/db.py.

Usage:
    python ingest/compute_match_scores.py --date 2026-04-22
    python ingest/compute_match_scores.py --today
"""
import logging
import argparse
from datetime import date

from utils.db import get_connection, DB_BACKEND

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

LEAGUE_AVG_BA          = 0.243
MIN_PITCHES_THRESHOLD  = 150
REGRESSION_TARGET      = 0.22
SLG_REGRESSION_TARGET  = 0.380
LEAGUE_AVG_HR_PER_PA   = 0.0293
MIN_BBE_THRESHOLD      = 100
LEAGUE_AVG_BARREL_RATE = 0.0708

SLOT_AB = {1: 3.888, 2: 3.781, 3: 3.708, 4: 3.652, 5: 3.549,
           6: 3.456, 7: 3.339, 8: 3.113, 9: 3.031}


# ── League average helpers ─────────────────────────────────────────────────

def _league_avg_by_pitch_type(conn, as_of_date, window_code):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT pitch_type_code,
               CAST(SUM(hits) AS REAL) / NULLIF(SUM(at_bats), 0) AS lg_avg
        FROM   fact_batter_pitch_type_splits
        WHERE  as_of_date = :aod AND window_code = :wc
        GROUP  BY pitch_type_code
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    return {r[0]: r[1] for r in cur.fetchall() if r[1] is not None}

def _league_avg_by_zone(conn, as_of_date, window_code):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT zone_code,
               CAST(SUM(hits) AS REAL) / NULLIF(SUM(in_play_events), 0) AS lg_avg
        FROM   fact_batter_zone_splits
        WHERE  as_of_date = :aod AND window_code = :wc
        GROUP  BY zone_code
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    return {r[0]: r[1] for r in cur.fetchall() if r[1] is not None}

def _league_slg_by_pitch_type(conn, as_of_date, window_code):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT pitch_type_code,
               CAST(SUM(total_bases) AS REAL) / NULLIF(SUM(at_bats), 0) AS lg_slg
        FROM   fact_batter_pitch_type_splits
        WHERE  as_of_date = :aod AND window_code = :wc
        GROUP  BY pitch_type_code
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    return {r[0]: r[1] for r in cur.fetchall() if r[1] is not None}

def _league_slg_by_zone(conn, as_of_date, window_code):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT zone_code,
               CAST(SUM(total_bases) AS REAL) / NULLIF(SUM(in_play_events), 0) AS lg_slg
        FROM   fact_batter_zone_splits
        WHERE  as_of_date = :aod AND window_code = :wc
        GROUP  BY zone_code
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    return {r[0]: r[1] for r in cur.fetchall() if r[1] is not None}


# ── Regression helper ──────────────────────────────────────────────────────

def _regress(observed, pitches_seen, league_avg, threshold, regression_weight):
    if observed is None:
        return league_avg
    if pitches_seen >= threshold:
        return observed
    sample_weight = regression_weight * (pitches_seen / threshold)
    return (observed * sample_weight) + (league_avg * (1 - sample_weight))


# ── Pitch type match score ─────────────────────────────────────────────────

def _compute_pitch_type_match_score(conn, as_of_date, batter_id, pitcher_id,
                                     pitcher_hand, batter_hand, window_code,
                                     league_avgs, regression_weight):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT pitch_type_code, usage_pct, pitches_thrown
        FROM   fact_pitcher_pitch_mix
        WHERE  as_of_date  = :aod AND pitcher_id = :pid
          AND  split_hand  = :hand AND window_code = :wc
          AND  usage_pct IS NOT NULL AND pitches_thrown > 0
        """,
        {"aod": as_of_date, "pid": pitcher_id, "hand": batter_hand, "wc": window_code},
    )
    pitch_mix = cur.fetchall()
    if not pitch_mix:
        return None
    total_usage = sum(r[1] for r in pitch_mix)
    if total_usage <= 0:
        return None
    weighted_avg = 0.0
    coverage     = 0.0
    for pitch_type_code, raw_usage, pitches_thrown in pitch_mix:
        usage = raw_usage / total_usage
        cur.execute(
            """
            SELECT batting_avg, pitches_seen
            FROM   fact_batter_pitch_type_splits
            WHERE  as_of_date = :aod AND player_id = :bid
              AND  split_hand = :hand AND pitch_type_code = :ptc
              AND  window_code = :wc
            """,
            {"aod": as_of_date, "bid": batter_id, "hand": pitcher_hand,
             "ptc": pitch_type_code, "wc": window_code},
        )
        batter_row   = cur.fetchone()
        observed_avg = batter_row[0] if batter_row and batter_row[0] is not None else None
        pitches_seen = batter_row[1] if batter_row and batter_row[1] is not None else 0
        league_avg_pt = league_avgs.get(pitch_type_code, REGRESSION_TARGET)
        reg_target    = REGRESSION_TARGET if pitches_seen < MIN_PITCHES_THRESHOLD else league_avg_pt
        regressed = _regress(observed_avg, pitches_seen, reg_target,
                             MIN_PITCHES_THRESHOLD, regression_weight)
        weighted_avg += usage * regressed
        coverage     += usage
    if coverage < 0.25:
        return None
    return round(weighted_avg, 4)


# ── Zone match score ───────────────────────────────────────────────────────

def _compute_zone_match_score(conn, as_of_date, batter_id, pitcher_id,
                               pitcher_hand, batter_hand, window_code,
                               league_avgs_zone, regression_weight):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT zone_code, SUM(pitches_thrown) AS total_pitches
        FROM   fact_pitcher_zone_profile
        WHERE  as_of_date = :aod AND pitcher_id = :pid
          AND  split_hand = :hand AND window_code = :wc
          AND  pitches_thrown > 0
        GROUP  BY zone_code
        """,
        {"aod": as_of_date, "pid": pitcher_id, "hand": batter_hand, "wc": window_code},
    )
    zone_profile = cur.fetchall()
    if not zone_profile:
        return None
    total_pitches = sum(r[1] for r in zone_profile)
    if total_pitches <= 0:
        return None
    weighted_avg = 0.0
    coverage     = 0.0
    for zone_code, zone_pitches in zone_profile:
        zone_usage = zone_pitches / total_pitches
        cur.execute(
            """
            SELECT batting_avg, pitches_seen
            FROM   fact_batter_zone_splits
            WHERE  as_of_date = :aod AND player_id = :bid
              AND  split_hand = :hand AND zone_code = :zc
              AND  window_code = :wc
            """,
            {"aod": as_of_date, "bid": batter_id, "hand": pitcher_hand,
             "zc": zone_code, "wc": window_code},
        )
        batter_row     = cur.fetchone()
        observed_avg   = batter_row[0] if batter_row and batter_row[0] is not None else None
        pitches_seen   = batter_row[1] if batter_row and batter_row[1] is not None else 0
        league_avg_zone = league_avgs_zone.get(zone_code, REGRESSION_TARGET)
        reg_target      = REGRESSION_TARGET if pitches_seen < MIN_PITCHES_THRESHOLD else league_avg_zone
        regressed = _regress(observed_avg, pitches_seen, reg_target,
                             MIN_PITCHES_THRESHOLD, regression_weight)
        weighted_avg += zone_usage * regressed
        coverage     += zone_usage
    if coverage < 0.25:
        return None
    return round(weighted_avg, 4)


# ── SLG match scores ───────────────────────────────────────────────────────

def _compute_pitch_type_slg_score(conn, as_of_date, batter_id, pitcher_id,
                                   pitcher_hand, batter_hand, window_code,
                                   league_slgs, regression_weight):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT pitch_type_code, usage_pct, pitches_thrown
        FROM   fact_pitcher_pitch_mix
        WHERE  as_of_date = :aod AND pitcher_id = :pid
          AND  split_hand = :hand AND window_code = :wc
          AND  usage_pct IS NOT NULL AND pitches_thrown > 0
        """,
        {"aod": as_of_date, "pid": pitcher_id, "hand": batter_hand, "wc": window_code},
    )
    pitch_mix = cur.fetchall()
    if not pitch_mix:
        return None
    total_usage = sum(r[1] for r in pitch_mix)
    if total_usage <= 0:
        return None
    weighted_slg = 0.0
    coverage     = 0.0
    for pitch_type_code, raw_usage, _ in pitch_mix:
        usage = raw_usage / total_usage
        cur.execute(
            """
            SELECT slugging_pct, pitches_seen
            FROM   fact_batter_pitch_type_splits
            WHERE  as_of_date = :aod AND player_id = :bid
              AND  split_hand = :hand AND pitch_type_code = :ptc
              AND  window_code = :wc
            """,
            {"aod": as_of_date, "bid": batter_id, "hand": pitcher_hand,
             "ptc": pitch_type_code, "wc": window_code},
        )
        batter_row   = cur.fetchone()
        observed_slg = batter_row[0] if batter_row and batter_row[0] is not None else None
        pitches_seen = batter_row[1] if batter_row and batter_row[1] is not None else 0
        league_slg   = league_slgs.get(pitch_type_code, SLG_REGRESSION_TARGET)
        regressed = _regress(observed_slg, pitches_seen, league_slg,
                             MIN_PITCHES_THRESHOLD, regression_weight)
        weighted_slg += usage * regressed
        coverage     += usage
    if coverage < 0.25:
        return None
    return round(weighted_slg, 4)


def _compute_zone_slg_score(conn, as_of_date, batter_id, pitcher_id,
                             pitcher_hand, batter_hand, window_code,
                             league_slgs_zone, regression_weight):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT zone_code, SUM(pitches_thrown) AS total_pitches
        FROM   fact_pitcher_zone_profile
        WHERE  as_of_date = :aod AND pitcher_id = :pid
          AND  split_hand = :hand AND window_code = :wc
          AND  pitches_thrown > 0
        GROUP  BY zone_code
        """,
        {"aod": as_of_date, "pid": pitcher_id, "hand": batter_hand, "wc": window_code},
    )
    zone_profile = cur.fetchall()
    if not zone_profile:
        return None
    total_pitches = sum(r[1] for r in zone_profile)
    if total_pitches <= 0:
        return None
    weighted_slg = 0.0
    coverage     = 0.0
    for zone_code, zone_pitches in zone_profile:
        zone_usage = zone_pitches / total_pitches
        cur.execute(
            """
            SELECT slugging_pct, pitches_seen
            FROM   fact_batter_zone_splits
            WHERE  as_of_date = :aod AND player_id = :bid
              AND  split_hand = :hand AND zone_code = :zc
              AND  window_code = :wc
            """,
            {"aod": as_of_date, "bid": batter_id, "hand": pitcher_hand,
             "zc": zone_code, "wc": window_code},
        )
        batter_row      = cur.fetchone()
        observed_slg    = batter_row[0] if batter_row and batter_row[0] is not None else None
        pitches_seen    = batter_row[1] if batter_row and batter_row[1] is not None else 0
        league_slg_zone = league_slgs_zone.get(zone_code, SLG_REGRESSION_TARGET)
        regressed = _regress(observed_slg, pitches_seen, league_slg_zone,
                             MIN_PITCHES_THRESHOLD, regression_weight)
        weighted_slg += zone_usage * regressed
        coverage     += zone_usage
    if coverage < 0.25:
        return None
    return round(weighted_slg, 4)


# ── HR probability ─────────────────────────────────────────────────────────

def _compute_hr_probability(conn, as_of_date, batter_id, pitcher_id,
                             effective_batter_hand, pitcher_throws, window_code,
                             park_hr_factor, weather_adj, regression_weight,
                             ab_per_game):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT hr_per_pa,
               CASE :pthrows WHEN 'R' THEN hard_hit_rate_vs_rhp
                             WHEN 'L' THEN hard_hit_rate_vs_lhp END,
               CASE :pthrows WHEN 'R' THEN barrels_per_pa_vs_rhp
                             WHEN 'L' THEN barrels_per_pa_vs_lhp END,
               barrels_per_pa, batted_ball_events
        FROM   fact_batter_power_profile
        WHERE  as_of_date <= :aod AND player_id = :bid AND window_code = :wc
        ORDER  BY as_of_date DESC LIMIT 1
        """,
        {"pthrows": pitcher_throws, "aod": as_of_date,
         "bid": batter_id, "wc": window_code},
    )
    power_row = cur.fetchone()
    if not power_row:
        return None, None, None
    overall_hr_per_pa = power_row[0]
    bpp_vs_hand       = power_row[2]
    overall_bpp       = power_row[3]
    bbe_count         = power_row[4] or 0
    batter_bpp = bpp_vs_hand if bpp_vs_hand is not None else overall_bpp
    if overall_hr_per_pa is None:
        return None, batter_bpp, None
    batter_hr_rate = _regress(overall_hr_per_pa, bbe_count,
                              LEAGUE_AVG_HR_PER_PA, MIN_BBE_THRESHOLD,
                              regression_weight)
    cur.execute(
        """
        SELECT hr_per_bf_allowed, barrel_rate_allowed, batted_ball_events
        FROM   fact_pitcher_hr_vulnerability
        WHERE  as_of_date <= :aod AND pitcher_id = :pid
          AND  split_hand = :hand AND window_code = :wc
        ORDER  BY as_of_date DESC LIMIT 1
        """,
        {"aod": as_of_date, "pid": pitcher_id,
         "hand": effective_batter_hand, "wc": window_code},
    )
    vuln_row            = cur.fetchone()
    pitcher_hr_per_bf   = vuln_row[0] if vuln_row and vuln_row[0] is not None else None
    pitcher_barrel_rate = vuln_row[1] if vuln_row and vuln_row[1] is not None else None
    pitcher_bbe         = vuln_row[2] if vuln_row and vuln_row[2] is not None else 0
    pitcher_hr_rate = None
    if pitcher_hr_per_bf is not None:
        pitcher_hr_rate = _regress(pitcher_hr_per_bf, pitcher_bbe,
                                   LEAGUE_AVG_HR_PER_PA, MIN_BBE_THRESHOLD,
                                   regression_weight)
    barrel_context = None
    if batter_bpp is not None and pitcher_barrel_rate is not None:
        batter_barrel_rel  = batter_bpp          / max(LEAGUE_AVG_BARREL_RATE, 0.001)
        pitcher_barrel_rel = pitcher_barrel_rate / max(LEAGUE_AVG_BARREL_RATE, 0.001)
        barrel_context = LEAGUE_AVG_HR_PER_PA * batter_barrel_rel * pitcher_barrel_rel
    if pitcher_hr_rate is not None and barrel_context is not None:
        blended = (batter_hr_rate * 0.70) + (pitcher_hr_rate * 0.20) + (barrel_context * 0.10)
    elif pitcher_hr_rate is not None:
        blended = (batter_hr_rate * 0.80) + (pitcher_hr_rate * 0.20)
    elif barrel_context is not None:
        blended = (batter_hr_rate * 0.90) + (barrel_context * 0.10)
    else:
        blended = batter_hr_rate
    projected_hr_prob = round(blended * ab_per_game * park_hr_factor * weather_adj, 4)
    return projected_hr_prob, batter_bpp, pitcher_barrel_rate


# ── Main scoring function ──────────────────────────────────────────────────

def compute_match_scores(conn, as_of_date, window_code="SEASON"):
    log.info("Computing match scores for %s (window=%s)...", as_of_date, window_code)
    league_avgs_pt   = _league_avg_by_pitch_type(conn, as_of_date, window_code)
    league_avgs_zone = _league_avg_by_zone(conn, as_of_date, window_code)
    league_slgs_pt   = _league_slg_by_pitch_type(conn, as_of_date, window_code)
    league_slgs_zone = _league_slg_by_zone(conn, as_of_date, window_code)

    cur = conn.cursor()
    cur.execute(
        """
        SELECT CAST(SUM(hits) AS REAL) / NULLIF(SUM(at_bats), 0)
        FROM fact_batter_overall WHERE window_code = :wc AND at_bats >= 50
        """,
        {"wc": window_code},
    )
    dyn_ba_row = cur.fetchone()
    cur.execute(
        """
        SELECT CAST(SUM(total_bases) AS REAL) / NULLIF(SUM(at_bats), 0)
        FROM fact_batter_pitch_type_splits WHERE window_code = :wc AND at_bats >= 10
        """,
        {"wc": window_code},
    )
    dyn_slg_row = cur.fetchone()
    dynamic_league_ba  = dyn_ba_row[0]  if dyn_ba_row  and dyn_ba_row[0]  else LEAGUE_AVG_BA
    dynamic_league_slg = dyn_slg_row[0] if dyn_slg_row and dyn_slg_row[0] else 0.402

    log.info("  League avg by pitch type: %d types.", len(league_avgs_pt))
    log.info("  League avg by zone: %d zones.", len(league_avgs_zone))

    cur.execute(
        "SELECT regression_weight FROM dim_split_windows WHERE window_code = :wc",
        {"wc": window_code},
    )
    rw_row = cur.fetchone()
    regression_weight = rw_row[0] if rw_row else 1.0

    cur.execute(
        """
        SELECT m.game_id, m.batter_id, m.pitcher_id,
               m.batter_vs_hand_batting_avg,
               m.pitcher_vs_hand_batting_avg_allowed,
               m.park_adjustment_factor,
               m.weather_adjustment_factor,
               p_pitcher.throws, p_batter.bats, g.venue_id
        FROM   fact_matchup_batter_pitcher m
        JOIN   dim_players p_pitcher ON p_pitcher.player_id = m.pitcher_id
        JOIN   dim_players p_batter  ON p_batter.player_id  = m.batter_id
        LEFT JOIN fact_games g
               ON g.as_of_date = m.as_of_date AND g.game_id = m.game_id
        WHERE  m.as_of_date = :aod AND m.window_code = :wc
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    matchups = cur.fetchall()
    log.info("  Processing %d matchup rows...", len(matchups))
    if not matchups:
        log.warning("  No matchup rows found for %s window=%s.", as_of_date, window_code)
        return

    updated = skipped = 0
    for (game_id, batter_id, pitcher_id, batter_avg, pitcher_avg_allowed,
         park_adj, weather_adj, pitcher_throws, batter_bats, venue_id) in matchups:

        if not pitcher_throws or not batter_bats:
            skipped += 1
            continue
        effective_batter_hand = (
            ('L' if pitcher_throws == 'R' else 'R') if batter_bats == 'S'
            else batter_bats
        )
        if pitcher_throws not in ('R', 'L'):
            skipped += 1
            continue

        park_hr_col = ("park_hr_factor_lhb" if effective_batter_hand == "L"
                       else "park_hr_factor_rhb")
        park_hr_row = None
        if venue_id:
            cur.execute(
                f"SELECT {park_hr_col}, park_run_factor FROM dim_venues WHERE venue_id = :vid",
                {"vid": venue_id},
            )
            park_hr_row = cur.fetchone()
        if park_hr_row and park_hr_row[0] is not None:
            park_hr_factor = round(park_hr_row[0] / 100.0, 4)
        elif park_hr_row and park_hr_row[1] is not None:
            park_hr_factor = park_hr_row[1]
        else:
            park_hr_factor = 1.0

        b_avg    = batter_avg           if batter_avg           is not None else REGRESSION_TARGET
        p_avg    = pitcher_avg_allowed  if pitcher_avg_allowed  is not None else REGRESSION_TARGET
        baseline_avg = round((b_avg * 0.30) + (p_avg * 0.70), 5)

        pt_score   = _compute_pitch_type_match_score(
            conn, as_of_date, batter_id, pitcher_id, pitcher_throws,
            effective_batter_hand, window_code, league_avgs_pt, regression_weight)
        zone_score = _compute_zone_match_score(
            conn, as_of_date, batter_id, pitcher_id, pitcher_throws,
            effective_batter_hand, window_code, league_avgs_zone, regression_weight)
        pt_slg_score   = _compute_pitch_type_slg_score(
            conn, as_of_date, batter_id, pitcher_id, pitcher_throws,
            effective_batter_hand, window_code, league_slgs_pt, regression_weight)
        zone_slg_score = _compute_zone_slg_score(
            conn, as_of_date, batter_id, pitcher_id, pitcher_throws,
            effective_batter_hand, window_code, league_slgs_zone, regression_weight)

        cur.execute(
            """
            SELECT l.lineup_slot FROM fact_game_lineups l
            WHERE  l.as_of_date = :aod AND l.game_id = :gid AND l.player_id = :bid
            LIMIT  1
            """,
            {"aod": as_of_date, "gid": game_id, "bid": batter_id},
        )
        slot_row = cur.fetchone()
        if slot_row and slot_row[0] in SLOT_AB:
            ab_per_game = SLOT_AB[slot_row[0]]
        else:
            cur.execute(
                """
                SELECT ab_per_game FROM fact_batter_overall
                WHERE  as_of_date = :aod AND player_id = :bid AND window_code = :wc
                """,
                {"aod": as_of_date, "bid": batter_id, "wc": window_code},
            )
            ab_game_row = cur.fetchone()
            ab_per_game = ab_game_row[0] if ab_game_row and ab_game_row[0] else 3.502

        park  = park_adj    or 1.0
        weather = weather_adj or 1.0
        if pt_score and zone_score:
            blended = (baseline_avg * 0.70) + (pt_score * 0.20) + (zone_score * 0.10)
        elif pt_score:
            blended = (baseline_avg * 0.80) + (pt_score * 0.20)
        elif zone_score:
            blended = (baseline_avg * 0.90) + (zone_score * 0.10)
        else:
            blended = baseline_avg
        projected = round(blended * park * weather, 4)

        cur.execute(
            """
            SELECT slugging_pct FROM fact_batter_hand_splits
            WHERE  as_of_date = :aod AND player_id = :bid
              AND  split_hand = :hand AND window_code = :wc
            """,
            {"aod": as_of_date, "bid": batter_id,
             "hand": pitcher_throws, "wc": window_code},
        )
        batter_slg_row = cur.fetchone()
        cur.execute(
            """
            SELECT slugging_pct_allowed FROM fact_pitcher_hand_splits
            WHERE  as_of_date = :aod AND pitcher_id = :pid
              AND  split_hand = :hand AND window_code = :wc
            """,
            {"aod": as_of_date, "pid": pitcher_id,
             "hand": effective_batter_hand, "wc": window_code},
        )
        pitcher_slg_row = cur.fetchone()
        b_slg = (batter_slg_row[0]  if batter_slg_row  and batter_slg_row[0]  is not None
                 else SLG_REGRESSION_TARGET)
        p_slg = (pitcher_slg_row[0] if pitcher_slg_row and pitcher_slg_row[0] is not None
                 else SLG_REGRESSION_TARGET)
        slg_baseline = round((b_slg * 0.30) + (p_slg * 0.70), 5)
        if pt_slg_score and zone_slg_score:
            proj_slg = (slg_baseline * 0.70) + (pt_slg_score * 0.20) + (zone_slg_score * 0.10)
        elif pt_slg_score:
            proj_slg = (slg_baseline * 0.80) + (pt_slg_score * 0.20)
        elif zone_slg_score:
            proj_slg = (slg_baseline * 0.90) + (zone_slg_score * 0.10)
        else:
            proj_slg = slg_baseline
        proj_slg = round(proj_slg * park * weather, 4)
        proj_tb  = round(proj_slg * ab_per_game, 4)

        proj_hr_prob, batter_barrel_rate, pitcher_barrel_rate = _compute_hr_probability(
            conn, as_of_date, batter_id, pitcher_id, effective_batter_hand,
            pitcher_throws, window_code, park_hr_factor, weather_adj,
            regression_weight, ab_per_game,
        )

        conn.execute(
            """
            UPDATE fact_matchup_batter_pitcher
            SET    pitch_type_match_score      = :pt,
                   zone_match_score            = :zs,
                   projected_batting_avg       = :pba,
                   pt_slg_score                = :pts,
                   zone_slg_score              = :zss,
                   projected_slugging          = :pslg,
                   projected_total_bases       = :ptb,
                   proj_at_bats_per_game       = :abg,
                   projected_hr_probability    = :phr,
                   batter_barrel_rate          = :bbr,
                   pitcher_barrel_rate_allowed = :pbra
            WHERE  as_of_date  = :aod AND game_id    = :gid
              AND  batter_id   = :bid AND pitcher_id = :pid
              AND  window_code = :wc
            """,
            {
                "pt": pt_score, "zs": zone_score, "pba": projected,
                "pts": pt_slg_score, "zss": zone_slg_score,
                "pslg": proj_slg, "ptb": proj_tb, "abg": ab_per_game,
                "phr": proj_hr_prob, "bbr": batter_barrel_rate,
                "pbra": pitcher_barrel_rate,
                "aod": as_of_date, "gid": game_id,
                "bid": batter_id, "pid": pitcher_id, "wc": window_code,
            },
        )
        updated += 1

    conn.commit()
    log.info("  Match scores written: %d updated, %d skipped.", updated, skipped)


# ── Entrypoint ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",    help="as_of_date YYYY-MM-DD")
    parser.add_argument("--today",   action="store_true")
    parser.add_argument("--windows", default="SEASON,L30D,L14D,L7D")
    args = parser.parse_args()

    as_of = args.date if args.date else (
        date.today().isoformat() if args.today else None
    )
    if not as_of:
        parser.error("Provide --date YYYY-MM-DD or --today")

    with get_connection() as conn:
        if DB_BACKEND == "sqlite":
            conn.execute("PRAGMA foreign_keys=OFF;")
            conn.execute("PRAGMA journal_mode=WAL;")
        for wc in [w.strip() for w in args.windows.split(",")]:
            compute_match_scores(conn, as_of_date=as_of, window_code=wc)

    log.info("Match score computation complete.")