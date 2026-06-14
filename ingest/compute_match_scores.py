"""
compute_match_scores.py
-----------------------
Computes pitch_type_match_score and zone_match_score for every row in
fact_matchup_batter_pitcher, then writes those scores back to the table.

Works with both SQLite (local) and Supabase (cloud) via utils/db.py.

Performance: all split tables loaded into memory once per window,
scored via dict lookups, updates batched via bulk_upsert.
Reduces 270-row run from ~15 min to ~30 sec.

Usage:
    python ingest/compute_match_scores.py --date 2026-04-22
    python ingest/compute_match_scores.py --today
"""
import logging
import argparse
from datetime import date
from collections import defaultdict

from utils.db import get_connection, DB_BACKEND
from utils.db_bulk import bulk_upsert

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


# ── Regression helper ──────────────────────────────────────────────────────

def _regress(observed, pitches_seen, league_avg, threshold, regression_weight):
    if observed is None:
        return league_avg
    if pitches_seen >= threshold:
        return observed
    sample_weight = regression_weight * (pitches_seen / threshold)
    return (observed * sample_weight) + (league_avg * (1 - sample_weight))


# ── Bulk data loaders (one round-trip each) ────────────────────────────────

def _load_pitcher_pitch_mix(conn, as_of_date, window_code):
    """Returns {(pitcher_id, split_hand): [(pitch_type_code, usage_pct, pitches_thrown)]}"""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT pitcher_id, split_hand, pitch_type_code, usage_pct, pitches_thrown
        FROM   fact_pitcher_pitch_mix
        WHERE  as_of_date = :aod AND window_code = :wc
          AND  usage_pct IS NOT NULL AND pitches_thrown > 0
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    result = defaultdict(list)
    for pid, hand, ptc, usage, pitches in cur.fetchall():
        result[(pid, hand)].append((ptc, usage, pitches))
    return result


def _load_pitcher_zone_profile(conn, as_of_date, window_code):
    """Returns {(pitcher_id, split_hand): [(zone_code, total_pitches)]}"""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT pitcher_id, split_hand, zone_code, SUM(pitches_thrown) AS total_pitches
        FROM   fact_pitcher_zone_profile
        WHERE  as_of_date = :aod AND window_code = :wc
          AND  pitches_thrown > 0
        GROUP  BY pitcher_id, split_hand, zone_code
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    result = defaultdict(list)
    for pid, hand, zc, pitches in cur.fetchall():
        result[(pid, hand)].append((zc, pitches))
    return result


def _load_batter_pitch_type_splits(conn, as_of_date, window_code):
    """Returns {(player_id, split_hand, pitch_type_code): (batting_avg, slugging_pct, pitches_seen)}"""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT player_id, split_hand, pitch_type_code,
               batting_avg, slugging_pct, pitches_seen
        FROM   fact_batter_pitch_type_splits
        WHERE  as_of_date = :aod AND window_code = :wc
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    return {(pid, hand, ptc): (ba, slg, ps)
            for pid, hand, ptc, ba, slg, ps in cur.fetchall()}


def _load_batter_zone_splits(conn, as_of_date, window_code):
    """Returns {(player_id, split_hand, zone_code): (batting_avg, slugging_pct, pitches_seen)}"""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT player_id, split_hand, zone_code,
               batting_avg, slugging_pct, pitches_seen
        FROM   fact_batter_zone_splits
        WHERE  as_of_date = :aod AND window_code = :wc
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    return {(pid, hand, zc): (ba, slg, ps)
            for pid, hand, zc, ba, slg, ps in cur.fetchall()}


def _load_batter_hand_splits(conn, as_of_date, window_code):
    """Returns {(player_id, split_hand): (batting_avg, slugging_pct)}"""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT player_id, split_hand, batting_avg, slugging_pct
        FROM   fact_batter_hand_splits
        WHERE  as_of_date = :aod AND window_code = :wc
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    return {(pid, hand): (ba, slg) for pid, hand, ba, slg in cur.fetchall()}


def _load_pitcher_hand_splits(conn, as_of_date, window_code):
    """Returns {(pitcher_id, split_hand): (batting_avg_allowed, slugging_pct_allowed)}"""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT pitcher_id, split_hand, batting_avg_allowed, slugging_pct_allowed
        FROM   fact_pitcher_hand_splits
        WHERE  as_of_date = :aod AND window_code = :wc
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    return {(pid, hand): (ba, slg) for pid, hand, ba, slg in cur.fetchall()}


def _load_batter_power_profile(conn, as_of_date, window_code):
    """Returns {player_id: (hr_per_pa, barrels_per_pa, bpp_vs_rhp, bpp_vs_lhp, bbe)}"""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT player_id, hr_per_pa, barrels_per_pa,
               barrels_per_pa_vs_rhp, barrels_per_pa_vs_lhp, batted_ball_events
        FROM   fact_batter_power_profile
        WHERE  as_of_date = :aod AND window_code = :wc
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    return {pid: (hr, bpp, bpp_r, bpp_l, bbe)
            for pid, hr, bpp, bpp_r, bpp_l, bbe in cur.fetchall()}


def _load_pitcher_hr_vulnerability(conn, as_of_date, window_code):
    """Returns {(pitcher_id, split_hand): (hr_per_bf, barrel_rate, bbe)}"""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT pitcher_id, split_hand, hr_per_bf_allowed, barrel_rate_allowed, batted_ball_events
        FROM   fact_pitcher_hr_vulnerability
        WHERE  as_of_date = :aod AND window_code = :wc
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    return {(pid, hand): (hr, br, bbe)
            for pid, hand, hr, br, bbe in cur.fetchall()}


def _load_venues(conn):
    """Returns {venue_id: (park_hr_factor_lhb, park_hr_factor_rhb, park_run_factor)}"""
    cur = conn.cursor()
    cur.execute(
        "SELECT venue_id, park_hr_factor_lhb, park_hr_factor_rhb, park_run_factor FROM dim_venues"
    )
    return {vid: (lhb, rhb, run) for vid, lhb, rhb, run in cur.fetchall()}


def _load_lineup_slots(conn, as_of_date):
    """Returns {(game_id, player_id): lineup_slot}"""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT game_id, player_id, lineup_slot
        FROM   fact_game_lineups
        WHERE  as_of_date = :aod
        """,
        {"aod": as_of_date},
    )
    return {(gid, pid): slot for gid, pid, slot in cur.fetchall()}


def _load_batter_overall(conn, as_of_date, window_code):
    """Returns {player_id: ab_per_game}"""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT player_id, ab_per_game
        FROM   fact_batter_overall
        WHERE  as_of_date = :aod AND window_code = :wc
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    return {pid: abpg for pid, abpg in cur.fetchall()}


# ── In-memory scoring functions ────────────────────────────────────────────

def _score_pitch_type(batter_id, pitcher_id, pitcher_hand, batter_hand,
                      pitch_mix_data, batter_pt_data, league_avgs_pt,
                      league_slgs_pt, regression_weight):
    """Returns (pt_ba_score, pt_slg_score) using preloaded dicts."""
    mix = pitch_mix_data.get((pitcher_id, batter_hand), [])
    if not mix:
        return None, None
    total_usage = sum(u for _, u, _ in mix)
    if total_usage <= 0:
        return None, None

    weighted_ba = weighted_slg = coverage = 0.0
    for ptc, raw_usage, _ in mix:
        usage = raw_usage / total_usage
        row = batter_pt_data.get((batter_id, pitcher_hand, ptc))
        ba       = row[0] if row and row[0] is not None else None
        slg      = row[1] if row and row[1] is not None else None
        ps       = row[2] if row and row[2] is not None else 0
        lg_ba    = league_avgs_pt.get(ptc, REGRESSION_TARGET)
        lg_slg   = league_slgs_pt.get(ptc, SLG_REGRESSION_TARGET)
        reg_ba   = REGRESSION_TARGET if ps < MIN_PITCHES_THRESHOLD else lg_ba
        weighted_ba  += usage * _regress(ba,  ps, reg_ba,  MIN_PITCHES_THRESHOLD, regression_weight)
        weighted_slg += usage * _regress(slg, ps, lg_slg, MIN_PITCHES_THRESHOLD, regression_weight)
        coverage += usage

    if coverage < 0.25:
        return None, None
    return round(weighted_ba, 4), round(weighted_slg, 4)


def _score_zone(batter_id, pitcher_id, pitcher_hand, batter_hand,
                zone_profile_data, batter_zone_data, league_avgs_zone,
                league_slgs_zone, regression_weight):
    """Returns (zone_ba_score, zone_slg_score) using preloaded dicts."""
    profile = zone_profile_data.get((pitcher_id, batter_hand), [])
    if not profile:
        return None, None
    total_pitches = sum(p for _, p in profile)
    if total_pitches <= 0:
        return None, None

    weighted_ba = weighted_slg = coverage = 0.0
    for zc, zone_pitches in profile:
        zone_usage = zone_pitches / total_pitches
        row = batter_zone_data.get((batter_id, pitcher_hand, zc))
        ba      = row[0] if row and row[0] is not None else None
        slg     = row[1] if row and row[1] is not None else None
        ps      = row[2] if row and row[2] is not None else 0
        lg_ba   = league_avgs_zone.get(zc, REGRESSION_TARGET)
        lg_slg  = league_slgs_zone.get(zc, SLG_REGRESSION_TARGET)
        reg_ba  = REGRESSION_TARGET if ps < MIN_PITCHES_THRESHOLD else lg_ba
        weighted_ba  += zone_usage * _regress(ba,  ps, reg_ba, MIN_PITCHES_THRESHOLD, regression_weight)
        weighted_slg += zone_usage * _regress(slg, ps, lg_slg, MIN_PITCHES_THRESHOLD, regression_weight)
        coverage += zone_usage

    if coverage < 0.25:
        return None, None
    return round(weighted_ba, 4), round(weighted_slg, 4)


def _score_hr(batter_id, pitcher_id, effective_batter_hand, pitcher_throws,
              power_profile_data, hr_vuln_data, park_hr_factor,
              weather_adj, regression_weight, ab_per_game):
    """Returns (proj_hr_prob, batter_bpp, pitcher_barrel_rate)."""
    power = power_profile_data.get(batter_id)
    if not power:
        return None, None, None

    hr_per_pa, overall_bpp, bpp_vs_rhp, bpp_vs_lhp, bbe = power
    bpp_vs_hand = bpp_vs_rhp if pitcher_throws == 'R' else bpp_vs_lhp
    batter_bpp  = bpp_vs_hand if bpp_vs_hand is not None else overall_bpp
    bbe_count   = bbe or 0

    if hr_per_pa is None:
        return None, batter_bpp, None

    batter_hr_rate = _regress(hr_per_pa, bbe_count, LEAGUE_AVG_HR_PER_PA,
                              MIN_BBE_THRESHOLD, regression_weight)

    vuln = hr_vuln_data.get((pitcher_id, effective_batter_hand))
    pitcher_hr_per_bf   = vuln[0] if vuln and vuln[0] is not None else None
    pitcher_barrel_rate = vuln[1] if vuln and vuln[1] is not None else None
    pitcher_bbe         = vuln[2] if vuln and vuln[2] is not None else 0

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

    proj_hr_prob = round(blended * ab_per_game * park_hr_factor * weather_adj, 4)
    return proj_hr_prob, batter_bpp, pitcher_barrel_rate


# ── Main scoring function ──────────────────────────────────────────────────

def compute_match_scores(conn, as_of_date, window_code="SEASON"):
    log.info("Computing match scores for %s (window=%s)...", as_of_date, window_code)

    # ── Load everything into memory upfront (one round-trip per table) ──
    cur = conn.cursor()

    # League averages
    cur.execute(
        """
        SELECT pitch_type_code,
               CAST(SUM(hits) AS REAL) / NULLIF(SUM(at_bats), 0),
               CAST(SUM(total_bases) AS REAL) / NULLIF(SUM(at_bats), 0)
        FROM   fact_batter_pitch_type_splits
        WHERE  as_of_date = :aod AND window_code = :wc
        GROUP  BY pitch_type_code
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    league_avgs_pt  = {}
    league_slgs_pt  = {}
    for ptc, ba, slg in cur.fetchall():
        if ba  is not None: league_avgs_pt[ptc]  = ba
        if slg is not None: league_slgs_pt[ptc] = slg
    log.info("  League avg by pitch type: %d types.", len(league_avgs_pt))

    cur.execute(
        """
        SELECT zone_code,
               CAST(SUM(hits) AS REAL) / NULLIF(SUM(in_play_events), 0),
               CAST(SUM(total_bases) AS REAL) / NULLIF(SUM(in_play_events), 0)
        FROM   fact_batter_zone_splits
        WHERE  as_of_date = :aod AND window_code = :wc
        GROUP  BY zone_code
        """,
        {"aod": as_of_date, "wc": window_code},
    )
    league_avgs_zone = {}
    league_slgs_zone = {}
    for zc, ba, slg in cur.fetchall():
        if ba  is not None: league_avgs_zone[zc]  = ba
        if slg is not None: league_slgs_zone[zc] = slg
    log.info("  League avg by zone: %d zones.", len(league_avgs_zone))

    # Dynamic league BA/SLG
    cur.execute(
        "SELECT CAST(SUM(hits) AS REAL) / NULLIF(SUM(at_bats), 0) "
        "FROM fact_batter_overall WHERE window_code = :wc AND at_bats >= 50",
        {"wc": window_code},
    )
    row = cur.fetchone()
    dynamic_league_ba = row[0] if row and row[0] else LEAGUE_AVG_BA

    # Regression weight
    cur.execute(
        "SELECT regression_weight FROM dim_split_windows WHERE window_code = :wc",
        {"wc": window_code},
    )
    rw_row = cur.fetchone()
    regression_weight = rw_row[0] if rw_row else 1.0

    # All split tables
    pitch_mix_data    = _load_pitcher_pitch_mix(conn, as_of_date, window_code)
    zone_profile_data = _load_pitcher_zone_profile(conn, as_of_date, window_code)
    batter_pt_data    = _load_batter_pitch_type_splits(conn, as_of_date, window_code)
    batter_zone_data  = _load_batter_zone_splits(conn, as_of_date, window_code)
    batter_hand_data  = _load_batter_hand_splits(conn, as_of_date, window_code)
    pitcher_hand_data = _load_pitcher_hand_splits(conn, as_of_date, window_code)
    power_profile_data= _load_batter_power_profile(conn, as_of_date, window_code)
    hr_vuln_data      = _load_pitcher_hr_vulnerability(conn, as_of_date, window_code)
    venues_data       = _load_venues(conn)
    lineup_slots      = _load_lineup_slots(conn, as_of_date)
    batter_overall    = _load_batter_overall(conn, as_of_date, window_code)

    # Matchup rows
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

    # ── Score all rows in memory, collect updates ──────────────────────
    update_rows = []
    skipped = 0

    for (game_id, batter_id, pitcher_id, batter_avg, pitcher_avg_allowed,
         park_adj, weather_adj, pitcher_throws, batter_bats, venue_id) in matchups:

        if not pitcher_throws or not batter_bats:
            skipped += 1
            continue
        if pitcher_throws not in ('R', 'L'):
            skipped += 1
            continue

        effective_batter_hand = (
            ('L' if pitcher_throws == 'R' else 'R') if batter_bats == 'S'
            else batter_bats
        )

        # Park HR factor
        venue = venues_data.get(venue_id)
        if venue:
            lhb_factor, rhb_factor, run_factor = venue
            if effective_batter_hand == 'L' and lhb_factor is not None:
                park_hr_factor = round(lhb_factor / 100.0, 4)
            elif rhb_factor is not None:
                park_hr_factor = round(rhb_factor / 100.0, 4)
            elif run_factor is not None:
                park_hr_factor = run_factor
            else:
                park_hr_factor = 1.0
        else:
            park_hr_factor = 1.0

        park    = park_adj    or 1.0
        weather = weather_adj or 1.0

        # Baseline BA
        b_avg = batter_avg          if batter_avg          is not None else REGRESSION_TARGET
        p_avg = pitcher_avg_allowed if pitcher_avg_allowed is not None else REGRESSION_TARGET
        baseline_avg = round((b_avg * 0.30) + (p_avg * 0.70), 5)

        # PT and zone scores (BA + SLG together, one pass each)
        pt_score, pt_slg_score = _score_pitch_type(
            batter_id, pitcher_id, pitcher_throws, effective_batter_hand,
            pitch_mix_data, batter_pt_data, league_avgs_pt, league_slgs_pt,
            regression_weight)

        zone_score, zone_slg_score = _score_zone(
            batter_id, pitcher_id, pitcher_throws, effective_batter_hand,
            zone_profile_data, batter_zone_data, league_avgs_zone, league_slgs_zone,
            regression_weight)

        # Projected BA
        if pt_score and zone_score:
            blended = (baseline_avg * 0.70) + (pt_score * 0.20) + (zone_score * 0.10)
        elif pt_score:
            blended = (baseline_avg * 0.80) + (pt_score * 0.20)
        elif zone_score:
            blended = (baseline_avg * 0.90) + (zone_score * 0.10)
        else:
            blended = baseline_avg
        projected = round(blended * park * weather, 4)

        # SLG baseline
        bh = batter_hand_data.get((batter_id, pitcher_throws))
        ph = pitcher_hand_data.get((pitcher_id, effective_batter_hand))
        b_slg = bh[1] if bh and bh[1] is not None else SLG_REGRESSION_TARGET
        p_slg = ph[1] if ph and ph[1] is not None else SLG_REGRESSION_TARGET
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

        # AB per game from lineup slot
        slot = lineup_slots.get((game_id, batter_id))
        if slot and slot in SLOT_AB:
            ab_per_game = SLOT_AB[slot]
        else:
            ab_per_game = batter_overall.get(batter_id) or 3.502

        proj_tb = round(proj_slg * ab_per_game, 4)

        # HR probability
        proj_hr_prob, batter_barrel_rate, pitcher_barrel_rate = _score_hr(
            batter_id, pitcher_id, effective_batter_hand, pitcher_throws,
            power_profile_data, hr_vuln_data, park_hr_factor, weather,
            regression_weight, ab_per_game)

        update_rows.append({
            "as_of_date":               as_of_date,
            "game_id":                  game_id,
            "batter_id":                batter_id,
            "pitcher_id":               pitcher_id,
            "window_code":              window_code,
            "pitch_type_match_score":   pt_score,
            "zone_match_score":         zone_score,
            "projected_batting_avg":    projected,
            "pt_slg_score":             pt_slg_score,
            "zone_slg_score":           zone_slg_score,
            "projected_slugging":       proj_slg,
            "projected_total_bases":    proj_tb,
            "proj_at_bats_per_game":    ab_per_game,
            "projected_hr_probability": proj_hr_prob,
            "batter_barrel_rate":       batter_barrel_rate,
            "pitcher_barrel_rate_allowed": pitcher_barrel_rate,
        })

    # ── Batch update via bulk_upsert ───────────────────────────────────
    if update_rows:
        bulk_upsert(
            conn, "fact_matchup_batter_pitcher", update_rows,
            conflict_cols="as_of_date,game_id,batter_id,pitcher_id,window_code",
            update_cols=[
                "pitch_type_match_score", "zone_match_score",
                "projected_batting_avg", "pt_slg_score", "zone_slg_score",
                "projected_slugging", "projected_total_bases",
                "proj_at_bats_per_game", "projected_hr_probability",
                "batter_barrel_rate", "pitcher_barrel_rate_allowed",
            ],
        )
        conn.commit()

    log.info("  Match scores written: %d updated, %d skipped.", len(update_rows), skipped)


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
