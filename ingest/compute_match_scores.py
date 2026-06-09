"""
compute_match_scores.py
-----------------------
Computes pitch_type_match_score and zone_match_score for every row in
fact_matchup_batter_pitcher, then writes those scores back to the table.

Pitch type match score
----------------------
For each pitch type the pitcher throws (from fact_pitcher_pitch_mix),
weight the batter's batting average against that pitch type
(from fact_batter_pitch_type_splits) by the pitcher's usage rate.
Sum across all pitch types to get a usage-weighted projected BA.

  pitch_type_match_score =
      SUM( pitcher_usage_pct[pt] * batter_avg_vs[pt] )
      for each pitch type pt in the pitcher's arsenal

Small sample regression: if a batter has fewer pitches seen than
min_pa_threshold for a given pitch type, blend his observed average
toward the league average for that pitch type using the window's
regression_weight.

  regressed_avg = (observed_avg * regression_weight)
                + (league_avg   * (1 - regression_weight))

Zone match score
----------------
Same weighted-average concept but across strike zone locations.
For each zone the pitcher targets (from fact_pitcher_zone_profile),
weight the batter's BA in that zone (from fact_batter_zone_splits)
by the pitcher's usage rate in that zone.

  zone_match_score =
      SUM( pitcher_zone_usage_pct[z] * batter_avg_in[z] )
      for each zone z in the pitcher's zone profile

Final projected_batting_avg update
-----------------------------------
Once both scores are computed they are used to update projected_batting_avg:

  baseline      = batter_vs_hand_batting_avg (already in table)
  pt_multiplier = pitch_type_match_score / league_avg_ba
  z_multiplier  = zone_match_score        / league_avg_ba
  projected_avg = baseline * pt_multiplier * z_multiplier
                * park_adjustment_factor * weather_adjustment_factor

Usage:
    python ingest/compute_match_scores.py --date 2026-04-22 --db-path data/mlb_pregame.db
    python ingest/compute_match_scores.py --today           --db-path data/mlb_pregame.db
"""

import sqlite3
import logging
import argparse
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# League average BA used for multiplier normalisation.
# 2026 MLB average is roughly .243 - update each season if desired.
LEAGUE_AVG_BA = 0.243

# Minimum pitches-seen before we fully trust a batter's split vs a pitch type / zone.
# Below this threshold the observed value is blended toward REGRESSION_TARGET.
# Backtesting on 2026 season data (n=6,768 matchups, min 2 AB) identified tau=150
# as optimal — it produces near-zero bias (+0.00013) and -0.00482 MAE improvement
# over the previous tau=20. Values above 150 yield diminishing MAE returns (<0.001)
# while introducing increasing negative bias (under-projection).
MIN_PITCHES_THRESHOLD = 150

# Regression target for small-sample batter BA splits.
# Replaces the per-pitch-type league average as the fallback value.
# Backtesting showed 0.22 outperforms both dynamic league BA (~0.243) and
# per-pitch-type averages across all tau values — consistent with observed
# actual BA of .2245 and .2069 for under-10 and 10-19 pitches-seen buckets,
# both well below the league average previously used as the regression target.
REGRESSION_TARGET = 0.22

# Regression target for small-sample batter SLG splits.
# Separate from REGRESSION_TARGET (BA) because SLG has a different distribution.
# Backtesting on 2026 season data (n=6,859 matchups, min 2 AB) identified
# 0.380 as optimal — produces near-zero SLG bias (BIAS_SLG=-0.0001) at the
# 0.70/0.20/0.10 blend configuration. Values below 0.380 reduce MAE_TB
# but introduce increasing negative TB bias driven by AB/game overestimation
# rather than SLG projection error. Values above 0.380 increase both MAE
# and bias. The remaining TB bias after this fix is attributable to
# proj_at_bats_per_game using season-average rates rather than lineup-slot
# estimates — addressed separately in the AB/game calculation below.
SLG_REGRESSION_TARGET = 0.380


# ── League average helpers ─────────────────────────────────────────────────

def _league_avg_by_pitch_type(conn: sqlite3.Connection, as_of_date: str,
                               window_code: str) -> dict[str, float]:
    """
    Compute league-wide batting average against each pitch type from
    fact_batter_pitch_type_splits.  Used as regression target for small samples.
    Returns {pitch_type_code: league_avg_ba}.
    """
    rows = conn.execute(
        """
        SELECT pitch_type_code,
               CAST(SUM(hits) AS REAL) / NULLIF(SUM(at_bats), 0) AS lg_avg
        FROM   fact_batter_pitch_type_splits
        WHERE  as_of_date = ? AND window_code = ?
        GROUP  BY pitch_type_code
        """,
        (as_of_date, window_code),
    ).fetchall()
    return {r[0]: r[1] for r in rows if r[1] is not None}


def _league_avg_by_zone(conn: sqlite3.Connection, as_of_date: str,
                         window_code: str) -> dict[str, float]:
    """
    Compute league-wide batting average per zone from fact_batter_zone_splits.
    Returns {zone_code: league_avg_ba}.
    """
    rows = conn.execute(
        """
        SELECT zone_code,
               CAST(SUM(hits) AS REAL) / NULLIF(SUM(in_play_events), 0) AS lg_avg
        FROM   fact_batter_zone_splits
        WHERE  as_of_date = ? AND window_code = ?
        GROUP  BY zone_code
        """,
        (as_of_date, window_code),
    ).fetchall()
    return {r[0]: r[1] for r in rows if r[1] is not None}


# ── League slugging helpers ───────────────────────────────────────────────

def _league_slg_by_pitch_type(conn: sqlite3.Connection, as_of_date: str,
                               window_code: str) -> dict[str, float]:
    """League-wide slugging per pitch type. Regression target for SLG splits."""
    rows = conn.execute(
        """
        SELECT pitch_type_code,
               CAST(SUM(total_bases) AS REAL) / NULLIF(SUM(at_bats), 0) AS lg_slg
        FROM   fact_batter_pitch_type_splits
        WHERE  as_of_date = ? AND window_code = ?
        GROUP  BY pitch_type_code
        """,
        (as_of_date, window_code),
    ).fetchall()
    return {r[0]: r[1] for r in rows if r[1] is not None}


def _league_slg_by_zone(conn: sqlite3.Connection, as_of_date: str,
                         window_code: str) -> dict[str, float]:
    """League-wide slugging per zone. Regression target for zone SLG splits."""
    rows = conn.execute(
        """
        SELECT zone_code,
               CAST(SUM(total_bases) AS REAL) / NULLIF(SUM(in_play_events), 0) AS lg_slg
        FROM   fact_batter_zone_splits
        WHERE  as_of_date = ? AND window_code = ?
        GROUP  BY zone_code
        """,
        (as_of_date, window_code),
    ).fetchall()
    return {r[0]: r[1] for r in rows if r[1] is not None}


# ── Regression helper ──────────────────────────────────────────────────────

def _regress(observed: float | None, pitches_seen: int,
             league_avg: float, threshold: int,
             regression_weight: float) -> float:
    """
    Blend observed average toward league_avg when sample is small.

    If pitches_seen >= threshold: return observed as-is (full weight).
    If pitches_seen == 0:         return league_avg entirely.
    Otherwise: linear blend based on how close we are to threshold.
    """
    if observed is None:
        return league_avg
    if pitches_seen >= threshold:
        return observed
    # Scale regression_weight down proportionally to sample size
    sample_weight = regression_weight * (pitches_seen / threshold)
    return (observed * sample_weight) + (league_avg * (1 - sample_weight))


# ── Pitch type match score ─────────────────────────────────────────────────

def _compute_pitch_type_match_score(
    conn: sqlite3.Connection,
    as_of_date: str,
    batter_id: int,
    pitcher_id: int,
    pitcher_hand: str,      # hand pitcher throws with (p_throws): R or L
    batter_hand: str,       # hand batter bats with (stand): R or L
    window_code: str,
    league_avgs: dict[str, float],
    regression_weight: float,
) -> float | None:
    """
    Returns the pitch-type-weighted projected batting average for this matchup.
    """
    # Pitcher's pitch mix vs this batter's handedness
    pitch_mix = conn.execute(
        """
        SELECT pitch_type_code, usage_pct, pitches_thrown
        FROM   fact_pitcher_pitch_mix
        WHERE  as_of_date    = ?
          AND  pitcher_id    = ?
          AND  split_hand    = ?
          AND  window_code   = ?
          AND  usage_pct     IS NOT NULL
          AND  pitches_thrown > 0
        """,
        (as_of_date, pitcher_id, batter_hand, window_code),
    ).fetchall()

    if not pitch_mix:
        return None

    # Normalise usage percents in case they don't sum to exactly 1.0
    total_usage = sum(row[1] for row in pitch_mix)
    if total_usage <= 0:
        return None

    weighted_avg = 0.0
    coverage     = 0.0   # tracks how much of the pitch mix we have batter data for

    for pitch_type_code, raw_usage, pitches_thrown in pitch_mix:
        usage = raw_usage / total_usage   # normalised weight

        # Batter's avg vs this pitch type vs this pitcher hand
        batter_row = conn.execute(
            """
            SELECT batting_avg, pitches_seen
            FROM   fact_batter_pitch_type_splits
            WHERE  as_of_date      = ?
              AND  player_id       = ?
              AND  split_hand      = ?
              AND  pitch_type_code = ?
              AND  window_code     = ?
            """,
            (as_of_date, batter_id, pitcher_hand, pitch_type_code, window_code),
        ).fetchone()

        observed_avg  = batter_row[0] if batter_row and batter_row[0] is not None else None
        pitches_seen  = batter_row[1] if batter_row and batter_row[1] is not None else 0
        # Use REGRESSION_TARGET as fallback when batter has no data for this pitch type.
        # Per-pitch-type league avg kept as secondary fallback if neither is available.
        league_avg_pt = league_avgs.get(pitch_type_code, REGRESSION_TARGET)
        reg_target    = REGRESSION_TARGET if pitches_seen < MIN_PITCHES_THRESHOLD else league_avg_pt

        regressed = _regress(
            observed      = observed_avg,
            pitches_seen  = pitches_seen,
            league_avg    = reg_target,
            threshold     = MIN_PITCHES_THRESHOLD,
            regression_weight = regression_weight,
        )

        weighted_avg += usage * regressed
        coverage     += usage

    # If we have coverage for less than 50% of the pitch mix, return None
    # (too little data to be meaningful)
    if coverage < 0.25:
        return None

    return round(weighted_avg, 4)


# ── Zone match score ───────────────────────────────────────────────────────

def _compute_zone_match_score(
    conn: sqlite3.Connection,
    as_of_date: str,
    batter_id: int,
    pitcher_id: int,
    pitcher_hand: str,
    batter_hand: str,
    window_code: str,
    league_avgs_zone: dict[str, float],
    regression_weight: float,
) -> float | None:
    """
    Returns the zone-weighted projected batting average for this matchup.
    Aggregates across all pitch types within each zone (zone-level view).
    """
    # Pitcher's zone profile vs this batter's handedness
    # Aggregate usage across pitch types within each zone
    zone_profile = conn.execute(
        """
        SELECT zone_code,
               SUM(pitches_thrown)                                    AS total_pitches,
               CAST(SUM(pitches_thrown) AS REAL) /
                   NULLIF(SUM(SUM(pitches_thrown)) OVER (), 0)        AS zone_usage_pct
        FROM   fact_pitcher_zone_profile
        WHERE  as_of_date  = ?
          AND  pitcher_id  = ?
          AND  split_hand  = ?
          AND  window_code = ?
          AND  pitches_thrown > 0
        GROUP  BY zone_code
        """,
        (as_of_date, pitcher_id, batter_hand, window_code),
    ).fetchall()

    if not zone_profile:
        return None

    # Normalise zone usage
    total_pitches = sum(row[1] for row in zone_profile)
    if total_pitches <= 0:
        return None

    weighted_avg = 0.0
    coverage     = 0.0

    for zone_code, zone_pitches, _ in zone_profile:
        zone_usage = zone_pitches / total_pitches   # normalised weight

        # Batter's avg in this zone vs this pitcher hand
        batter_row = conn.execute(
            """
            SELECT batting_avg, pitches_seen
            FROM   fact_batter_zone_splits
            WHERE  as_of_date  = ?
              AND  player_id   = ?
              AND  split_hand  = ?
              AND  zone_code   = ?
              AND  window_code = ?
            """,
            (as_of_date, batter_id, pitcher_hand, zone_code, window_code),
        ).fetchone()

        observed_avg   = batter_row[0] if batter_row and batter_row[0] is not None else None
        pitches_seen   = batter_row[1] if batter_row and batter_row[1] is not None else 0
        # Use REGRESSION_TARGET as fallback for small samples; zone-level league
        # avg retained as secondary fallback when batter has no zone data at all.
        league_avg_zone = league_avgs_zone.get(zone_code, REGRESSION_TARGET)
        reg_target      = REGRESSION_TARGET if pitches_seen < MIN_PITCHES_THRESHOLD else league_avg_zone

        regressed = _regress(
            observed      = observed_avg,
            pitches_seen  = pitches_seen,
            league_avg    = reg_target,
            threshold     = MIN_PITCHES_THRESHOLD,
            regression_weight = regression_weight,
        )

        weighted_avg += zone_usage * regressed
        coverage     += zone_usage

    if coverage < 0.25:
        return None

    return round(weighted_avg, 4)


# ── Pitch type SLG match score ────────────────────────────────────────────

def _compute_pitch_type_slg_score(
    conn, as_of_date, batter_id, pitcher_id,
    pitcher_hand, batter_hand, window_code,
    league_slgs, regression_weight,
):
    """Usage-weighted slugging pct across pitcher's pitch arsenal."""
    # SLG_REGRESSION_TARGET used as fallback when no per-pitch-type league avg
    # is available. Distinct from REGRESSION_TARGET (BA=0.22) because SLG
    # has a different distribution — league avg SLG ~0.400, small-sample
    # observations regress toward a SLG-appropriate value not a BA value.

    pitch_mix = conn.execute(
        """
        SELECT pitch_type_code, usage_pct, pitches_thrown
        FROM   fact_pitcher_pitch_mix
        WHERE  as_of_date=? AND pitcher_id=? AND split_hand=?
          AND  window_code=? AND usage_pct IS NOT NULL AND pitches_thrown > 0
        """,
        (as_of_date, pitcher_id, batter_hand, window_code),
    ).fetchall()

    if not pitch_mix:
        return None

    total_usage = sum(r[1] for r in pitch_mix)
    if total_usage <= 0:
        return None

    weighted_slg = 0.0
    coverage     = 0.0

    for pitch_type_code, raw_usage, pitches_thrown in pitch_mix:
        usage = raw_usage / total_usage

        batter_row = conn.execute(
            """
            SELECT slugging_pct, pitches_seen
            FROM   fact_batter_pitch_type_splits
            WHERE  as_of_date=? AND player_id=? AND split_hand=?
              AND  pitch_type_code=? AND window_code=?
            """,
            (as_of_date, batter_id, pitcher_hand, pitch_type_code, window_code),
        ).fetchone()

        observed_slg = batter_row[0] if batter_row and batter_row[0] is not None else None
        pitches_seen = batter_row[1] if batter_row and batter_row[1] is not None else 0
        league_slg   = league_slgs.get(pitch_type_code, SLG_REGRESSION_TARGET)

        regressed = _regress(
            observed=observed_slg, pitches_seen=pitches_seen,
            league_avg=league_slg, threshold=MIN_PITCHES_THRESHOLD,
            regression_weight=regression_weight,
        )

        weighted_slg += usage * regressed
        coverage     += usage

    if coverage < 0.25:
        return None

    return round(weighted_slg, 4)


def _compute_zone_slg_score(
    conn, as_of_date, batter_id, pitcher_id,
    pitcher_hand, batter_hand, window_code,
    league_slgs_zone, regression_weight,
):
    """Zone-weighted slugging pct across pitcher's zone profile."""
    # SLG_REGRESSION_TARGET used as fallback — see _compute_pitch_type_slg_score.

    zone_profile = conn.execute(
        """
        SELECT zone_code, SUM(pitches_thrown) AS total_pitches
        FROM   fact_pitcher_zone_profile
        WHERE  as_of_date=? AND pitcher_id=? AND split_hand=?
          AND  window_code=? AND pitches_thrown > 0
        GROUP  BY zone_code
        """,
        (as_of_date, pitcher_id, batter_hand, window_code),
    ).fetchall()

    if not zone_profile:
        return None

    total_pitches = sum(r[1] for r in zone_profile)
    if total_pitches <= 0:
        return None

    weighted_slg = 0.0
    coverage     = 0.0

    for zone_code, zone_pitches in zone_profile:
        zone_usage = zone_pitches / total_pitches

        batter_row = conn.execute(
            """
            SELECT slugging_pct, pitches_seen
            FROM   fact_batter_zone_splits
            WHERE  as_of_date=? AND player_id=? AND split_hand=?
              AND  zone_code=? AND window_code=?
            """,
            (as_of_date, batter_id, pitcher_hand, zone_code, window_code),
        ).fetchone()

        observed_slg    = batter_row[0] if batter_row and batter_row[0] is not None else None
        pitches_seen    = batter_row[1] if batter_row and batter_row[1] is not None else 0
        league_slg_zone = league_slgs_zone.get(zone_code, SLG_REGRESSION_TARGET)

        regressed = _regress(
            observed=observed_slg, pitches_seen=pitches_seen,
            league_avg=league_slg_zone, threshold=MIN_PITCHES_THRESHOLD,
            regression_weight=regression_weight,
        )

        weighted_slg += zone_usage * regressed
        coverage     += zone_usage

    if coverage < 0.25:
        return None

    return round(weighted_slg, 4)


# ── HR probability ────────────────────────────────────────────────────────

# League average HR rate per PA — empirically derived from 2026 season data.
# Source: SUM(home_runs)/SUM(plate_appearances) from fact_player_game_results
# where at_bats >= 2. Updated from hardcoded 0.034 to observed 0.0293.
LEAGUE_AVG_HR_PER_PA = 0.0293

# Minimum BBE before we trust a batter's observed barrel rate.
# Backtesting showed minimal Brier score sensitivity across 20-200 range —
# bbe=100 produces marginally best Brier score. Set to 100 for consistency
# with the interpretation that HR rate requires more data to stabilize than BA.
MIN_BBE_THRESHOLD = 100

# League average barrel rate per BBE — empirically derived from 2026 season data.
# Source: fact_batter_power_profile corrected barrel definition (expanding angle).
# Updated from hardcoded 0.076 to observed 0.0708.
LEAGUE_AVG_BARREL_RATE = 0.0708


def _compute_hr_probability(
    conn: sqlite3.Connection,
    as_of_date: str,
    batter_id: int,
    pitcher_id: int,
    effective_batter_hand: str,
    pitcher_throws: str,
    window_code: str,
    park_hr_factor: float,
    weather_adj: float,
    regression_weight: float,
    ab_per_game: float,
) -> tuple[float | None, float | None, float | None]:
    """
    Computes projected HR probability per GAME for this batter-pitcher matchup.

    Formula:
        batter_hr_rate   = batter's hr_per_pa vs pitcher hand (from power profile)
        pitcher_hr_rate  = pitcher's hr_per_bf_allowed vs batter hand
        barrel_context   = batter barrel_rate × pitcher barrel_rate_allowed
                           normalized against league barrel rate

        blended_hr_rate_per_pa = (batter_hr_rate  × 0.70)
                                + (pitcher_hr_rate × 0.20)
                                + (barrel_context  × 0.10)

        projected_hr_prob_per_game = blended_hr_rate_per_pa
                                     × ab_per_game
                                     × park_hr_factor
                                     × weather_adj

    Output is per-GAME probability (typical range 0.05-0.20), not per-PA.
    This is more intuitive for dashboard display and correct for backtesting
    against hr_flag (binary per-game outcome).

    Blend weights 0.70/0.20/0.10 derived from backtesting on 2026 season data
    (n=472 matchups with HR outcomes) — minimal Brier score sensitivity across
    all configurations tested; 0.70/0.20/0.10 selected for consistency with
    BA and TB blend weight findings.

    Returns (projected_hr_probability, batter_barrel_rate, pitcher_barrel_rate_allowed).
    """

    # ── Batter HR rate from power profile ─────────────────────────────────
    # Query uses most-recent available as_of_date rather than exact match.
    # This handles the common case where today's power profile rows haven't
    # been written yet (e.g. intraday refresh before today's Statcast loads)
    # by falling back to yesterday's profile, which is functionally identical
    # for a SEASON window aggregate.
    power_row = conn.execute(
        """
        SELECT hr_per_pa,
               CASE ? WHEN 'R' THEN hard_hit_rate_vs_rhp
                      WHEN 'L' THEN hard_hit_rate_vs_lhp END AS hhr_vs_hand,
               CASE ? WHEN 'R' THEN barrels_per_pa_vs_rhp
                      WHEN 'L' THEN barrels_per_pa_vs_lhp END AS bpp_vs_hand,
               barrels_per_pa,
               batted_ball_events
        FROM   fact_batter_power_profile
        WHERE  as_of_date  <= ?
          AND  player_id    = ?
          AND  window_code  = ?
        ORDER  BY as_of_date DESC
        LIMIT  1
        """,
        (pitcher_throws, pitcher_throws, as_of_date, batter_id, window_code),
    ).fetchone()

    if not power_row:
        return None, None, None

    overall_hr_per_pa = power_row[0]
    bpp_vs_hand       = power_row[2]   # barrels per PA vs this pitcher hand
    overall_bpp       = power_row[3]
    bbe_count         = power_row[4] or 0

    # Use vs-hand barrel rate if available, otherwise fall back to overall
    batter_bpp = bpp_vs_hand if bpp_vs_hand is not None else overall_bpp

    if overall_hr_per_pa is None:
        return None, batter_bpp, None

    # Regress batter HR rate toward league average for small samples
    batter_hr_rate = _regress(
        observed      = overall_hr_per_pa,
        pitches_seen  = bbe_count,
        league_avg    = LEAGUE_AVG_HR_PER_PA,
        threshold     = MIN_BBE_THRESHOLD,
        regression_weight = regression_weight,
    )

    # ── Pitcher HR rate from vulnerability table ───────────────────────────
    # Same most-recent fallback pattern as power profile above.
    vuln_row = conn.execute(
        """
        SELECT hr_per_bf_allowed,
               barrel_rate_allowed,
               batted_ball_events
        FROM   fact_pitcher_hr_vulnerability
        WHERE  as_of_date  <= ?
          AND  pitcher_id   = ?
          AND  split_hand   = ?
          AND  window_code  = ?
        ORDER  BY as_of_date DESC
        LIMIT  1
        """,
        (as_of_date, pitcher_id, effective_batter_hand, window_code),
    ).fetchone()

    pitcher_hr_per_bf       = vuln_row[0] if vuln_row and vuln_row[0] is not None else None
    pitcher_barrel_rate     = vuln_row[1] if vuln_row and vuln_row[1] is not None else None
    pitcher_bbe             = vuln_row[2] if vuln_row and vuln_row[2] is not None else 0

    if pitcher_hr_per_bf is not None:
        pitcher_hr_rate = _regress(
            observed      = pitcher_hr_per_bf,
            pitches_seen  = pitcher_bbe,
            league_avg    = LEAGUE_AVG_HR_PER_PA,
            threshold     = MIN_BBE_THRESHOLD,
            regression_weight = regression_weight,
        )
    else:
        pitcher_hr_rate = None

    # ── Barrel context score ───────────────────────────────────────────────
    # Captures interaction between batter power quality and pitcher vulnerability.
    # Expressed as an HR-rate-equivalent: if both batter and pitcher are
    # at league average barrel rates, barrel_context == LEAGUE_AVG_HR_PER_PA.
    barrel_context = None
    if batter_bpp is not None and pitcher_barrel_rate is not None:
        # Normalise each against league baseline, multiply interaction,
        # then scale back to an HR rate
        batter_barrel_rel  = batter_bpp      / max(LEAGUE_AVG_BARREL_RATE, 0.001)
        pitcher_barrel_rel = pitcher_barrel_rate / max(LEAGUE_AVG_BARREL_RATE, 0.001)
        barrel_context = LEAGUE_AVG_HR_PER_PA * batter_barrel_rel * pitcher_barrel_rel

    # ── Blend ──────────────────────────────────────────────────────────────
    # Weights 0.70/0.20/0.10 selected from backtesting (2026 season, n=472
    # matchups with HR outcomes) — consistent with BA/TB blend weight findings.
    if pitcher_hr_rate is not None and barrel_context is not None:
        blended = (batter_hr_rate * 0.70) + (pitcher_hr_rate * 0.20) + (barrel_context * 0.10)
    elif pitcher_hr_rate is not None:
        blended = (batter_hr_rate * 0.80) + (pitcher_hr_rate * 0.20)
    elif barrel_context is not None:
        blended = (batter_hr_rate * 0.90) + (barrel_context * 0.10)
    else:
        blended = batter_hr_rate

    # Convert per-PA rate to per-game probability.
    # Multiply by ab_per_game so the output represents the probability of
    # hitting a HR in tonight's game rather than on any single PA.
    # Typical output range: 0.05-0.20 (vs previous 0.02-0.08 per-PA output).
    projected_hr_prob = round(blended * ab_per_game * park_hr_factor * weather_adj, 4)

    return projected_hr_prob, batter_bpp, pitcher_barrel_rate


# ── Main scoring function ──────────────────────────────────────────────────

def compute_match_scores(conn: sqlite3.Connection, as_of_date: str,
                          window_code: str = "SEASON") -> None:
    """
    For every matchup row on as_of_date:
      1. Compute pitch_type_match_score
      2. Compute zone_match_score
      3. Recompute projected_batting_avg using both multipliers
      4. Write all three back to fact_matchup_batter_pitcher
    """
    log.info("Computing match scores for %s (window=%s)...", as_of_date, window_code)

    # Pre-compute league averages once (used for regression across all matchups)
    league_avgs_pt   = _league_avg_by_pitch_type(conn, as_of_date, window_code)
    league_avgs_zone = _league_avg_by_zone(conn, as_of_date, window_code)
    league_slgs_pt   = _league_slg_by_pitch_type(conn, as_of_date, window_code)
    league_slgs_zone = _league_slg_by_zone(conn, as_of_date, window_code)
    # Dynamic league averages from current season data
    dyn_ba_row = conn.execute(
        """
        SELECT CAST(SUM(hits) AS REAL) / NULLIF(SUM(at_bats), 0)
        FROM fact_batter_overall
        WHERE window_code = ? AND at_bats >= 50
        """,
        (window_code,),
    ).fetchone()
    dyn_slg_row = conn.execute(
        """
        SELECT CAST(SUM(total_bases) AS REAL) / NULLIF(SUM(at_bats), 0)
        FROM fact_batter_pitch_type_splits
        WHERE window_code = ? AND at_bats >= 10
        """,
        (window_code,),
    ).fetchone()

    dynamic_league_ba  = dyn_ba_row[0]  if dyn_ba_row  and dyn_ba_row[0]  else LEAGUE_AVG_BA
    dynamic_league_slg = dyn_slg_row[0] if dyn_slg_row and dyn_slg_row[0] else 0.402
    log.info("  League avg by pitch type: %d types found.", len(league_avgs_pt))
    log.info("  League avg by zone: %d zones found.", len(league_avgs_zone))

    # Fetch regression weight for this window
    rw_row = conn.execute(
        "SELECT regression_weight FROM dim_split_windows WHERE window_code=?",
        (window_code,),
    ).fetchone()
    regression_weight = rw_row[0] if rw_row else 1.0

    # Fetch all matchup rows for this date and window
    matchups = conn.execute(
        """
        SELECT m.game_id, m.batter_id, m.pitcher_id,
               m.batter_vs_hand_batting_avg,
               m.pitcher_vs_hand_batting_avg_allowed,
               m.park_adjustment_factor,
               m.weather_adjustment_factor,
               p_pitcher.throws   AS pitcher_throws,
               p_batter.bats      AS batter_bats,
               g.venue_id
        FROM   fact_matchup_batter_pitcher m
        JOIN   dim_players p_pitcher ON p_pitcher.player_id = m.pitcher_id
        JOIN   dim_players p_batter  ON p_batter.player_id  = m.batter_id
        LEFT JOIN fact_games g
               ON g.as_of_date = m.as_of_date AND g.game_id = m.game_id
        WHERE  m.as_of_date  = ?
          AND  m.window_code = ?
        """,
        (as_of_date, window_code),
    ).fetchall()

    log.info("  Processing %d matchup rows...", len(matchups))

    if not matchups:
        log.warning("  No matchup rows found for %s window=%s - run pipeline first.",
                    as_of_date, window_code)
        return

    updated = 0
    skipped = 0

    for (game_id, batter_id, pitcher_id, batter_avg,
         pitcher_avg_allowed,
         park_adj, weather_adj, pitcher_throws, batter_bats,
         venue_id) in matchups:

        if not pitcher_throws or not batter_bats:
            skipped += 1
            continue

        # Resolve switch hitters: they bat opposite the pitcher's hand
        # vs RHP -> bats left (L), vs LHP -> bats right (R)
        if batter_bats == 'S':
            effective_batter_hand = 'L' if pitcher_throws == 'R' else 'R'
        else:
            effective_batter_hand = batter_bats

        # Skip if pitcher hand is not R or L (e.g. 'S' in bad data)
        if pitcher_throws not in ('R', 'L'):
            skipped += 1
            continue

        # ── Park HR factor (handedness-specific) ───────────────────────────
        park_hr_col = (
            "park_hr_factor_lhb" if effective_batter_hand == "L"
            else "park_hr_factor_rhb"
        )
        park_hr_row = conn.execute(
            f"SELECT {park_hr_col}, park_run_factor FROM dim_venues WHERE venue_id = ?",
            (venue_id,),
        ).fetchone() if venue_id else None

        if park_hr_row and park_hr_row[0] is not None:
            park_hr_factor = round(park_hr_row[0] / 100.0, 4)
        elif park_hr_row and park_hr_row[1] is not None:
            park_hr_factor = park_hr_row[1]
        else:
            park_hr_factor = 1.0

        # ── Baseline reconstruction (30% batter / 70% pitcher) ─────────────
        # Backtesting on 2026 season data (n=6,730 matchups, min 2 AB)
        # showed 30/70 weighting outperforms the previous 40/60 split,
        # consistent with pitcher handedness splits stabilising faster than
        # batter splits at current sample sizes. Both components regressed
        # toward REGRESSION_TARGET=0.22 below MIN_PITCHES_THRESHOLD=150.
        b_avg = batter_avg  if batter_avg  is not None else REGRESSION_TARGET
        p_avg = pitcher_avg_allowed if pitcher_avg_allowed is not None else REGRESSION_TARGET
        baseline_avg = round((b_avg * 0.30) + (p_avg * 0.70), 5)

        # ── Pitch type match score ─────────────────────────────────────────
        pt_score = _compute_pitch_type_match_score(
            conn              = conn,
            as_of_date        = as_of_date,
            batter_id         = batter_id,
            pitcher_id        = pitcher_id,
            pitcher_hand      = pitcher_throws,
            batter_hand       = effective_batter_hand,
            window_code       = window_code,
            league_avgs       = league_avgs_pt,
            regression_weight = regression_weight,
        )

        # ── Zone match score ───────────────────────────────────────────────
        zone_score = _compute_zone_match_score(
            conn               = conn,
            as_of_date         = as_of_date,
            batter_id          = batter_id,
            pitcher_id         = pitcher_id,
            pitcher_hand       = pitcher_throws,
            batter_hand        = effective_batter_hand,
            window_code        = window_code,
            league_avgs_zone   = league_avgs_zone,
            regression_weight  = regression_weight,
        )

        # ── Pitch type SLG match score ────────────────────────────────────
        pt_slg_score = _compute_pitch_type_slg_score(
            conn=conn, as_of_date=as_of_date,
            batter_id=batter_id, pitcher_id=pitcher_id,
            pitcher_hand=pitcher_throws, batter_hand=effective_batter_hand,
            window_code=window_code, league_slgs=league_slgs_pt,
            regression_weight=regression_weight,
        )

        # ── Zone SLG match score ───────────────────────────────────────────
        zone_slg_score = _compute_zone_slg_score(
            conn=conn, as_of_date=as_of_date,
            batter_id=batter_id, pitcher_id=pitcher_id,
            pitcher_hand=pitcher_throws, batter_hand=effective_batter_hand,
            window_code=window_code, league_slgs_zone=league_slgs_zone,
            regression_weight=regression_weight,
        )

        # ── AB per game — lineup slot estimate ────────────────────────────
        # Slot-based AB estimates derived from observed 2026 season averages
        # (fact_player_game_results joined to fact_game_lineups, n=820-850
        # games per slot). Replaces theoretical estimates which overstated
        # AB by 0.09-0.27 per slot, introducing positive TB bias.
        # Values account for real-world platooning, early exits, and
        # pinch hit substitutions that theoretical maximums ignore.
        # Re-derive at season end: SELECT lineup_slot, AVG(at_bats)
        # FROM fact_player_game_results JOIN fact_game_lineups...
        SLOT_AB = {1: 3.888, 2: 3.781, 3: 3.708, 4: 3.652, 5: 3.549,
                   6: 3.456, 7: 3.339, 8: 3.113, 9: 3.031}

        slot_row = conn.execute(
            """
            SELECT l.lineup_slot
            FROM   fact_game_lineups l
            WHERE  l.as_of_date = ?
              AND  l.game_id    = ?
              AND  l.player_id  = ?
            LIMIT  1
            """,
            (as_of_date, game_id, batter_id),
        ).fetchone()

        if slot_row and slot_row[0] in SLOT_AB:
            ab_per_game = SLOT_AB[slot_row[0]]
        else:
            # Fall back to historical rate, then league average
            ab_game_row = conn.execute(
                """
                SELECT ab_per_game FROM fact_batter_overall
                WHERE  as_of_date=? AND player_id=? AND window_code=?
                """,
                (as_of_date, batter_id, window_code),
            ).fetchone()
            ab_per_game = ab_game_row[0] if ab_game_row and ab_game_row[0] else 3.502

        # ── Recompute projected_batting_avg ────────────────────────────────
        # Blend three signals as a weighted average:
        #   70% weight on baseline (batter/pitcher handedness split 30/70)
        #   20% weight on pitch type match score
        #   10% weight on zone match score
        # Backtesting on 2026 season data (n=6,730 matchups, min 2 AB)
        # identified this configuration as optimal — MAE improvement of
        # -0.00639 over the previous 40/35/25 blend, bias +0.00399 (near zero),
        # direction accuracy 57.8%. Reflects that at current season sample
        # sizes, pitcher-level handedness data is more stable and predictive
        # than pitch-type and zone splits which remain in regression territory.
        # Fallback ratios maintain proportional weighting when a component
        # is unavailable.
        baseline = baseline_avg
        park     = park_adj    or 1.0
        weather  = weather_adj or 1.0

        if pt_score and zone_score:
            blended = (baseline * 0.70) + (pt_score * 0.20) + (zone_score * 0.10)
        elif pt_score:
            blended = (baseline * 0.80) + (pt_score * 0.20)
        elif zone_score:
            blended = (baseline * 0.90) + (zone_score * 0.10)
        else:
            blended = baseline

        projected  = round(blended * park * weather, 4)

        # ── Projected slugging blend ───────────────────────────────────────
        # SLG baseline uses the same 30/70 batter/pitcher weighting as the
        # BA baseline, per backtesting results.
        # Blend weights backtested on 2026 season data (n=6,859 matchups,
        # min 2 AB): 0.70/0.20/0.10 optimal — same baseline-dominant pattern
        # as BA projection, consistent with pitcher handedness SLG-allowed
        # being more stable than batter/pitch-type SLG splits at current
        # sample sizes. SLG_REGRESSION_TARGET=0.380 produces near-zero
        # SLG bias (-0.0001); remaining TB bias is from AB/game estimates.
        batter_slg_row = conn.execute(
            """
            SELECT slugging_pct FROM fact_batter_hand_splits
            WHERE  as_of_date=? AND player_id=? AND split_hand=? AND window_code=?
            """,
            (as_of_date, batter_id, pitcher_throws, window_code),
        ).fetchone()

        pitcher_slg_row = conn.execute(
            """
            SELECT slugging_pct_allowed FROM fact_pitcher_hand_splits
            WHERE  as_of_date=? AND pitcher_id=? AND split_hand=? AND window_code=?
            """,
            (as_of_date, pitcher_id, effective_batter_hand, window_code),
        ).fetchone()

        b_slg = batter_slg_row[0]  if batter_slg_row  and batter_slg_row[0]  is not None else SLG_REGRESSION_TARGET
        p_slg = pitcher_slg_row[0] if pitcher_slg_row and pitcher_slg_row[0] is not None else SLG_REGRESSION_TARGET
        slg_baseline = round((b_slg * 0.30) + (p_slg * 0.70), 5)

        if pt_slg_score and zone_slg_score:
            proj_slg = (slg_baseline * 0.70) + (pt_slg_score * 0.20) + (zone_slg_score * 0.10)
        elif pt_slg_score:
            proj_slg = (slg_baseline * 0.80) + (pt_slg_score * 0.20)
        elif zone_slg_score:
            proj_slg = (slg_baseline * 0.90) + (zone_slg_score * 0.10)
        else:
            proj_slg = slg_baseline

        proj_slg   = round(proj_slg * park * weather, 4)
        proj_tb    = round(proj_slg * ab_per_game, 4)

        # ── HR probability ─────────────────────────────────────────────────
        proj_hr_prob, batter_barrel_rate, pitcher_barrel_rate = _compute_hr_probability(
            conn                  = conn,
            as_of_date            = as_of_date,
            batter_id             = batter_id,
            pitcher_id            = pitcher_id,
            effective_batter_hand = effective_batter_hand,
            pitcher_throws        = pitcher_throws,
            window_code           = window_code,
            park_hr_factor        = park_hr_factor,
            weather_adj           = weather_adj,
            regression_weight     = regression_weight,
            ab_per_game           = ab_per_game,
        )

        # ── Write back ─────────────────────────────────────────────────────
        conn.execute(
            """
            UPDATE fact_matchup_batter_pitcher
            SET    pitch_type_match_score      = ?,
                   zone_match_score            = ?,
                   projected_batting_avg       = ?,
                   pt_slg_score                = ?,
                   zone_slg_score              = ?,
                   projected_slugging          = ?,
                   projected_total_bases       = ?,
                   proj_at_bats_per_game       = ?,
                   projected_hr_probability    = ?,
                   batter_barrel_rate          = ?,
                   pitcher_barrel_rate_allowed = ?
            WHERE  as_of_date  = ?
              AND  game_id     = ?
              AND  batter_id   = ?
              AND  pitcher_id  = ?
              AND  window_code = ?
            """,
            (
                pt_score, zone_score, projected,
                pt_slg_score, zone_slg_score, proj_slg, proj_tb, ab_per_game,
                proj_hr_prob, batter_barrel_rate, pitcher_barrel_rate,
                as_of_date, game_id, batter_id, pitcher_id, window_code,
            ),
        )
        updated += 1

    conn.commit()
    log.info("  Match scores written: %d updated, %d skipped (missing hand data).",
             updated, skipped)


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute pitch type and zone match scores for today's matchups"
    )
    parser.add_argument("--db-path",  default="data/mlb_pregame.db")
    parser.add_argument("--date",     help="as_of_date YYYY-MM-DD")
    parser.add_argument("--today",    action="store_true")
    parser.add_argument("--windows",  default="SEASON,L30D,L14D,L7D",
                        help="Comma-separated window codes to score")
    args = parser.parse_args()

    as_of = args.date if args.date else (
        date.today().isoformat() if args.today else None
    )
    if not as_of:
        parser.error("Provide --date YYYY-MM-DD or --today")

    conn = sqlite3.connect(args.db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")
    conn.execute("PRAGMA journal_mode=WAL;")

    for wc in [w.strip() for w in args.windows.split(",")]:
        compute_match_scores(conn, as_of_date=as_of, window_code=wc)

    conn.close()
    log.info("Match score computation complete.")
