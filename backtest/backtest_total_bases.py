"""
backtest_total_bases.py
------------------------
Grid search over blend weights and SLG regression target for the
projected_total_bases model.

Tests all combinations of:
    slg_target in ['pt_specific', 'dynamic', 0.350, 0.380, 0.400]
    blend      in [(0.70, 0.20, 0.10), (0.60, 0.25, 0.15), ...]

Outcome variable: actual total_bases from fact_player_game_results.
Projection: projected_slg × proj_at_bats_per_game.

Because total_bases = slg × ab, two sources of error exist:
  1. SLG projection error
  2. AB/game estimate error (always present — uses historical avg)
This script measures the combined error. AB/game is held fixed at
the stored proj_at_bats_per_game value for all configurations so
the blend weight and SLG target tests are isolated.

READ-ONLY — no database writes. Safe to run while scheduler is active.

Metrics reported:
    MAE_TB    — mean absolute error on total bases (primary)
    bias_TB   — mean signed error on total bases
    MAE_SLG   — mean absolute error on slugging (secondary diagnostic)
    bias_SLG  — mean signed error on slugging
    n         — matchup rows with actual outcomes and AB >= min_ab

Usage:
    python backtest/backtest_total_bases.py --db-path data/mlb_pregame.db
    python backtest/backtest_total_bases.py --db-path data/mlb_pregame.db --min-ab 2 --verbose
"""

import sqlite3
import argparse
from collections import defaultdict

# ── Configuration ──────────────────────────────────────────────────────────

# SLG regression target candidates
# 'pt_specific' = per-pitch-type league SLG from fact_batter_pitch_type_splits
# 'dynamic'     = single dynamic league SLG from fact_batter_overall
# float         = fixed fallback value
SLG_TARGET_CANDIDATES = [
    "pt_specific",
    "dynamic",
    0.350,   # current live value (conservative, below league avg)
    0.380,   # moderate — between conservative and league avg
    0.402,   # full league average SLG (original hardcoded value)
]

# Blend weight candidates for SLG — (W_BASELINE, W_PT, W_ZONE)
# Starting from BA-optimized weights and exploring the same frontier
BLEND_CANDIDATES = [
    (0.40, 0.35, 0.25),   # ← current live (pre-backtest placeholder)
    (0.50, 0.30, 0.20),
    (0.55, 0.30, 0.15),
    (0.60, 0.25, 0.15),
    (0.65, 0.20, 0.15),
    (0.70, 0.20, 0.10),   # BA-optimized weights — may or may not transfer to SLG
    (0.70, 0.15, 0.15),
    (0.75, 0.15, 0.10),
]

# Batter/pitcher baseline split — fixed at backtested optimum
BATTER_WEIGHT  = 0.30
PITCHER_WEIGHT = 0.70

# Regression tau — fixed at backtested optimum
TAU = 150

# Current live blend for comparison tagging
LIVE_BLEND      = (0.40, 0.35, 0.25)
LIVE_SLG_TARGET = 0.350   # current live SLG_REGRESSION_TARGET

WINDOW_CODE    = "SEASON"
DEFAULT_MIN_AB = 2


# ── Regression helper ──────────────────────────────────────────────────────

def _regress(observed, pitches_seen, league_avg, tau):
    if observed is None:
        return league_avg
    if pitches_seen >= tau:
        return observed
    w = pitches_seen / tau
    return (observed * w) + (league_avg * (1 - w))


# ── Data loading ───────────────────────────────────────────────────────────

def _load_league_slg_by_pt(conn, window_code):
    """Per-pitch-type league SLG — pt_specific regression target."""
    rows = conn.execute(
        """
        SELECT pitch_type_code,
               CAST(SUM(total_bases) AS REAL) / NULLIF(SUM(at_bats), 0) AS lg_slg
        FROM   fact_batter_pitch_type_splits
        WHERE  window_code = ?
          AND  at_bats     > 0
        GROUP  BY pitch_type_code
        """,
        (window_code,),
    ).fetchall()
    return {r[0]: r[1] for r in rows if r[1] is not None}


def _load_league_slg_by_zone(conn, window_code):
    """Per-zone league SLG — pt_specific regression target for zones."""
    rows = conn.execute(
        """
        SELECT zone_code,
               CAST(SUM(total_bases) AS REAL) / NULLIF(SUM(in_play_events), 0) AS lg_slg
        FROM   fact_batter_zone_splits
        WHERE  window_code    = ?
          AND  in_play_events > 0
        GROUP  BY zone_code
        """,
        (window_code,),
    ).fetchall()
    return {r[0]: r[1] for r in rows if r[1] is not None}


def _load_dynamic_league_slg(conn, window_code):
    """Single dynamic league SLG from fact_batter_pitch_type_splits."""
    row = conn.execute(
        """
        SELECT CAST(SUM(total_bases) AS REAL) / NULLIF(SUM(at_bats), 0)
        FROM   fact_batter_pitch_type_splits
        WHERE  window_code = ? AND at_bats >= 10
        """,
        (window_code,),
    ).fetchone()
    return row[0] if row and row[0] else 0.402


def _load_batter_hand_slg(conn, window_code):
    """Batter SLG vs pitcher hand. Used for baseline reconstruction."""
    rows = conn.execute(
        """
        SELECT as_of_date, player_id, split_hand, slugging_pct, at_bats
        FROM   fact_batter_hand_splits
        WHERE  window_code    = ?
          AND  slugging_pct   IS NOT NULL
        """,
        (window_code,),
    ).fetchall()
    return {(r[0], r[1], r[2]): (r[3], r[4] or 0) for r in rows}


def _load_pitcher_hand_slg(conn, window_code):
    """Pitcher SLG allowed vs batter hand. Used for baseline reconstruction."""
    rows = conn.execute(
        """
        SELECT as_of_date, pitcher_id, split_hand,
               slugging_pct_allowed, batters_faced
        FROM   fact_pitcher_hand_splits
        WHERE  window_code           = ?
          AND  slugging_pct_allowed  IS NOT NULL
        """,
        (window_code,),
    ).fetchall()
    return {(r[0], r[1], r[2]): (r[3], r[4] or 0) for r in rows}


def _load_batter_pt_slg(conn, window_code):
    """Batter SLG by pitch type. Returns {(date, player_id, p_hand, ptc): (slg, pitches)}"""
    rows = conn.execute(
        """
        SELECT as_of_date, player_id, split_hand,
               pitch_type_code, slugging_pct, pitches_seen
        FROM   fact_batter_pitch_type_splits
        WHERE  window_code = ?
        """,
        (window_code,),
    ).fetchall()
    return {(r[0], r[1], r[2], r[3]): (r[4], r[5] or 0) for r in rows}


def _load_batter_zone_slg(conn, window_code):
    """Batter SLG by zone. Returns {(date, player_id, p_hand, zone): (slg, pitches)}"""
    rows = conn.execute(
        """
        SELECT as_of_date, player_id, split_hand,
               zone_code, slugging_pct, pitches_seen
        FROM   fact_batter_zone_splits
        WHERE  window_code = ?
        """,
        (window_code,),
    ).fetchall()
    return {(r[0], r[1], r[2], r[3]): (r[4], r[5] or 0) for r in rows}


def _load_pitcher_pitch_mix(conn, window_code):
    """Pitcher pitch mix. Returns {(date, pitcher_id, batter_hand): [(ptc, usage, thrown)]}"""
    rows = conn.execute(
        """
        SELECT as_of_date, pitcher_id, split_hand,
               pitch_type_code, usage_pct, pitches_thrown
        FROM   fact_pitcher_pitch_mix
        WHERE  window_code    = ?
          AND  usage_pct      IS NOT NULL
          AND  pitches_thrown > 0
        """,
        (window_code,),
    ).fetchall()
    out = defaultdict(list)
    for r in rows:
        out[(r[0], r[1], r[2])].append((r[3], r[4], r[5]))
    return dict(out)


def _load_pitcher_zone_profile(conn, window_code):
    """Pitcher zone profile. Returns {(date, pitcher_id, batter_hand): [(zone, pitches)]}"""
    rows = conn.execute(
        """
        SELECT as_of_date, pitcher_id, split_hand,
               zone_code, SUM(pitches_thrown) AS total
        FROM   fact_pitcher_zone_profile
        WHERE  window_code    = ?
          AND  pitches_thrown > 0
        GROUP  BY as_of_date, pitcher_id, split_hand, zone_code
        """,
        (window_code,),
    ).fetchall()
    out = defaultdict(list)
    for r in rows:
        out[(r[0], r[1], r[2])].append((r[3], r[4]))
    return dict(out)


def _load_matchups(conn, window_code, min_ab):
    """
    Load matchup rows joined to actual outcomes.

    ab_per_game is now computed live from lineup_slot rather than reading
    the stored proj_at_bats_per_game, so the backtest always reflects the
    current AB/game logic regardless of when matchup rows were written.
    """
    # Slot-based AB estimates — empirically derived from 2026 season data
    # (n=820-850 games per slot). Mirrors compute_match_scores.py exactly.
    SLOT_AB = {1: 3.888, 2: 3.781, 3: 3.708, 4: 3.652, 5: 3.549,
               6: 3.456, 7: 3.339, 8: 3.113, 9: 3.031}

    rows = conn.execute(
        """
        SELECT
            m.as_of_date,
            m.game_id,
            m.batter_id,
            m.pitcher_id,
            m.projected_total_bases        AS current_proj_tb,
            m.projected_slugging           AS current_proj_slg,
            m.proj_at_bats_per_game        AS stored_ab_per_game,
            m.park_adjustment_factor       AS park_adj,
            m.weather_adjustment_factor    AS weather_adj,
            p_pitcher.throws               AS pitcher_throws,
            p_batter.bats                  AS batter_bats,
            r.total_bases                  AS actual_tb,
            r.at_bats                      AS actual_ab,
            CASE WHEN r.at_bats > 0
                 THEN CAST(r.total_bases AS REAL) / r.at_bats
                 ELSE NULL END             AS actual_slg,
            l.lineup_slot
        FROM   fact_matchup_batter_pitcher m
        JOIN   dim_players p_pitcher ON p_pitcher.player_id = m.pitcher_id
        JOIN   dim_players p_batter  ON p_batter.player_id  = m.batter_id
        JOIN   fact_player_game_results r
               ON  r.game_date = m.as_of_date
               AND r.player_id = m.batter_id
        LEFT JOIN fact_game_lineups l
               ON  l.as_of_date = m.as_of_date
               AND l.game_id    = m.game_id
               AND l.player_id  = m.batter_id
        WHERE  m.window_code           = ?
          AND  r.at_bats              >= ?
          AND  m.projected_total_bases IS NOT NULL
        ORDER  BY m.as_of_date, m.batter_id
        """,
        (window_code, min_ab),
    ).fetchall()

    result = []
    for r in rows:
        pitcher_throws = r[9]
        batter_bats    = r[10]
        if pitcher_throws not in ("R", "L") or batter_bats not in ("R", "L", "S"):
            continue
        eff_hand = (
            "L" if pitcher_throws == "R" else "R"
        ) if batter_bats == "S" else batter_bats

        # Compute live AB/game from lineup slot — same logic as compute_match_scores
        lineup_slot     = r[14]
        stored_ab       = r[6]
        if lineup_slot and lineup_slot in SLOT_AB:
            ab_per_game = SLOT_AB[lineup_slot]
        elif stored_ab:
            ab_per_game = stored_ab
        else:
            ab_per_game = 3.6

        result.append({
            "as_of_date":            r[0],
            "game_id":               r[1],
            "batter_id":             r[2],
            "pitcher_id":            r[3],
            "current_proj_tb":       r[4],
            "current_proj_slg":      r[5],
            "ab_per_game":           ab_per_game,
            "park":                  r[7] or 1.0,
            "weather":               r[8] or 1.0,
            "pitcher_throws":        pitcher_throws,
            "effective_batter_hand": eff_hand,
            "actual_tb":             r[11],
            "actual_ab":             r[12],
            "actual_slg":            r[13],
        })
    return result


# ── Score replay ───────────────────────────────────────────────────────────

def _replay_pt_slg_score(matchup, pitch_mix_cache, batter_pt_slg,
                          league_slg_pt, dynamic_slg, slg_target):
    """Replay pt_slg_score with a given SLG regression target."""
    key = (matchup["as_of_date"], matchup["pitcher_id"],
           matchup["effective_batter_hand"])
    mix = pitch_mix_cache.get(key, [])
    if not mix:
        return None

    total_usage = sum(u for _, u, _ in mix)
    if total_usage <= 0:
        return None

    weighted = coverage = 0.0
    for ptc, raw_usage, _ in mix:
        usage = raw_usage / total_usage
        split_key = (matchup["as_of_date"], matchup["batter_id"],
                     matchup["pitcher_throws"], ptc)
        split = batter_pt_slg.get(split_key)
        observed     = split[0] if split else None
        pitches_seen = split[1] if split else 0

        if slg_target == "pt_specific":
            lg = league_slg_pt.get(ptc, dynamic_slg)
        elif slg_target == "dynamic":
            lg = dynamic_slg
        else:
            lg = float(slg_target)

        regressed = _regress(observed, pitches_seen, lg, TAU)
        weighted += usage * regressed
        coverage += usage

    return round(weighted, 4) if coverage >= 0.25 else None


def _replay_zone_slg_score(matchup, zone_profile_cache, batter_zone_slg,
                            league_slg_zone, dynamic_slg, slg_target):
    """Replay zone_slg_score with a given SLG regression target."""
    key = (matchup["as_of_date"], matchup["pitcher_id"],
           matchup["effective_batter_hand"])
    profile = zone_profile_cache.get(key, [])
    if not profile:
        return None

    total_pitches = sum(p for _, p in profile)
    if total_pitches <= 0:
        return None

    weighted = coverage = 0.0
    for zc, zone_pitches in profile:
        zone_usage = zone_pitches / total_pitches
        split_key  = (matchup["as_of_date"], matchup["batter_id"],
                      matchup["pitcher_throws"], zc)
        split = batter_zone_slg.get(split_key)
        observed     = split[0] if split else None
        pitches_seen = split[1] if split else 0

        if slg_target == "pt_specific":
            lg = league_slg_zone.get(zc, dynamic_slg)
        elif slg_target == "dynamic":
            lg = dynamic_slg
        else:
            lg = float(slg_target)

        regressed = _regress(observed, pitches_seen, lg, TAU)
        weighted += zone_usage * regressed
        coverage += zone_usage

    return round(weighted, 4) if coverage >= 0.25 else None


def _build_slg_baseline(matchup, batter_hand_slg, pitcher_hand_slg, slg_target):
    """Reconstruct SLG baseline at 30/70 batter/pitcher weighting."""
    b_key = (matchup["as_of_date"], matchup["batter_id"],
             matchup["pitcher_throws"])
    p_key = (matchup["as_of_date"], matchup["pitcher_id"],
             matchup["effective_batter_hand"])

    b_row = batter_hand_slg.get(b_key)
    p_row = pitcher_hand_slg.get(p_key)

    fallback = float(slg_target) if isinstance(slg_target, float) else 0.350

    b_slg = _regress(b_row[0] if b_row else None,
                     b_row[1] if b_row else 0, fallback, TAU)
    p_slg = _regress(p_row[0] if p_row else None,
                     p_row[1] if p_row else 0, fallback, TAU)

    return round((b_slg * BATTER_WEIGHT) + (p_slg * PITCHER_WEIGHT), 5)


# ── Metrics ────────────────────────────────────────────────────────────────

def _compute_metrics(matchups, proj_tb_list):
    """Compute MAE, bias for TB and SLG."""
    tb_errors = tb_signed = []
    slg_errors = slg_signed = []
    tb_errors, tb_signed, slg_errors, slg_signed = [], [], [], []

    for m, proj_tb in zip(matchups, proj_tb_list):
        if proj_tb is None or m["actual_tb"] is None:
            continue
        tb_errors.append(abs(proj_tb - m["actual_tb"]))
        tb_signed.append(proj_tb - m["actual_tb"])

        if m["actual_slg"] is not None and m["ab_per_game"]:
            proj_slg = proj_tb / m["ab_per_game"]
            slg_errors.append(abs(proj_slg - m["actual_slg"]))
            slg_signed.append(proj_slg - m["actual_slg"])

    n = len(tb_errors)
    def avg(lst): return round(sum(lst) / len(lst), 5) if lst else None

    return {
        "n":        n,
        "mae_tb":   avg(tb_errors),
        "bias_tb":  avg(tb_signed),
        "mae_slg":  avg(slg_errors),
        "bias_slg": avg(slg_signed),
    }


# ── Main ───────────────────────────────────────────────────────────────────

def run(db_path, window_code=WINDOW_CODE, min_ab=DEFAULT_MIN_AB,
        verbose=False):

    print(f"\nBacktest: Projected Total Bases — SLG Target × Blend Weight Grid Search")
    print(f"DB: {db_path} | Window: {window_code} | Min AB: {min_ab}")
    print(f"Fixed: tau={TAU}, batter_w={BATTER_WEIGHT}, pitcher_w={PITCHER_WEIGHT}")
    print("Loading data...")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")

    league_slg_pt    = _load_league_slg_by_pt(conn, window_code)
    league_slg_zone  = _load_league_slg_by_zone(conn, window_code)
    dynamic_slg      = _load_dynamic_league_slg(conn, window_code)
    batter_hand_slg  = _load_batter_hand_slg(conn, window_code)
    pitcher_hand_slg = _load_pitcher_hand_slg(conn, window_code)
    batter_pt_slg    = _load_batter_pt_slg(conn, window_code)
    batter_zone_slg  = _load_batter_zone_slg(conn, window_code)
    pitch_mix_cache  = _load_pitcher_pitch_mix(conn, window_code)
    zone_profile_cache = _load_pitcher_zone_profile(conn, window_code)
    matchups         = _load_matchups(conn, window_code, min_ab)
    conn.close()

    print(f"Matchup rows loaded: {len(matchups)}")
    print(f"Dynamic league SLG: {dynamic_slg:.4f}")
    print(f"Pitch type SLG avgs: {len(league_slg_pt)} types")
    print(f"Zone SLG avgs: {len(league_slg_zone)} zones")

    if not matchups:
        print("No matchup rows found — check that boxscore ingestion completed.")
        return

    # ── Current live model baseline ────────────────────────────────────────
    # Replay using stored proj_slg × live ab_per_game (slot-based) so the
    # "current live model" comparison reflects the actual deployed logic,
    # not stored projected_total_bases which may predate the AB/game fix.
    current_tb = [
        round(m["current_proj_slg"] * m["ab_per_game"], 4)
        if m["current_proj_slg"] is not None else m["current_proj_tb"]
        for m in matchups
    ]
    current_metrics = _compute_metrics(matchups, current_tb)

    print(f"\n{'='*72}")
    print(f"CURRENT LIVE MODEL (blend=0.70/0.20/0.10, slg_target=0.380, slot-based AB)")
    print(f"  n={current_metrics['n']:,}  "
          f"MAE_TB={current_metrics['mae_tb']:.4f}  "
          f"bias_TB={current_metrics['bias_tb']:+.4f}  "
          f"MAE_SLG={current_metrics['mae_slg']:.4f}  "
          f"bias_SLG={current_metrics['bias_slg']:+.4f}")
    print(f"{'='*72}")

    # ── Grid search ────────────────────────────────────────────────────────
    results = []
    total   = len(SLG_TARGET_CANDIDATES) * len(BLEND_CANDIDATES)
    count   = 0

    for slg_target in SLG_TARGET_CANDIDATES:
        for w_base, w_pt, w_zone in BLEND_CANDIDATES:
            count += 1
            is_live = (
                (w_base, w_pt, w_zone) == LIVE_BLEND and
                (isinstance(slg_target, float) and abs(slg_target - LIVE_SLG_TARGET) < 0.001)
            )

            proj_tb_list = []
            for m in matchups:
                slg_baseline = _build_slg_baseline(
                    m, batter_hand_slg, pitcher_hand_slg, slg_target
                )
                pt_slg = _replay_pt_slg_score(
                    m, pitch_mix_cache, batter_pt_slg,
                    league_slg_pt, dynamic_slg, slg_target
                )
                zone_slg = _replay_zone_slg_score(
                    m, zone_profile_cache, batter_zone_slg,
                    league_slg_zone, dynamic_slg, slg_target
                )

                if pt_slg and zone_slg:
                    proj_slg = (slg_baseline * w_base) + (pt_slg * w_pt) + (zone_slg * w_zone)
                elif pt_slg:
                    proj_slg = (slg_baseline * 0.80) + (pt_slg * 0.20)
                elif zone_slg:
                    proj_slg = (slg_baseline * 0.90) + (zone_slg * 0.10)
                else:
                    proj_slg = slg_baseline

                proj_slg = round(proj_slg * m["park"] * m["weather"], 4)
                proj_tb  = round(proj_slg * m["ab_per_game"], 4)
                proj_tb_list.append(proj_tb)

            metrics   = _compute_metrics(matchups, proj_tb_list)
            tb_delta  = round(metrics["mae_tb"] - current_metrics["mae_tb"], 4) \
                        if metrics["mae_tb"] else None

            results.append({
                "slg_target": str(slg_target),
                "w_base":     w_base,
                "w_pt":       w_pt,
                "w_zone":     w_zone,
                "metrics":    metrics,
                "tb_delta":   tb_delta,
                "is_live":    is_live,
            })

            if verbose:
                tag = " (live)" if is_live else ""
                print(f"  [{count:2}/{total}] target={str(slg_target):<12} "
                      f"blend={w_base}/{w_pt}/{w_zone} | "
                      f"MAE_TB={metrics['mae_tb']:.4f} ({tb_delta:+.4f})  "
                      f"bias_TB={metrics['bias_tb']:+.4f}  "
                      f"MAE_SLG={metrics['mae_slg']:.4f}{tag}")

    # ── Results table ──────────────────────────────────────────────────────
    results.sort(key=lambda r: r["metrics"]["mae_tb"] or 9999)

    print(f"\n{'─'*90}")
    print(f"{'SLG_TARGET':<14} {'BASELINE':<10} {'PT':<6} {'ZONE':<6} "
          f"{'MAE_TB':>8} {'vs LIVE':>8} {'BIAS_TB':>8} "
          f"{'MAE_SLG':>8} {'BIAS_SLG':>9} {'N':>6}")
    print(f"{'─'*90}")

    for r in results:
        m    = r["metrics"]
        tag  = " ◄ BEST" if r == results[0] else ""
        live = " (live)" if r["is_live"] else ""
        delta_str = f"{r['tb_delta']:+.4f}" if r["tb_delta"] is not None else "  N/A "
        print(
            f"{r['slg_target']:<14} "
            f"{r['w_base']:<10.2f}"
            f"{r['w_pt']:<6.2f}"
            f"{r['w_zone']:<6.2f}"
            f"{m['mae_tb']:>8.4f} "
            f"{delta_str:>8} "
            f"{m['bias_tb']:>+8.4f} "
            f"{m['mae_slg']:>8.4f} "
            f"{m['bias_slg']:>+9.4f} "
            f"{m['n']:>6,}"
            f"{tag}{live}"
        )

    print(f"{'─'*90}")

    best = results[0]
    print(f"\nBest configuration:")
    print(f"  SLG target: {best['slg_target']}")
    print(f"  Blend: baseline={best['w_base']:.2f} / pt={best['w_pt']:.2f} / zone={best['w_zone']:.2f}")
    print(f"  MAE_TB improvement over live: {best['tb_delta']:+.4f}")

    # Sensitivity summaries
    print(f"\nSLG target sensitivity (averaged across blend configs):")
    for t in SLG_TARGET_CANDIDATES:
        sub = [r for r in results if r["slg_target"] == str(t)]
        if not sub:
            continue
        avg_mae  = sum(r["metrics"]["mae_tb"] for r in sub) / len(sub)
        avg_bias = sum(r["metrics"]["bias_tb"] for r in sub) / len(sub)
        avg_d    = sum(r["tb_delta"] for r in sub if r["tb_delta"]) / len(sub)
        print(f"  target={str(t):<12}  avg MAE_TB={avg_mae:.4f}  "
              f"avg bias={avg_bias:+.4f}  avg delta={avg_d:+.4f}")

    print(f"\nBaseline weight sensitivity (averaged across SLG targets):")
    for w in sorted(set(r["w_base"] for r in results)):
        sub = [r for r in results if r["w_base"] == w]
        avg_mae = sum(r["metrics"]["mae_tb"] for r in sub) / len(sub)
        avg_d   = sum(r["tb_delta"] for r in sub if r["tb_delta"]) / len(sub)
        print(f"  baseline={w:.2f}  avg MAE_TB={avg_mae:.4f}  avg delta={avg_d:+.4f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Grid search SLG regression target and blend weights for projected_total_bases"
    )
    parser.add_argument("--db-path", default="data/mlb_pregame.db")
    parser.add_argument("--window",  default=WINDOW_CODE)
    parser.add_argument("--min-ab",  type=int, default=DEFAULT_MIN_AB)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    run(
        db_path     = args.db_path,
        window_code = args.window,
        min_ab      = args.min_ab,
        verbose     = args.verbose,
    )
