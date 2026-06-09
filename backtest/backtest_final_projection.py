"""
backtest_final_projection.py
-----------------------------
Grid search over regression threshold (tau) and regression target
for the Final Projection (projected_batting_avg) model.

Tests all combinations of:
    tau    in [10, 20, 50, 100]
    target in ['pitch_type_specific', 'dynamic_league', 0.220]

For each configuration, replays the pitch-type and zone match score
computation against stored split data, reblends with stored baseline,
and measures accuracy against actual outcomes in fact_player_game_results.

READ-ONLY — no database writes. Safe to run while scheduler is active.

Metrics reported per configuration:
    MAE          — mean absolute error (lower is better)
    bias         — mean signed error, + = over-projecting (closer to 0 is better)
    dir_acc      — direction accuracy vs baseline (higher is better)
    mae_improve  — MAE improvement over current live model
    n            — number of matchup rows evaluated

Usage:
    python backtest_final_projection.py --db-path data/mlb_pregame.db
    python backtest_final_projection.py --db-path data/mlb_pregame.db --window SEASON
    python backtest_final_projection.py --db-path data/mlb_pregame.db --min-ab 1
    python backtest_final_projection.py --db-path data/mlb_pregame.db --verbose
"""

import sqlite3
import argparse
import math
from datetime import date
from collections import defaultdict

# ── Configuration ──────────────────────────────────────────────────────────

TAU_CANDIDATES = [300, 500, 750]

# Regression target options:
#   'pt_specific'   — use per-pitch-type league average (already in DB)
#   'dynamic'       — single dynamic league BA computed from fact_batter_overall
#   float           — fixed fallback value (e.g. 0.220)
TARGET_CANDIDATES = [0.220]

# Blend weights — fixed for this test (tested separately)
W_BASELINE = 0.40
W_PT       = 0.35
W_ZONE     = 0.25

# Minimum AB in the actual game to include in evaluation
# 1 = include all plate appearances including 0-for-1
# 2 = exclude very short games / early exits
DEFAULT_MIN_AB = 1

WINDOW_CODE = "SEASON"

# ── Regression helper (mirrors compute_match_scores._regress exactly) ──────

def _regress(observed, pitches_seen, league_avg, tau, regression_weight=1.0):
    if observed is None:
        return league_avg
    if pitches_seen >= tau:
        return observed
    sample_weight = regression_weight * (pitches_seen / tau)
    return (observed * sample_weight) + (league_avg * (1 - sample_weight))


# ── Data loading ───────────────────────────────────────────────────────────

def _load_league_avgs_pt(conn, window_code):
    """Per-pitch-type league BA from fact_batter_pitch_type_splits."""
    rows = conn.execute(
        """
        SELECT pitch_type_code,
               CAST(SUM(hits) AS REAL) / NULLIF(SUM(at_bats), 0) AS lg_ba
        FROM   fact_batter_pitch_type_splits
        WHERE  window_code = ?
          AND  at_bats     > 0
        GROUP  BY pitch_type_code
        """,
        (window_code,),
    ).fetchall()
    return {r[0]: r[1] for r in rows if r[1] is not None}


def _load_league_avgs_zone(conn, window_code):
    """Per-zone league BA from fact_batter_zone_splits."""
    rows = conn.execute(
        """
        SELECT zone_code,
               CAST(SUM(hits) AS REAL) / NULLIF(SUM(in_play_events), 0) AS lg_ba
        FROM   fact_batter_zone_splits
        WHERE  window_code = ?
          AND  in_play_events > 0
        GROUP  BY zone_code
        """,
        (window_code,),
    ).fetchall()
    return {r[0]: r[1] for r in rows if r[1] is not None}


def _load_dynamic_league_ba(conn, window_code):
    """Single dynamic league BA from fact_batter_overall."""
    row = conn.execute(
        """
        SELECT CAST(SUM(hits) AS REAL) / NULLIF(SUM(at_bats), 0)
        FROM   fact_batter_overall
        WHERE  window_code = ? AND at_bats >= 50
        """,
        (window_code,),
    ).fetchone()
    return row[0] if row and row[0] else 0.243


def _load_pitch_mix(conn, window_code):
    """
    Preload all pitcher pitch mixes into memory.
    Returns {(as_of_date, pitcher_id, batter_hand): [(pitch_type_code, usage_pct, pitches_thrown)]}
    """
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
    for as_of_date, pid, hand, ptc, usage, thrown in rows:
        out[(as_of_date, pid, hand)].append((ptc, usage, thrown))
    return dict(out)


def _load_zone_profile(conn, window_code):
    """
    Preload all pitcher zone profiles into memory.
    Returns {(as_of_date, pitcher_id, batter_hand): [(zone_code, pitches_thrown)]}
    """
    rows = conn.execute(
        """
        SELECT as_of_date, pitcher_id, split_hand,
               zone_code, SUM(pitches_thrown) AS total_pitches
        FROM   fact_pitcher_zone_profile
        WHERE  window_code    = ?
          AND  pitches_thrown > 0
        GROUP  BY as_of_date, pitcher_id, split_hand, zone_code
        """,
        (window_code,),
    ).fetchall()
    out = defaultdict(list)
    for as_of_date, pid, hand, zc, thrown in rows:
        out[(as_of_date, pid, hand)].append((zc, thrown))
    return dict(out)


def _load_batter_pt_splits(conn, window_code):
    """
    Preload batter pitch type splits into memory.
    Returns {(as_of_date, player_id, pitcher_hand, pitch_type_code): (batting_avg, pitches_seen)}
    """
    rows = conn.execute(
        """
        SELECT as_of_date, player_id, split_hand,
               pitch_type_code, batting_avg, pitches_seen
        FROM   fact_batter_pitch_type_splits
        WHERE  window_code = ?
        """,
        (window_code,),
    ).fetchall()
    return {
        (r[0], r[1], r[2], r[3]): (r[4], r[5] or 0)
        for r in rows
    }


def _load_batter_zone_splits(conn, window_code):
    """
    Preload batter zone splits into memory.
    Returns {(as_of_date, player_id, pitcher_hand, zone_code): (batting_avg, pitches_seen)}
    """
    rows = conn.execute(
        """
        SELECT as_of_date, player_id, split_hand,
               zone_code, batting_avg, pitches_seen
        FROM   fact_batter_zone_splits
        WHERE  window_code = ?
        """,
        (window_code,),
    ).fetchall()
    return {
        (r[0], r[1], r[2], r[3]): (r[4], r[5] or 0)
        for r in rows
    }


def _load_matchups(conn, window_code, min_ab):
    """
    Load all matchup rows that have both a stored projection and an actual outcome.
    Returns list of dicts with all fields needed for replay.
    """
    rows = conn.execute(
        """
        SELECT
            m.as_of_date,
            m.game_id,
            m.batter_id,
            m.pitcher_id,
            m.batter_vs_hand_batting_avg    AS baseline,
            m.projected_batting_avg         AS current_projection,
            m.park_adjustment_factor        AS park_adj,
            m.weather_adjustment_factor     AS weather_adj,
            p_pitcher.throws                AS pitcher_throws,
            p_batter.bats                   AS batter_bats,
            r.at_bats,
            r.hits,
            r.batting_avg                   AS actual_ba
        FROM   fact_matchup_batter_pitcher m
        JOIN   dim_players p_pitcher ON p_pitcher.player_id = m.pitcher_id
        JOIN   dim_players p_batter  ON p_batter.player_id  = m.batter_id
        JOIN   fact_player_game_results r
               ON  r.game_date = m.as_of_date
               AND r.player_id = m.batter_id
        WHERE  m.window_code            = ?
          AND  r.at_bats               >= ?
          AND  m.projected_batting_avg  IS NOT NULL
          AND  m.batter_vs_hand_batting_avg IS NOT NULL
        ORDER  BY m.as_of_date, m.batter_id
        """,
        (window_code, min_ab),
    ).fetchall()

    return [
        {
            "as_of_date":         r[0],
            "game_id":            r[1],
            "batter_id":          r[2],
            "pitcher_id":         r[3],
            "baseline":           r[4],
            "current_projection": r[5],
            "park_adj":           r[6] or 1.0,
            "weather_adj":        r[7] or 1.0,
            "pitcher_throws":     r[8],
            "batter_bats":        r[9],
            "at_bats":            r[10],
            "hits":               r[11],
            "actual_ba":          r[12],
        }
        for r in rows
        if r[8] in ("R", "L") and r[9] in ("R", "L", "S")
    ]


# ── Score replay ───────────────────────────────────────────────────────────

def _replay_pt_score(matchup, pitch_mix_cache, batter_pt_cache,
                     league_avgs_pt, dynamic_league_ba,
                     tau, target_mode):
    """Recompute pitch type match score with given tau and target."""
    key = (matchup["as_of_date"], matchup["pitcher_id"],
           matchup["effective_batter_hand"])
    mix = pitch_mix_cache.get(key, [])
    if not mix:
        return None

    total_usage = sum(u for _, u, _ in mix)
    if total_usage <= 0:
        return None

    weighted = 0.0
    coverage = 0.0

    for ptc, raw_usage, _ in mix:
        usage = raw_usage / total_usage

        split_key = (matchup["as_of_date"], matchup["batter_id"],
                     matchup["pitcher_throws"], ptc)
        split = batter_pt_cache.get(split_key)
        observed     = split[0] if split else None
        pitches_seen = split[1] if split else 0

        # Regression target
        if target_mode == "pt_specific":
            lg = league_avgs_pt.get(ptc, dynamic_league_ba)
        elif target_mode == "dynamic":
            lg = dynamic_league_ba
        else:
            lg = float(target_mode)   # fixed value e.g. 0.220

        regressed = _regress(observed, pitches_seen, lg, tau)
        weighted += usage * regressed
        coverage += usage

    if coverage < 0.25:
        return None
    return weighted


def _replay_zone_score(matchup, zone_profile_cache, batter_zone_cache,
                       league_avgs_zone, dynamic_league_ba,
                       tau, target_mode):
    """Recompute zone match score with given tau and target."""
    key = (matchup["as_of_date"], matchup["pitcher_id"],
           matchup["effective_batter_hand"])
    profile = zone_profile_cache.get(key, [])
    if not profile:
        return None

    total_pitches = sum(p for _, p in profile)
    if total_pitches <= 0:
        return None

    weighted = 0.0
    coverage = 0.0

    for zc, zone_pitches in profile:
        zone_usage = zone_pitches / total_pitches

        split_key = (matchup["as_of_date"], matchup["batter_id"],
                     matchup["pitcher_throws"], zc)
        split = batter_zone_cache.get(split_key)
        observed     = split[0] if split else None
        pitches_seen = split[1] if split else 0

        if target_mode == "pt_specific":
            # zone splits don't have pitch-type grouping so use zone-level lg avg
            lg = league_avgs_zone.get(zc, dynamic_league_ba)
        elif target_mode == "dynamic":
            lg = dynamic_league_ba
        else:
            lg = float(target_mode)

        regressed = _regress(observed, pitches_seen, lg, tau)
        weighted += zone_usage * regressed
        coverage += zone_usage

    if coverage < 0.25:
        return None
    return weighted


# ── Metrics ────────────────────────────────────────────────────────────────

def _compute_metrics(matchups, projections, label=""):
    """
    Given a list of matchup dicts and corresponding projected BA values,
    compute MAE, bias, and direction accuracy vs baseline.
    """
    errors      = []
    signed      = []
    dir_correct = 0
    dir_total   = 0

    for m, proj in zip(matchups, projections):
        if proj is None or m["actual_ba"] is None:
            continue
        actual   = m["actual_ba"]
        baseline = m["baseline"]

        err = abs(proj - actual)
        errors.append(err)
        signed.append(proj - actual)

        # Direction: does projection correctly call above/below baseline?
        if baseline is not None:
            actual_above   = actual > baseline
            proj_above     = proj   > baseline
            if actual_above == proj_above:
                dir_correct += 1
            dir_total += 1

    n   = len(errors)
    mae = round(sum(errors) / n, 5) if n > 0 else None
    bias = round(sum(signed) / n, 5) if n > 0 else None
    dir_acc = round(dir_correct / dir_total, 4) if dir_total > 0 else None

    return {"n": n, "mae": mae, "bias": bias, "dir_acc": dir_acc}


# ── Main ───────────────────────────────────────────────────────────────────

def run(db_path: str, window_code: str = "SEASON",
        min_ab: int = DEFAULT_MIN_AB, verbose: bool = False) -> None:

    print(f"\nBacktest: Final Projection — Tau × Target Grid Search")
    print(f"DB: {db_path} | Window: {window_code} | Min AB: {min_ab}")
    print("Loading data into memory...")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")

    # Load all reference data once — avoids per-row DB queries
    league_avgs_pt   = _load_league_avgs_pt(conn, window_code)
    league_avgs_zone = _load_league_avgs_zone(conn, window_code)
    dynamic_league_ba = _load_dynamic_league_ba(conn, window_code)
    pitch_mix_cache  = _load_pitch_mix(conn, window_code)
    zone_profile_cache = _load_zone_profile(conn, window_code)
    batter_pt_cache  = _load_batter_pt_splits(conn, window_code)
    batter_zone_cache = _load_batter_zone_splits(conn, window_code)
    matchups          = _load_matchups(conn, window_code, min_ab)
    conn.close()

    print(f"Matchup rows loaded: {len(matchups)}")
    print(f"Dynamic league BA: {dynamic_league_ba:.4f}")
    print(f"Pitch type league avgs: {len(league_avgs_pt)} types")
    print(f"Zone league avgs: {len(league_avgs_zone)} zones")

    if not matchups:
        print("\nNo matchup rows found — check that boxscore ingestion completed.")
        return

    # Resolve effective batter hand for each matchup once
    for m in matchups:
        if m["batter_bats"] == "S":
            m["effective_batter_hand"] = "L" if m["pitcher_throws"] == "R" else "R"
        else:
            m["effective_batter_hand"] = m["batter_bats"]

    # ── Baseline: current live model ───────────────────────────────────────
    current_projs = [m["current_projection"] for m in matchups]
    current_metrics = _compute_metrics(matchups, current_projs, "current")

    print(f"\n{'='*72}")
    print(f"CURRENT LIVE MODEL (tau=20, pt_specific targets)")
    print(f"  n={current_metrics['n']:,}  "
          f"MAE={current_metrics['mae']:.5f}  "
          f"bias={current_metrics['bias']:+.5f}  "
          f"dir_acc={current_metrics['dir_acc']:.4f}")
    print(f"{'='*72}")

    # ── Grid search ────────────────────────────────────────────────────────
    results = []

    total_configs = len(TAU_CANDIDATES) * len(TARGET_CANDIDATES)
    config_num    = 0

    for tau in TAU_CANDIDATES:
        for target in TARGET_CANDIDATES:
            config_num += 1
            label = f"tau={tau:<4} target={str(target):<15}"

            projections = []
            for m in matchups:
                pt_score = _replay_pt_score(
                    m, pitch_mix_cache, batter_pt_cache,
                    league_avgs_pt, dynamic_league_ba,
                    tau, target,
                )
                zone_score = _replay_zone_score(
                    m, zone_profile_cache, batter_zone_cache,
                    league_avgs_zone, dynamic_league_ba,
                    tau, target,
                )

                baseline = m["baseline"]
                park     = m["park_adj"]
                weather  = m["weather_adj"]

                if pt_score is not None and zone_score is not None:
                    blended = (baseline * W_BASELINE) + (pt_score * W_PT) + (zone_score * W_ZONE)
                elif pt_score is not None:
                    blended = (baseline * 0.55) + (pt_score * 0.45)
                elif zone_score is not None:
                    blended = (baseline * 0.60) + (zone_score * 0.40)
                else:
                    blended = baseline

                proj = round(blended * park * weather, 5)
                projections.append(proj)

            metrics = _compute_metrics(matchups, projections, label)
            mae_delta = round(metrics["mae"] - current_metrics["mae"], 5) if metrics["mae"] else None

            results.append({
                "tau":       tau,
                "target":    str(target),
                "label":     label,
                "metrics":   metrics,
                "mae_delta": mae_delta,
            })

            if verbose:
                delta_str = f"{mae_delta:+.5f}" if mae_delta is not None else "N/A"
                print(f"  [{config_num:2}/{total_configs}] {label} | "
                      f"MAE={metrics['mae']:.5f} ({delta_str})  "
                      f"bias={metrics['bias']:+.5f}  "
                      f"dir={metrics['dir_acc']:.4f}")

    # ── Results table ──────────────────────────────────────────────────────
    results.sort(key=lambda r: r["metrics"]["mae"] or 9999)

    print(f"\n{'─'*80}")
    print(f"{'TAU':<6} {'TARGET':<18} {'MAE':>8} {'vs LIVE':>9} {'BIAS':>9} {'DIR ACC':>9} {'N':>7}")
    print(f"{'─'*80}")

    for r in results:
        m         = r["metrics"]
        delta_str = f"{r['mae_delta']:+.5f}" if r["mae_delta"] is not None else "  N/A  "
        marker    = " ◄ BEST" if r == results[0] else ""
        is_current = r["tau"] == 20 and r["target"] == "pt_specific"
        tag        = " (live)" if is_current else ""
        print(
            f"{r['tau']:<6} "
            f"{r['target']:<18} "
            f"{m['mae']:>8.5f} "
            f"{delta_str:>9} "
            f"{m['bias']:>+9.5f} "
            f"{m['dir_acc']:>9.4f} "
            f"{m['n']:>7,}"
            f"{marker}{tag}"
        )

    print(f"{'─'*80}")

    # ── Summary findings ───────────────────────────────────────────────────
    best = results[0]
    print(f"\nBest configuration: tau={best['tau']}, target={best['target']}")
    print(f"  MAE improvement over live model: {best['mae_delta']:+.5f}")
    if abs(best["mae_delta"]) < 0.00050:
        print("  NOTE: Improvement is very small (<0.0005). "
              "Current tau=20 / pt_specific is likely near-optimal.")
    elif best["mae_delta"] < 0:
        print(f"  Recommend updating MIN_PITCHES_THRESHOLD to {best['tau']} "
              f"and regression target to '{best['target']}'.")

    # Tau sensitivity summary
    print(f"\nTau sensitivity (averaged across all targets):")
    for tau in TAU_CANDIDATES:
        tau_results  = [r for r in results if r["tau"] == tau]
        avg_mae      = sum(r["metrics"]["mae"] for r in tau_results) / len(tau_results)
        avg_delta    = sum(r["mae_delta"] for r in tau_results) / len(tau_results)
        print(f"  tau={tau:<5} avg MAE={avg_mae:.5f}  avg delta vs live={avg_delta:+.5f}")

    print(f"\nTarget sensitivity (averaged across all tau values):")
    for target in TARGET_CANDIDATES:
        t_results = [r for r in results if r["target"] == str(target)]
        avg_mae   = sum(r["metrics"]["mae"] for r in t_results) / len(t_results)
        avg_delta = sum(r["mae_delta"] for r in t_results) / len(t_results)
        print(f"  target={str(target):<15} avg MAE={avg_mae:.5f}  avg delta vs live={avg_delta:+.5f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Grid search tau and regression target for Final Projection"
    )
    parser.add_argument("--db-path", default="data/mlb_pregame.db")
    parser.add_argument("--window",  default=WINDOW_CODE,
                        help="Split window code (default: SEASON)")
    parser.add_argument("--min-ab",  type=int, default=DEFAULT_MIN_AB,
                        help="Minimum at-bats in actual game (default: 1)")
    parser.add_argument("--verbose", action="store_true",
                        help="Print each config result as it runs")
    args = parser.parse_args()

    run(
        db_path     = args.db_path,
        window_code = args.window,
        min_ab      = args.min_ab,
        verbose     = args.verbose,
    )
