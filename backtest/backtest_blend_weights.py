"""
backtest_blend_weights.py
--------------------------
Grid search over the top-level blend weights for Final Projection:

    final = (baseline * W_BASELINE) + (pt_score * W_PT) + (zone_score * W_ZONE)

Current live values: 0.40 / 0.35 / 0.25

Tests 9 weight combinations where all weights sum to 1.0, covering
a range from baseline-heavy to pt_score-heavy configurations.

IMPORTANT: Run backtest_baseline_split.py first and lock in the optimal
batter/pitcher split weighting before running this script. Update
BATTER_WEIGHT and PITCHER_WEIGHT below if the optimal values differ
from the current live 0.40/0.60.

Uses stored pt_score and zone_score values from fact_matchup_batter_pitcher.
Baseline is reconstructed from raw components using the optimized
batter/pitcher split weighting set below.

READ-ONLY — no database writes. Safe to run while scheduler is active.

Usage:
    python backtest_blend_weights.py --db-path data/mlb_pregame.db
    python backtest_blend_weights.py --db-path data/mlb_pregame.db --min-ab 2 --verbose

    # After updating BATTER_WEIGHT from baseline split test results:
    # Edit BATTER_WEIGHT below, then run.
"""

import sqlite3
import argparse

# ── Configuration ──────────────────────────────────────────────────────────

# UPDATE these after running backtest_baseline_split.py
# Optimal values from baseline split test (2026-05-30): batter=0.30, pitcher=0.70
BATTER_WEIGHT  = 0.30
PITCHER_WEIGHT = 0.70

# Blend weight candidates — each tuple must sum to 1.0
# Format: (W_BASELINE, W_PT, W_ZONE)
# Original 9 configs + 6 extended configs testing higher baseline / lower pt
BLEND_CANDIDATES = [
    # Confirmed best from previous run — anchor points
    (0.60, 0.25, 0.15),   # current best
    (0.55, 0.30, 0.15),
    (0.50, 0.35, 0.15),
    # Extended — higher baseline with pt=0.25 locked
    (0.65, 0.25, 0.10),
    (0.65, 0.20, 0.15),
    (0.70, 0.20, 0.10),
    (0.70, 0.15, 0.15),
    (0.75, 0.15, 0.10),
    # Test pt=0.20 at high baseline to confirm pt=0.25 is truly optimal
    (0.60, 0.20, 0.20),   # already tested — anchor
    (0.65, 0.15, 0.20),
]

# Regression constants — fixed at optimized values
TAU               = 150
REGRESSION_TARGET = 0.22

LIVE_BLEND = (0.40, 0.35, 0.25)
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

def _load_batter_hand_splits(conn, window_code):
    rows = conn.execute(
        """
        SELECT as_of_date, player_id, split_hand,
               batting_avg, at_bats
        FROM   fact_batter_hand_splits
        WHERE  window_code = ?
          AND  batting_avg IS NOT NULL
        """,
        (window_code,),
    ).fetchall()
    return {(r[0], r[1], r[2]): (r[3], r[4] or 0) for r in rows}


def _load_pitcher_hand_splits(conn, window_code):
    rows = conn.execute(
        """
        SELECT as_of_date, pitcher_id, split_hand,
               batting_avg_allowed, batters_faced
        FROM   fact_pitcher_hand_splits
        WHERE  window_code         = ?
          AND  batting_avg_allowed IS NOT NULL
        """,
        (window_code,),
    ).fetchall()
    return {(r[0], r[1], r[2]): (r[3], r[4] or 0) for r in rows}


def _load_matchups(conn, window_code, min_ab):
    rows = conn.execute(
        """
        SELECT
            m.as_of_date,
            m.game_id,
            m.batter_id,
            m.pitcher_id,
            m.projected_batting_avg        AS current_projection,
            m.batter_vs_hand_batting_avg   AS stored_baseline,
            m.pitch_type_match_score,
            m.zone_match_score,
            m.park_adjustment_factor,
            m.weather_adjustment_factor,
            p_pitcher.throws               AS pitcher_throws,
            p_batter.bats                  AS batter_bats,
            r.batting_avg                  AS actual_ba
        FROM   fact_matchup_batter_pitcher m
        JOIN   dim_players p_pitcher ON p_pitcher.player_id = m.pitcher_id
        JOIN   dim_players p_batter  ON p_batter.player_id  = m.batter_id
        JOIN   fact_player_game_results r
               ON  r.game_date = m.as_of_date
               AND r.player_id = m.batter_id
        WHERE  m.window_code              = ?
          AND  r.at_bats                 >= ?
          AND  m.projected_batting_avg    IS NOT NULL
          AND  m.pitch_type_match_score   IS NOT NULL
          AND  m.zone_match_score         IS NOT NULL
        ORDER  BY m.as_of_date, m.batter_id
        """,
        (window_code, min_ab),
    ).fetchall()

    result = []
    for r in rows:
        pitcher_throws = r[10]
        batter_bats    = r[11]
        if pitcher_throws not in ("R", "L") or batter_bats not in ("R", "L", "S"):
            continue
        eff_hand = (
            "L" if pitcher_throws == "R" else "R"
        ) if batter_bats == "S" else batter_bats

        result.append({
            "as_of_date":           r[0],
            "game_id":              r[1],
            "batter_id":            r[2],
            "pitcher_id":           r[3],
            "current_projection":   r[4],
            "stored_baseline":      r[5],
            "pt_score":             r[6],
            "zone_score":           r[7],
            "park":                 r[8] or 1.0,
            "weather":              r[9] or 1.0,
            "pitcher_throws":       pitcher_throws,
            "effective_batter_hand": eff_hand,
            "actual_ba":            r[12],
        })
    return result


# ── Baseline reconstruction ────────────────────────────────────────────────

def _build_baseline(matchup, batter_splits, pitcher_splits):
    b_key = (matchup["as_of_date"], matchup["batter_id"],
             matchup["pitcher_throws"])
    p_key = (matchup["as_of_date"], matchup["pitcher_id"],
             matchup["effective_batter_hand"])

    b_row = batter_splits.get(b_key)
    p_row = pitcher_splits.get(p_key)

    b_avg = _regress(b_row[0] if b_row else None,
                     b_row[1] if b_row else 0, REGRESSION_TARGET, TAU)
    p_avg = _regress(p_row[0] if p_row else None,
                     p_row[1] if p_row else 0, REGRESSION_TARGET, TAU)

    return (b_avg * BATTER_WEIGHT) + (p_avg * PITCHER_WEIGHT)


# ── Metrics ────────────────────────────────────────────────────────────────

def _compute_metrics(matchups, projections):
    errors = []
    signed = []
    dir_correct = dir_total = 0

    for m, proj in zip(matchups, projections):
        if proj is None or m["actual_ba"] is None:
            continue
        actual   = m["actual_ba"]
        baseline = m["stored_baseline"]
        errors.append(abs(proj - actual))
        signed.append(proj - actual)
        if baseline is not None:
            if (actual > baseline) == (proj > baseline):
                dir_correct += 1
            dir_total += 1

    n    = len(errors)
    mae  = round(sum(errors) / n, 5)  if n > 0 else None
    bias = round(sum(signed) / n, 5)  if n > 0 else None
    dir_acc = round(dir_correct / dir_total, 4) if dir_total > 0 else None
    return {"n": n, "mae": mae, "bias": bias, "dir_acc": dir_acc}


# ── Main ───────────────────────────────────────────────────────────────────

def run(db_path, window_code=WINDOW_CODE, min_ab=DEFAULT_MIN_AB,
        verbose=False):

    print(f"\nBacktest: Blend Weight Grid Search")
    print(f"DB: {db_path} | Window: {window_code} | Min AB: {min_ab}")
    print(f"Fixed: tau={TAU}, regression_target={REGRESSION_TARGET}, "
          f"batter_w={BATTER_WEIGHT}, pitcher_w={PITCHER_WEIGHT}")
    print("Loading data...")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")

    batter_splits  = _load_batter_hand_splits(conn, window_code)
    pitcher_splits = _load_pitcher_hand_splits(conn, window_code)
    matchups       = _load_matchups(conn, window_code, min_ab)
    conn.close()

    print(f"Matchup rows: {len(matchups)}")

    if not matchups:
        print("No matchup rows found.")
        return

    # Pre-build baselines once — same for all blend weight configs
    baselines = [_build_baseline(m, batter_splits, pitcher_splits)
                 for m in matchups]

    # ── Baseline: current live model ───────────────────────────────────────
    current_projs = [m["current_projection"] for m in matchups]
    current_metrics = _compute_metrics(matchups, current_projs)

    print(f"\n{'='*72}")
    print(f"CURRENT LIVE MODEL (baseline=0.40, pt=0.35, zone=0.25)")
    print(f"  n={current_metrics['n']:,}  "
          f"MAE={current_metrics['mae']:.5f}  "
          f"bias={current_metrics['bias']:+.5f}  "
          f"dir_acc={current_metrics['dir_acc']:.4f}")
    print(f"{'='*72}")

    # ── Grid search ────────────────────────────────────────────────────────
    results = []

    for w_base, w_pt, w_zone in BLEND_CANDIDATES:
        assert abs(w_base + w_pt + w_zone - 1.0) < 0.001, \
            f"Weights must sum to 1.0: {w_base}+{w_pt}+{w_zone}"

        projections = []
        for m, baseline in zip(matchups, baselines):
            blended = ((baseline      * w_base) +
                       (m["pt_score"] * w_pt)   +
                       (m["zone_score"] * w_zone))
            projections.append(round(blended * m["park"] * m["weather"], 5))

        metrics   = _compute_metrics(matchups, projections)
        mae_delta = round(metrics["mae"] - current_metrics["mae"], 5)
        is_live   = (w_base, w_pt, w_zone) == LIVE_BLEND

        results.append({
            "w_base":    w_base,
            "w_pt":      w_pt,
            "w_zone":    w_zone,
            "metrics":   metrics,
            "mae_delta": mae_delta,
            "is_live":   is_live,
        })

        if verbose:
            tag = " (live)" if is_live else ""
            print(f"  base={w_base:.2f} pt={w_pt:.2f} zone={w_zone:.2f} | "
                  f"MAE={metrics['mae']:.5f} ({mae_delta:+.5f})  "
                  f"bias={metrics['bias']:+.5f}  "
                  f"dir={metrics['dir_acc']:.4f}{tag}")

    # ── Results table ──────────────────────────────────────────────────────
    results.sort(key=lambda r: r["metrics"]["mae"] or 9999)

    print(f"\n{'─'*76}")
    print(f"{'BASELINE':<10} {'PT_SCORE':<10} {'ZONE':<8} "
          f"{'MAE':>8} {'vs LIVE':>9} {'BIAS':>9} {'DIR ACC':>9} {'N':>7}")
    print(f"{'─'*76}")

    for r in results:
        m    = r["metrics"]
        tag  = " ◄ BEST" if r == results[0] else ""
        live = " (live)" if r["is_live"] else ""
        print(
            f"{r['w_base']:<10.2f}"
            f"{r['w_pt']:<10.2f}"
            f"{r['w_zone']:<8.2f}"
            f"{m['mae']:>8.5f} "
            f"{r['mae_delta']:>+9.5f} "
            f"{m['bias']:>+9.5f} "
            f"{m['dir_acc']:>9.4f} "
            f"{m['n']:>7,}"
            f"{tag}{live}"
        )

    print(f"{'─'*76}")

    best = results[0]
    print(f"\nBest configuration: baseline={best['w_base']:.2f}, "
          f"pt={best['w_pt']:.2f}, zone={best['w_zone']:.2f}")
    print(f"MAE improvement over live: {best['mae_delta']:+.5f}")

    if abs(best["mae_delta"]) < 0.00030:
        print("NOTE: Improvement < 0.0003 — current 0.40/0.35/0.25 blend is near-optimal.")
    elif best["mae_delta"] < 0:
        print(f"Recommend updating blend weights to "
              f"baseline={best['w_base']:.2f} / pt={best['w_pt']:.2f} / "
              f"zone={best['w_zone']:.2f}")

    # Component sensitivity
    print(f"\nBaseline weight sensitivity (averaged across pt/zone combos):")
    for w in sorted(set(r["w_base"] for r in results)):
        sub = [r for r in results if r["w_base"] == w]
        avg_mae = sum(r["metrics"]["mae"] for r in sub) / len(sub)
        avg_d   = sum(r["mae_delta"] for r in sub) / len(sub)
        print(f"  baseline={w:.2f}  avg MAE={avg_mae:.5f}  avg delta={avg_d:+.5f}")

    print(f"\nPT score weight sensitivity (averaged across baseline/zone combos):")
    for w in sorted(set(r["w_pt"] for r in results)):
        sub = [r for r in results if r["w_pt"] == w]
        avg_mae = sum(r["metrics"]["mae"] for r in sub) / len(sub)
        avg_d   = sum(r["mae_delta"] for r in sub) / len(sub)
        print(f"  pt_score={w:.2f}  avg MAE={avg_mae:.5f}  avg delta={avg_d:+.5f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Grid search blend weights for Final Projection"
    )
    parser.add_argument("--db-path", default="data/mlb_pregame.db")
    parser.add_argument("--window",  default=WINDOW_CODE)
    parser.add_argument("--min-ab",  type=int, default=DEFAULT_MIN_AB)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    run(db_path=args.db_path, window_code=args.window,
        min_ab=args.min_ab, verbose=args.verbose)
