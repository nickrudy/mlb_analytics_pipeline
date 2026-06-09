"""
backtest_baseline_split.py
---------------------------
Grid search over the batter/pitcher weighting inside the baseline
handedness split calculation.

Current live formula:
    baseline = (batter_vs_hand_avg * 0.40) + (pitcher_vs_hand_avg_allowed * 0.60)

Tests all combinations of batter_weight in [0.30, 0.40, 0.50, 0.60, 0.70]
with pitcher_weight = 1 - batter_weight.

Reconstructs the baseline from raw components in fact_batter_hand_splits
and fact_pitcher_hand_splits rather than using the pre-blended stored value,
so alternative weighting schemes can be evaluated fairly.

Uses tau=150 and REGRESSION_TARGET=0.22 (current live values) throughout
so the baseline weight is isolated as the only variable.

READ-ONLY — no database writes. Safe to run while scheduler is active.

Usage:
    python backtest_baseline_split.py --db-path data/mlb_pregame.db
    python backtest_baseline_split.py --db-path data/mlb_pregame.db --min-ab 2 --verbose
"""

import sqlite3
import argparse
from collections import defaultdict

# ── Configuration ──────────────────────────────────────────────────────────

# Batter weight candidates — pitcher weight = 1 - batter_weight
BATTER_WEIGHT_CANDIDATES = [0.30, 0.40, 0.50, 0.60, 0.70]

# Blend weights — fixed at current live values
W_BASELINE = 0.40
W_PT       = 0.35
W_ZONE     = 0.25

# Regression constants — fixed at current optimized values
TAU              = 150
REGRESSION_TARGET = 0.22

# Current live batter weight for comparison
LIVE_BATTER_WEIGHT = 0.40

WINDOW_CODE   = "SEASON"
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
    """
    Returns {(as_of_date, player_id, pitcher_hand): (batting_avg, at_bats)}
    split_hand in this table = pitcher hand the batter faced.
    """
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
    return {
        (r[0], r[1], r[2]): (r[3], r[4] or 0)
        for r in rows
    }


def _load_pitcher_hand_splits(conn, window_code):
    """
    Returns {(as_of_date, pitcher_id, batter_hand): (batting_avg_allowed, batters_faced)}
    split_hand = batter hand the pitcher faced.
    """
    rows = conn.execute(
        """
        SELECT as_of_date, pitcher_id, split_hand,
               batting_avg_allowed, batters_faced
        FROM   fact_pitcher_hand_splits
        WHERE  window_code        = ?
          AND  batting_avg_allowed IS NOT NULL
        """,
        (window_code,),
    ).fetchall()
    return {
        (r[0], r[1], r[2]): (r[3], r[4] or 0)
        for r in rows
    }


def _load_stored_scores(conn, window_code):
    """
    Load stored pt_score and zone_score from fact_matchup_batter_pitcher.
    These are fixed at current live values — only the baseline is being varied.
    Returns {(as_of_date, game_id, batter_id, pitcher_id): (pt_score, zone_score, park, weather)}
    """
    rows = conn.execute(
        """
        SELECT as_of_date, game_id, batter_id, pitcher_id,
               pitch_type_match_score,
               zone_match_score,
               park_adjustment_factor,
               weather_adjustment_factor
        FROM   fact_matchup_batter_pitcher
        WHERE  window_code = ?
          AND  pitch_type_match_score IS NOT NULL
          AND  zone_match_score IS NOT NULL
        """,
        (window_code,),
    ).fetchall()
    return {
        (r[0], r[1], r[2], r[3]): (r[4], r[5], r[6] or 1.0, r[7] or 1.0)
        for r in rows
    }


def _load_matchups(conn, window_code, min_ab):
    """
    Load all matchup rows with actual outcomes.
    Returns list of dicts.
    """
    rows = conn.execute(
        """
        SELECT
            m.as_of_date,
            m.game_id,
            m.batter_id,
            m.pitcher_id,
            m.projected_batting_avg        AS current_projection,
            m.batter_vs_hand_batting_avg   AS stored_baseline,
            p_pitcher.throws               AS pitcher_throws,
            p_batter.bats                  AS batter_bats,
            r.at_bats,
            r.batting_avg                  AS actual_ba
        FROM   fact_matchup_batter_pitcher m
        JOIN   dim_players p_pitcher ON p_pitcher.player_id = m.pitcher_id
        JOIN   dim_players p_batter  ON p_batter.player_id  = m.batter_id
        JOIN   fact_player_game_results r
               ON  r.game_date = m.as_of_date
               AND r.player_id = m.batter_id
        WHERE  m.window_code           = ?
          AND  r.at_bats              >= ?
          AND  m.projected_batting_avg IS NOT NULL
          AND  m.batter_vs_hand_batting_avg IS NOT NULL
        ORDER  BY m.as_of_date, m.batter_id
        """,
        (window_code, min_ab),
    ).fetchall()

    result = []
    for r in rows:
        pitcher_throws = r[6]
        batter_bats    = r[7]
        if pitcher_throws not in ("R", "L") or batter_bats not in ("R", "L", "S"):
            continue
        effective_batter_hand = (
            "L" if pitcher_throws == "R" else "R"
        ) if batter_bats == "S" else batter_bats

        result.append({
            "as_of_date":           r[0],
            "game_id":              r[1],
            "batter_id":            r[2],
            "pitcher_id":           r[3],
            "current_projection":   r[4],
            "stored_baseline":      r[5],
            "pitcher_throws":       pitcher_throws,
            "effective_batter_hand": effective_batter_hand,
            "at_bats":              r[8],
            "actual_ba":            r[9],
        })
    return result


# ── Baseline reconstruction ────────────────────────────────────────────────

def _reconstruct_baseline(matchup, batter_splits, pitcher_splits,
                           batter_weight, pitcher_weight):
    """
    Reconstruct baseline using given batter/pitcher weights.
    Falls back to stored baseline if raw components are unavailable.
    """
    b_key = (matchup["as_of_date"], matchup["batter_id"],
             matchup["pitcher_throws"])
    p_key = (matchup["as_of_date"], matchup["pitcher_id"],
             matchup["effective_batter_hand"])

    b_row = batter_splits.get(b_key)
    p_row = pitcher_splits.get(p_key)

    batter_avg  = b_row[0] if b_row and b_row[0] is not None else None
    batter_ab   = b_row[1] if b_row else 0
    pitcher_avg = p_row[0] if p_row and p_row[0] is not None else None
    pitcher_bf  = p_row[1] if p_row else 0

    # Regress each component toward REGRESSION_TARGET for small samples
    if batter_avg is not None:
        batter_regressed = _regress(batter_avg, batter_ab,
                                    REGRESSION_TARGET, TAU)
    else:
        batter_regressed = REGRESSION_TARGET

    if pitcher_avg is not None:
        pitcher_regressed = _regress(pitcher_avg, pitcher_bf,
                                     REGRESSION_TARGET, TAU)
    else:
        pitcher_regressed = REGRESSION_TARGET

    return (batter_regressed * batter_weight) + (pitcher_regressed * pitcher_weight)


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
        err = abs(proj - actual)
        errors.append(err)
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

    print(f"\nBacktest: Baseline Split Weighting Grid Search")
    print(f"DB: {db_path} | Window: {window_code} | Min AB: {min_ab}")
    print(f"Fixed: tau={TAU}, regression_target={REGRESSION_TARGET}, "
          f"blend={W_BASELINE}/{W_PT}/{W_ZONE}")
    print("Loading data...")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")

    batter_splits  = _load_batter_hand_splits(conn, window_code)
    pitcher_splits = _load_pitcher_hand_splits(conn, window_code)
    stored_scores  = _load_stored_scores(conn, window_code)
    matchups       = _load_matchups(conn, window_code, min_ab)
    conn.close()

    print(f"Matchup rows: {len(matchups)}")
    print(f"Batter hand splits: {len(batter_splits)}")
    print(f"Pitcher hand splits: {len(pitcher_splits)}")

    if not matchups:
        print("No matchup rows found.")
        return

    # ── Baseline: current live model ───────────────────────────────────────
    current_projs  = [m["current_projection"] for m in matchups]
    current_metrics = _compute_metrics(matchups, current_projs)

    print(f"\n{'='*68}")
    print(f"CURRENT LIVE MODEL (batter=0.40, pitcher=0.60)")
    print(f"  n={current_metrics['n']:,}  "
          f"MAE={current_metrics['mae']:.5f}  "
          f"bias={current_metrics['bias']:+.5f}  "
          f"dir_acc={current_metrics['dir_acc']:.4f}")
    print(f"{'='*68}")

    # ── Grid search ────────────────────────────────────────────────────────
    results = []

    for batter_weight in BATTER_WEIGHT_CANDIDATES:
        pitcher_weight = round(1.0 - batter_weight, 2)

        projections = []
        for m in matchups:
            scores_key = (m["as_of_date"], m["game_id"],
                          m["batter_id"],  m["pitcher_id"])
            scores = stored_scores.get(scores_key)

            baseline = _reconstruct_baseline(
                m, batter_splits, pitcher_splits,
                batter_weight, pitcher_weight
            )

            if scores:
                pt_score, zone_score, park, weather = scores
                blended = ((baseline  * W_BASELINE) +
                           (pt_score  * W_PT) +
                           (zone_score * W_ZONE))
            else:
                blended = baseline

            projections.append(round(blended * (scores[2] if scores else 1.0)
                                              * (scores[3] if scores else 1.0), 5))

        metrics   = _compute_metrics(matchups, projections)
        mae_delta = round(metrics["mae"] - current_metrics["mae"], 5)
        is_live   = abs(batter_weight - LIVE_BATTER_WEIGHT) < 0.001

        results.append({
            "batter_w":  batter_weight,
            "pitcher_w": pitcher_weight,
            "metrics":   metrics,
            "mae_delta": mae_delta,
            "is_live":   is_live,
        })

        if verbose:
            tag = " (live)" if is_live else ""
            print(f"  batter={batter_weight:.2f} pitcher={pitcher_weight:.2f} | "
                  f"MAE={metrics['mae']:.5f} ({mae_delta:+.5f})  "
                  f"bias={metrics['bias']:+.5f}  "
                  f"dir={metrics['dir_acc']:.4f}{tag}")

    # ── Results table ──────────────────────────────────────────────────────
    results.sort(key=lambda r: r["metrics"]["mae"] or 9999)

    print(f"\n{'─'*72}")
    print(f"{'BATTER_W':<10} {'PITCHER_W':<11} {'MAE':>8} {'vs LIVE':>9} "
          f"{'BIAS':>9} {'DIR ACC':>9} {'N':>7}")
    print(f"{'─'*72}")

    for r in results:
        m      = r["metrics"]
        tag    = " ◄ BEST" if r == results[0] else ""
        live   = " (live)" if r["is_live"] else ""
        print(
            f"{r['batter_w']:<10.2f} "
            f"{r['pitcher_w']:<11.2f} "
            f"{m['mae']:>8.5f} "
            f"{r['mae_delta']:>+9.5f} "
            f"{m['bias']:>+9.5f} "
            f"{m['dir_acc']:>9.4f} "
            f"{m['n']:>7,}"
            f"{tag}{live}"
        )

    print(f"{'─'*72}")

    best = results[0]
    print(f"\nBest configuration: batter={best['batter_w']:.2f}, "
          f"pitcher={best['pitcher_w']:.2f}")
    print(f"MAE improvement over live: {best['mae_delta']:+.5f}")

    if abs(best["mae_delta"]) < 0.00030:
        print("NOTE: Improvement < 0.0003 — current 40/60 split is near-optimal.")
    elif best["mae_delta"] < 0:
        print(f"Recommend updating baseline weights to "
              f"batter={best['batter_w']:.2f} / pitcher={best['pitcher_w']:.2f}")

    # Bias summary
    print(f"\nBias by configuration (closest to 0 = most unbiased):")
    bias_sorted = sorted(results, key=lambda r: abs(r["metrics"]["bias"] or 9999))
    for r in bias_sorted:
        print(f"  batter={r['batter_w']:.2f}  bias={r['metrics']['bias']:+.5f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Grid search batter/pitcher baseline split weighting"
    )
    parser.add_argument("--db-path", default="data/mlb_pregame.db")
    parser.add_argument("--window",  default=WINDOW_CODE)
    parser.add_argument("--min-ab",  type=int, default=DEFAULT_MIN_AB)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    run(db_path=args.db_path, window_code=args.window,
        min_ab=args.min_ab, verbose=args.verbose)
