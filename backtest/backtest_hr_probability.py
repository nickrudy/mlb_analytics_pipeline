"""
backtest_hr_probability.py
---------------------------
Grid search over MIN_BBE_THRESHOLD and blend weights for the
projected_hr_probability model.

Outcome variable: hr_flag (1 if batter hit >= 1 HR in the game, else 0)
from fact_player_game_results.

Because HR is a binary per-game outcome, metrics differ from BA/TB:
    MAE        — mean absolute error on hr_probability (lower is better)
    bias       — mean signed error; + = over-projecting HR probability
    calibration — observed HR rate within probability buckets vs predicted
    brier_score — proper scoring rule for probability forecasts (lower is better)
                  Brier = mean((predicted - actual)^2)

Tests:
    1. MIN_BBE_THRESHOLD in [20, 50, 100, 150, 200]
       (HR rate stabilizes more slowly than BA — current 20 likely too low)
    2. Blend weights (batter_hr_rate / pitcher_hr_rate / barrel_context)
       across configurations summing to 1.0

Fixed: LEAGUE_AVG_HR_PER_PA=0.0293, LEAGUE_AVG_BARREL_RATE=0.0708
       (empirically derived from 2026 season data)

READ-ONLY — no database writes. Safe to run while scheduler is active.

Usage:
    python backtest/backtest_hr_probability.py --db-path data/mlb_pregame.db
    python backtest/backtest_hr_probability.py --db-path data/mlb_pregame.db --min-ab 2 --verbose
"""

import sqlite3
import argparse
from collections import defaultdict

# ── Configuration ──────────────────────────────────────────────────────────

BBE_THRESHOLD_CANDIDATES = [20, 50, 100, 150, 200]

# Blend weight candidates — (W_BATTER, W_PITCHER, W_BARREL)
# Must sum to 1.0
BLEND_CANDIDATES = [
    (0.45, 0.35, 0.20),   # ← current live
    (0.50, 0.30, 0.20),
    (0.50, 0.35, 0.15),
    (0.55, 0.30, 0.15),
    (0.55, 0.25, 0.20),
    (0.60, 0.25, 0.15),
    (0.60, 0.30, 0.10),
    (0.65, 0.25, 0.10),
    (0.70, 0.20, 0.10),   # BA/TB optimal — test if it transfers
]

# Empirically derived 2026 constants
LEAGUE_AVG_HR_PER_PA   = 0.0293   # observed from fact_player_game_results (at_bats>=2)
LEAGUE_AVG_BARREL_RATE = 0.0708   # observed from fact_batter_power_profile (corrected barrel def)

LIVE_BLEND        = (0.45, 0.35, 0.20)
LIVE_BBE_THRESHOLD = 20

WINDOW_CODE    = "SEASON"
DEFAULT_MIN_AB = 2


# ── Regression helper ──────────────────────────────────────────────────────

def _regress(observed, sample_size, league_avg, threshold):
    if observed is None:
        return league_avg
    if sample_size >= threshold:
        return observed
    w = sample_size / threshold
    return (observed * w) + (league_avg * (1 - w))


# ── Data loading ───────────────────────────────────────────────────────────

def _load_batter_power(conn, window_code):
    """
    Batter power profile — hr_per_pa, barrel rates, BBE counts.
    Returns {(as_of_date, player_id, pitcher_hand): (hr_per_pa, bpp_vs_hand, bbe, overall_bpp)}
    """
    rows = conn.execute(
        """
        SELECT as_of_date, player_id,
               hr_per_pa,
               barrels_per_pa_vs_rhp,
               barrels_per_pa_vs_lhp,
               barrels_per_pa,
               batted_ball_events
        FROM   fact_batter_power_profile
        WHERE  window_code = ?
        """,
        (window_code,),
    ).fetchall()

    result = {}
    for r in rows:
        as_of, pid, hr_pa, bpp_r, bpp_l, bpp_overall, bbe = r
        result[(as_of, pid, 'R')] = (hr_pa, bpp_r, bbe or 0, bpp_overall)
        result[(as_of, pid, 'L')] = (hr_pa, bpp_l, bbe or 0, bpp_overall)
    return result


def _load_pitcher_hr_vuln(conn, window_code):
    """
    Pitcher HR vulnerability.
    Returns {(as_of_date, pitcher_id, batter_hand): (hr_per_bf, barrel_rate, bbe)}
    """
    rows = conn.execute(
        """
        SELECT as_of_date, pitcher_id, split_hand,
               hr_per_bf_allowed, barrel_rate_allowed, batted_ball_events
        FROM   fact_pitcher_hr_vulnerability
        WHERE  window_code = ?
        """,
        (window_code,),
    ).fetchall()
    return {
        (r[0], r[1], r[2]): (r[3], r[4], r[5] or 0)
        for r in rows
    }


def _load_park_hr_factors(conn):
    """Returns {venue_id: (factor_rhb, factor_lhb)} normalised to multipliers."""
    rows = conn.execute(
        """
        SELECT venue_id, park_hr_factor_rhb, park_hr_factor_lhb
        FROM   dim_venues
        WHERE  park_hr_factor_rhb IS NOT NULL
        """
    ).fetchall()
    return {
        r[0]: (
            round(r[1] / 100.0, 4) if r[1] else 1.0,
            round(r[2] / 100.0, 4) if r[2] else 1.0,
        )
        for r in rows
    }


def _load_matchups(conn, window_code, min_ab):
    """
    Load matchup rows joined to actual HR outcomes.
    ab_per_game computed live from lineup slot — same logic as compute_match_scores.py.
    """
    SLOT_AB = {1: 3.888, 2: 3.781, 3: 3.708, 4: 3.652, 5: 3.549,
               6: 3.456, 7: 3.339, 8: 3.113, 9: 3.031}

    rows = conn.execute(
        """
        SELECT
            m.as_of_date,
            m.game_id,
            m.batter_id,
            m.pitcher_id,
            m.projected_hr_probability     AS current_proj,
            m.weather_adjustment_factor    AS weather_adj,
            p_pitcher.throws               AS pitcher_throws,
            p_batter.bats                  AS batter_bats,
            g.venue_id,
            r.hr_flag,
            r.at_bats,
            l.lineup_slot,
            m.proj_at_bats_per_game        AS stored_ab
        FROM   fact_matchup_batter_pitcher m
        JOIN   dim_players p_pitcher ON p_pitcher.player_id = m.pitcher_id
        JOIN   dim_players p_batter  ON p_batter.player_id  = m.batter_id
        LEFT JOIN fact_games g
               ON  g.as_of_date = m.as_of_date
               AND g.game_id    = m.game_id
        JOIN   fact_player_game_results r
               ON  r.game_date = m.as_of_date
               AND r.player_id = m.batter_id
        LEFT JOIN fact_game_lineups l
               ON  l.as_of_date = m.as_of_date
               AND l.game_id    = m.game_id
               AND l.player_id  = m.batter_id
        WHERE  m.window_code            = ?
          AND  r.at_bats               >= ?
          AND  m.projected_hr_probability IS NOT NULL
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
        eff_hand = (
            "L" if pitcher_throws == "R" else "R"
        ) if batter_bats == "S" else batter_bats

        lineup_slot = r[11]
        stored_ab   = r[12]
        if lineup_slot and lineup_slot in SLOT_AB:
            ab_per_game = SLOT_AB[lineup_slot]
        elif stored_ab:
            ab_per_game = stored_ab
        else:
            ab_per_game = 3.502

        result.append({
            "as_of_date":            r[0],
            "game_id":               r[1],
            "batter_id":             r[2],
            "pitcher_id":            r[3],
            "current_proj":          r[4],
            "weather_adj":           r[5] or 1.0,
            "pitcher_throws":        pitcher_throws,
            "effective_batter_hand": eff_hand,
            "venue_id":              r[8],
            "hr_flag":               r[9] or 0,
            "at_bats":               r[10],
            "ab_per_game":           ab_per_game,
        })
    return result


# ── HR probability replay ──────────────────────────────────────────────────

def _compute_hr_prob(matchup, batter_power, pitcher_vuln, park_factors,
                     bbe_threshold, w_batter, w_pitcher, w_barrel):
    """
    Replay _compute_hr_probability with given threshold and blend weights.
    Returns projected HR probability per game, or None.
    """
    as_of      = matchup["as_of_date"]
    batter_id  = matchup["batter_id"]
    pitcher_id = matchup["pitcher_id"]
    p_throws   = matchup["pitcher_throws"]
    eff_hand   = matchup["effective_batter_hand"]
    weather    = matchup["weather_adj"]
    venue_id   = matchup["venue_id"]
    ab_per_game = matchup["ab_per_game"]

    # ── Batter HR rate ─────────────────────────────────────────────────────
    b_key  = (as_of, batter_id, p_throws)
    b_row  = batter_power.get(b_key)
    if not b_row:
        return None

    hr_per_pa, bpp_vs_hand, bbe_count, overall_bpp = b_row
    if hr_per_pa is None:
        return None

    batter_hr_rate = _regress(hr_per_pa, bbe_count,
                               LEAGUE_AVG_HR_PER_PA, bbe_threshold)
    batter_bpp     = bpp_vs_hand if bpp_vs_hand is not None else overall_bpp

    # ── Pitcher HR rate ────────────────────────────────────────────────────
    p_key  = (as_of, pitcher_id, eff_hand)
    p_row  = pitcher_vuln.get(p_key)

    pitcher_hr_rate    = None
    pitcher_barrel_rate = None

    if p_row:
        ph_bf, ph_barrel, p_bbe = p_row
        if ph_bf is not None:
            pitcher_hr_rate = _regress(ph_bf, p_bbe,
                                       LEAGUE_AVG_HR_PER_PA, bbe_threshold)
        pitcher_barrel_rate = ph_barrel

    # ── Barrel context ─────────────────────────────────────────────────────
    barrel_context = None
    if batter_bpp is not None and pitcher_barrel_rate is not None:
        b_rel = batter_bpp / max(LEAGUE_AVG_BARREL_RATE, 0.001)
        p_rel = pitcher_barrel_rate / max(LEAGUE_AVG_BARREL_RATE, 0.001)
        barrel_context = LEAGUE_AVG_HR_PER_PA * b_rel * p_rel

    # ── Park factor ────────────────────────────────────────────────────────
    park_factors_venue = park_factors.get(venue_id, (1.0, 1.0))
    park_hr = park_factors_venue[1] if eff_hand == "L" else park_factors_venue[0]

    # ── Blend and convert to per-game ─────────────────────────────────────
    if pitcher_hr_rate is not None and barrel_context is not None:
        blended = ((batter_hr_rate  * w_batter) +
                   (pitcher_hr_rate * w_pitcher) +
                   (barrel_context  * w_barrel))
    elif pitcher_hr_rate is not None:
        total = w_batter + w_pitcher
        blended = ((batter_hr_rate  * (w_batter  / total)) +
                   (pitcher_hr_rate * (w_pitcher / total)))
    elif barrel_context is not None:
        total = w_batter + w_barrel
        blended = ((batter_hr_rate * (w_batter / total)) +
                   (barrel_context * (w_barrel / total)))
    else:
        blended = batter_hr_rate

    # Per-game conversion — mirrors compute_match_scores.py exactly
    return round(blended * ab_per_game * park_hr * weather, 4)


# ── Metrics ────────────────────────────────────────────────────────────────

def _compute_metrics(matchups, projections):
    """
    Compute MAE, bias, Brier score, and calibration for HR probability.
    hr_flag is the binary outcome (1 = HR, 0 = no HR).
    """
    errors  = []
    signed  = []
    brier   = []

    # Calibration buckets — per-game probability ranges
    buckets = {
        "0.00-0.05": {"preds": [], "actuals": []},
        "0.05-0.08": {"preds": [], "actuals": []},
        "0.08-0.11": {"preds": [], "actuals": []},
        "0.11-0.14": {"preds": [], "actuals": []},
        "0.14-0.18": {"preds": [], "actuals": []},
        "0.18+":     {"preds": [], "actuals": []},
    }

    def get_bucket(p):
        if p < 0.05:  return "0.00-0.05"
        if p < 0.08:  return "0.05-0.08"
        if p < 0.11:  return "0.08-0.11"
        if p < 0.14:  return "0.11-0.14"
        if p < 0.18:  return "0.14-0.18"
        return "0.18+"

    for m, proj in zip(matchups, projections):
        if proj is None:
            continue
        actual = m["hr_flag"]
        errors.append(abs(proj - actual))
        signed.append(proj - actual)
        brier.append((proj - actual) ** 2)
        bk = get_bucket(proj)
        buckets[bk]["preds"].append(proj)
        buckets[bk]["actuals"].append(actual)

    n = len(errors)
    def avg(lst): return round(sum(lst) / len(lst), 5) if lst else None

    calibration = {}
    for bk, data in buckets.items():
        if data["preds"]:
            calibration[bk] = {
                "n":           len(data["preds"]),
                "avg_pred":    round(sum(data["preds"])   / len(data["preds"]),   4),
                "actual_rate": round(sum(data["actuals"]) / len(data["actuals"]), 4),
            }

    return {
        "n":            n,
        "mae":          avg(errors),
        "bias":         avg(signed),
        "brier":        avg(brier),
        "calibration":  calibration,
    }


# ── Main ───────────────────────────────────────────────────────────────────

def run(db_path, window_code=WINDOW_CODE, min_ab=DEFAULT_MIN_AB,
        verbose=False):

    print(f"\nBacktest: HR Probability — BBE Threshold × Blend Weight Grid Search")
    print(f"DB: {db_path} | Window: {window_code} | Min AB: {min_ab}")
    print(f"Fixed: LEAGUE_AVG_HR_PER_PA={LEAGUE_AVG_HR_PER_PA}, "
          f"LEAGUE_AVG_BARREL_RATE={LEAGUE_AVG_BARREL_RATE}")
    print("Loading data...")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")

    batter_power  = _load_batter_power(conn, window_code)
    pitcher_vuln  = _load_pitcher_hr_vuln(conn, window_code)
    park_factors  = _load_park_hr_factors(conn)
    matchups      = _load_matchups(conn, window_code, min_ab)
    conn.close()

    print(f"Matchup rows loaded: {len(matchups)}")
    print(f"Batter power profiles: {len(batter_power) // 2}")
    print(f"Pitcher HR vuln profiles: {len(pitcher_vuln)}")

    if not matchups:
        print("No matchup rows found.")
        return

    # ── Current live model — replayed live with current constants ─────────
    # Uses stored proj_slg equivalent: replays at current deployed settings
    # (bbe=100, blend=0.70/0.20/0.10) so comparison reflects actual deployed model
    current_projs = [
        _compute_hr_prob(m, batter_power, pitcher_vuln, park_factors,
                         100, 0.70, 0.20, 0.10)
        for m in matchups
    ]
    current_metrics = _compute_metrics(matchups, current_projs)

    print(f"\n{'='*72}")
    print(f"CURRENT LIVE MODEL (bbe_thresh=100, blend=0.70/0.20/0.10, "
          f"per-game output)")
    print(f"  n={current_metrics['n']:,}  "
          f"MAE={current_metrics['mae']:.5f}  "
          f"bias={current_metrics['bias']:+.5f}  "
          f"Brier={current_metrics['brier']:.5f}")
    print(f"  Calibration:")
    for bk, cal in current_metrics['calibration'].items():
        print(f"    {bk}: n={cal['n']:4}  avg_pred={cal['avg_pred']:.4f}  "
              f"actual_rate={cal['actual_rate']:.4f}  "
              f"gap={cal['avg_pred']-cal['actual_rate']:+.4f}")
    print(f"{'='*72}")

    # ── Grid search ────────────────────────────────────────────────────────
    results = []
    total   = len(BBE_THRESHOLD_CANDIDATES) * len(BLEND_CANDIDATES)
    count   = 0

    for bbe_thresh in BBE_THRESHOLD_CANDIDATES:
        for w_b, w_p, w_bar in BLEND_CANDIDATES:
            count += 1
            is_live = (bbe_thresh == LIVE_BBE_THRESHOLD and
                       (w_b, w_p, w_bar) == LIVE_BLEND)

            projections = [
                _compute_hr_prob(m, batter_power, pitcher_vuln,
                                 park_factors, bbe_thresh, w_b, w_p, w_bar)
                for m in matchups
            ]

            metrics   = _compute_metrics(matchups, projections)
            mae_delta = round(metrics["mae"] - current_metrics["mae"], 5) \
                        if metrics["mae"] else None

            results.append({
                "bbe_thresh": bbe_thresh,
                "w_b":        w_b,
                "w_p":        w_p,
                "w_bar":      w_bar,
                "metrics":    metrics,
                "mae_delta":  mae_delta,
                "is_live":    is_live,
            })

            if verbose:
                tag = " (live)" if is_live else ""
                print(f"  [{count:2}/{total}] bbe={bbe_thresh:<4} "
                      f"blend={w_b}/{w_p}/{w_bar} | "
                      f"MAE={metrics['mae']:.5f} ({mae_delta:+.5f})  "
                      f"bias={metrics['bias']:+.5f}  "
                      f"Brier={metrics['brier']:.5f}{tag}")

    # ── Results table ──────────────────────────────────────────────────────
    results.sort(key=lambda r: r["metrics"]["brier"] or 9999)

    print(f"\n{'─'*82}")
    print(f"{'BBE':<6} {'BATTER':<8} {'PITCHER':<9} {'BARREL':<8} "
          f"{'MAE':>8} {'vs LIVE':>8} {'BIAS':>8} {'BRIER':>8} {'N':>6}")
    print(f"{'─'*82}")

    for r in results:
        m    = r["metrics"]
        tag  = " ◄ BEST" if r == results[0] else ""
        live = " (live)" if r["is_live"] else ""
        d    = f"{r['mae_delta']:+.5f}" if r["mae_delta"] is not None else "  N/A "
        print(
            f"{r['bbe_thresh']:<6} "
            f"{r['w_b']:<8.2f}"
            f"{r['w_p']:<9.2f}"
            f"{r['w_bar']:<8.2f}"
            f"{m['mae']:>8.5f} "
            f"{d:>8} "
            f"{m['bias']:>+8.5f} "
            f"{m['brier']:>8.5f} "
            f"{m['n']:>6,}"
            f"{tag}{live}"
        )

    print(f"{'─'*82}")

    best = results[0]
    print(f"\nBest configuration (by Brier score):")
    print(f"  BBE threshold: {best['bbe_thresh']}")
    print(f"  Blend: batter={best['w_b']:.2f} / pitcher={best['w_p']:.2f} / "
          f"barrel={best['w_bar']:.2f}")
    print(f"  MAE delta vs live: {best['mae_delta']:+.5f}")

    # ── BBE threshold sensitivity ──────────────────────────────────────────
    print(f"\nBBE threshold sensitivity (averaged across blend configs):")
    for thresh in BBE_THRESHOLD_CANDIDATES:
        sub = [r for r in results if r["bbe_thresh"] == thresh]
        avg_mae   = sum(r["metrics"]["mae"]   for r in sub) / len(sub)
        avg_bias  = sum(r["metrics"]["bias"]  for r in sub) / len(sub)
        avg_brier = sum(r["metrics"]["brier"] for r in sub) / len(sub)
        avg_d     = sum(r["mae_delta"]        for r in sub if r["mae_delta"]) / len(sub)
        print(f"  bbe={thresh:<5}  avg MAE={avg_mae:.5f}  "
              f"avg bias={avg_bias:+.5f}  avg Brier={avg_brier:.5f}  "
              f"avg delta={avg_d:+.5f}")

    # ── Calibration of best config ─────────────────────────────────────────
    print(f"\nCalibration — best configuration "
          f"(bbe={best['bbe_thresh']}, blend={best['w_b']}/{best['w_p']}/{best['w_bar']}):")
    for bk, cal in best["metrics"]["calibration"].items():
        gap = cal['avg_pred'] - cal['actual_rate']
        flag = "  *** OVER" if gap > 0.005 else ("  *** UNDER" if gap < -0.005 else "")
        print(f"  {bk}: n={cal['n']:4}  pred={cal['avg_pred']:.4f}  "
              f"actual={cal['actual_rate']:.4f}  gap={gap:+.4f}{flag}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Grid search BBE threshold and blend weights for HR probability"
    )
    parser.add_argument("--db-path", default="data/mlb_pregame.db")
    parser.add_argument("--window",  default=WINDOW_CODE)
    parser.add_argument("--min-ab",  type=int, default=DEFAULT_MIN_AB)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    run(db_path=args.db_path, window_code=args.window,
        min_ab=args.min_ab, verbose=args.verbose)
