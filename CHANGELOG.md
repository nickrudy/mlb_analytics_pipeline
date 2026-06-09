# Changelog

All notable changes to this project are documented here.

---

## [1.0.0] — 2026-06-08

First stable release. Full pipeline operational with all three projection metrics empirically backtested and optimised against 2026 season actual outcomes.

### Added

**Projection model — Final Projection (projected_batting_avg)**
- Regression threshold τ empirically validated — τ=150 implemented (bias zero-crossing point)
- BA regression target updated from dynamic league average (~0.243) to fixed 0.22 (observed small-sample 2026 BA)
- Baseline split weighting backtested — 30/70 batter/pitcher implemented (pitcher splits more stable early-season)
- Blend weights backtested across 24 configurations — 70/20/10 (baseline/PT/zone) implemented

**Projection model — Projected Total Bases**
- SLG regression target — separate constant (SLG_REGRESSION_TARGET=0.380) implemented, distinct from BA target
- SLG baseline updated from batter-only to 30/70 batter/pitcher blend incorporating slugging_pct_allowed
- SLG blend weights backtested — 70/20/10 implemented, consistent with BA finding
- AB/game estimate replaced season-average with empirically derived lineup-slot-based values (n=820–851 games/slot)

**Projection model — Projected HR Probability**
- LEAGUE_AVG_HR_PER_PA updated from 0.034 to 0.0293 (observed 2026 rate)
- LEAGUE_AVG_BARREL_RATE updated from 0.076 to 0.0708 (observed 2026 corrected barrel rate)
- MIN_BBE_THRESHOLD updated from 20 to 100
- Blend weights updated from 45/35/20 to 70/20/10 (batter/pitcher/barrel)
- Output converted from per-PA probability (~0.02–0.08) to per-game probability (~0.05–0.20)

**Pipeline infrastructure**
- `build_matchups()` fallback chain — never drops a game due to missing pitcher data; resolves pitcher hand from dim_players when NULL; full degradation from baseline-only to regression target
- Wind direction incorporated in weather adjustment formula (cosine of wind_dir − 180°)
- Park HR factors seeded for all 30 MLB venues in dim_venues (hand-specific LHB/RHB factors)
- `fact_batter_power_profile` and `fact_pitcher_hr_vulnerability` tables added
- `fact_player_game_results` table added — actual game outcomes as backtesting ground truth
- `ingest_boxscores.py` — populates fact_player_game_results from MLB Stats API boxscores
- `hc_x`, `hc_y` columns added to stg_statcast_pitches for pull/oppo rate calculations
- Corrected barrel formula — expanding angle window replacing flat 26–30° definition

**Scheduler improvements**
- REFRESH_INTERVAL_MINUTES reduced from 45 to 30
- TRAIL_MINUTES_LAST_GAME reduced from 60 to 30
- Team-level visibility — expected teams per slot, pulled teams per run, MISSING TEAMS flag
- Double-export bug fixed — standalone export_to_sheets.py calls removed from run_lineup_refresh()
- Game list displays "away @ home" format with CT start times

**Tableau dashboard**
- `Is Latest Pitcher` calculated field — handles pitcher scratches by FIXED MAX on pitcher name
- `Game Time (CT)` calculated field — UTC to CDT conversion
- `Game Time Display` string field — h:mm AM/PM format for table display
- Three filtered views added: FL_MatchupProfile, FL_TotalBases, FL_HRUpside
- HR Edge display format updated to percentage

**Backtesting infrastructure**
- `backtest/backtest_final_projection.py` — BA grid search (τ × regression target)
- `backtest/backtest_baseline_split.py` — batter/pitcher weighting grid search
- `backtest/backtest_blend_weights.py` — blend weight grid search (24 → 15 extended configs)
- `backtest/backtest_total_bases.py` — SLG target × blend weight × AB/game validation
- `backtest/backtest_hr_probability.py` — BBE threshold × blend weight × calibration

**Documentation**
- `ARCHITECTURE.md` — full schema, pipeline flow, projection formulas, data source characteristics
- `BACKTESTING.md` — methodology, all results tables, cross-metric findings, re-evaluation SQL
- `CHANGELOG.md` — this file
- `docs/refinement_log.docx` — detailed backtesting record with decision rationale for each parameter change

### Performance Summary (v1.0 vs original baseline)

| Metric | Original MAE/Brier | v1.0 MAE/Brier | Improvement |
|--------|-------------------|----------------|-------------|
| Projected BA | 0.19585 MAE | 0.18954 MAE | −0.00631 |
| Projected TB | 1.3678 MAE | 1.3392 MAE | −0.0286 |
| HR Probability | 0.13740 Brier | 0.13511 Brier | −0.00229 |

Bias improvements: BA +0.02179 → +0.00399 · SLG Bias_SLG +0.0343 → −0.0001 · HR −0.132 → −0.051

---

## [0.9.0] — 2026-05-01 (approximate)

Initial working pipeline. All core infrastructure in place but projection parameters set by judgment rather than backtesting.

### Added
- `run_pipeline.py` orchestrator with 7-step pipeline
- `scheduler.py` game-aware daily scheduler
- Full star schema database (`data/mlb_pregame.db`) with 20+ tables
- Statcast ingestion via pybaseball
- MLB Stats API ingestion — schedule, lineups, rosters, hand splits
- Open-Meteo weather ingestion
- `transform_splits.py` — all batter/pitcher split fact tables
- `compute_match_scores.py` — initial BA projection (τ=20, blend=40/35/25)
- `export_to_sheets.py` — Google Sheets export
- Tableau Public dashboard with live Sheets connection
- `.gitignore`, `.env.template`
