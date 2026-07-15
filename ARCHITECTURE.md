# MLB Pre-Game Analytics Pipeline — Architecture

**Project path:** `C:\Python310\Projects\mlb_model_2026\`
**Stack:** Python 3.10 · PostgreSQL (Supabase, production) / SQLite (local, dev + backtesting) · pybaseball · MLB Stats API · Open-Meteo · Looker Studio (primary viz) · gspread (secondary export) · GitHub Actions (orchestration)
**Last updated:** July 15, 2026

---

## Pipeline Flow

`run_pipeline.py` orchestrates all steps in sequence. Backend (SQLite vs. Supabase)
is controlled entirely by the `DB_BACKEND` environment variable — no `--db-path`
flag exists; connections are handled centrally in `utils/db.py`.

```
Step 1   Init DB                             SQLite: runs db/init_db.py (schema DDL + seed)
                                              Supabase: SKIPPED (schema pre-migrated via scripts/)
Step 2   ingest_mlb_statsapi.py              Schedule, probable pitchers, lineups, rosters
Step 2b  ingest_batter_splits_statsapi.py    Hand splits from MLB Stats API (~2-4hr lag)
                                              -> writes fact_batter_hand_splits
                                              -> refreshes matchup baseline averages
Step 3   ingest_statcast.py                  Pitch-level Statcast via pybaseball (24-48hr lag)
                                              -> writes stg_statcast_pitches
                                              -> bulk_upsert (single execute_values call, not
                                                 per-row inserts)
                                              SKIPPED on intraday/--skip-statcast runs
Step 4   ingest_weather.py                   Open-Meteo forecast per venue
                                              -> writes fact_game_weather
Step 5   transform_splits.py                 Aggregates stg_statcast_pitches into all
                                              fact_batter_* and fact_pitcher_* tables.
                                              Projects only the ~22 columns the aggregators
                                              actually consume (not SELECT *).
                                              Builds SEASON window ONLY by default
                                              (--windows flag can request L30D/L14D/L7D,
                                              but these are not built on a normal run --
                                              intentional, see "Known Behaviors" below).
                                              Builds fact_matchup_batter_pitcher rows via
                                              build_matchups() -- preloaded dict lookups
                                              (one query per source, not per lineup row),
                                              full fallback chain (see below).
Step 5b  Cleanup stale split data             Supabase ONLY. Deletes all non-today rows
                                              across the 10 split-fact tables + the matchup
                                              table, to control Supabase's metered storage/
                                              IO cost. Skipped entirely on SQLite, where
                                              history is intentionally kept (see "Two-Tier
                                              Data Strategy" below).
[guard]  Zero-matchup check                   If zero matchup rows exist for today after
                                              Step 5b, log and exit cleanly -- Steps 6/7/7b
                                              are skipped rather than run against empty data
                                              (this is what lets every GitHub Actions
                                              trigger, at any time of day, correctly no-op
                                              before lineups post instead of failing).
Step 6   compute_match_scores.py             Computes all scores and projections:
                                              projected_batting_avg, projected_total_bases,
                                              projected_hr_probability (per game),
                                              pt_slg_score, zone_slg_score
Step 7   export_to_sheets.py                 SECONDARY export. Writes 28-column output to
                                              Google Sheet. Non-fatal -- wrapped in try/except,
                                              logs a warning and continues on failure.
Step 7b  export_to_daily_tables.py           PRIMARY production export. Writes
                                              daily_top_batting / daily_top_bases /
                                              daily_top_hrs -- the tables Looker Studio reads
                                              directly. HARD-FATAL: aborts before truncating
                                              if all three queries return zero rows, to avoid
                                              silently blanking the live dashboards.
```

**Common commands:**
```powershell
# Local (SQLite) -- set once per terminal session:
$env:DB_BACKEND = "sqlite"
python run_pipeline.py --today
python run_pipeline.py --today --skip-statcast
python ingest/compute_match_scores.py --today

# Production (Supabase) -- DB_BACKEND + SUPABASE_DB_URL supplied via
# GitHub Actions secrets, not typically run by hand. If running manually:
$env:DB_BACKEND = "supabase"
python run_pipeline.py --today
```

---

## Orchestration — GitHub Actions (production)

`scheduler.py` is a **local, manually-run fallback** — not dead code, and not
part of normal production. It predates the Supabase/GitHub Actions migration
and implemented an always-running local process (game-aware dynamic refresh
schedule, Ctrl+C to stop) that has since been fully superseded by
`.github/workflows/daily_pipeline.yml` for day-to-day operation. It's kept
intentionally, for the specific scenario where GitHub Actions and/or Supabase
are fully unavailable and data collection needs to continue locally in the
meantime — confirmed still relevant for that purpose as of July 2026.

**Current schedule** (all times UTC in the workflow file; ~CT = UTC-5 in July):

| Trigger | Approx. CT | Job | Does |
|---|---|---|---|
| `6 11 * * *` | ~6:06 AM | `full_run` | Full pipeline incl. Statcast |
| `12 13 * * 1` | ~8:12 AM Mon | `full_run` | Weekly seed-dimensions roster refresh |
| `18 14 * * *` | ~9:18 AM | `intraday_run` | `--skip-statcast` lineup watch |
| `41 16 * * *` | ~11:41 AM | `intraday_run` | `--skip-statcast` lineup watch |
| `27 17 * * *` | ~12:27 PM | `intraday_run` | `--skip-statcast` lineup watch |
| `53 18 * * *` | ~1:53 PM | `intraday_run` | `--skip-statcast` lineup watch |
| `36 19 * * *` | ~2:36 PM | `intraday_run` | `--skip-statcast` lineup watch |
| `22 21 * * *` | ~4:22 PM | `intraday_run` | `--skip-statcast` lineup watch |
| `44 23 * * *` | ~6:44 PM | `intraday_run` | `--skip-statcast` lineup watch |

`full_run` and `intraday_run` are separate concurrency groups: `full_run` never
gets interrupted mid-run (`cancel-in-progress: false`); `intraday_run` triggers
cancel a still-running prior intraday trigger (`cancel-in-progress: true`), since
each is an idempotent, as-of-date-keyed snapshot and a newer one superseding a
stale in-flight one is fine.

A `workflow_dispatch` manual trigger exists with `run_date`, `skip_statcast`, and
`dry_run` inputs.

Every trigger before lineups post for the day (typically most of the morning ones)
correctly no-ops via the zero-matchup guard above — this is expected, not a
failure, and generates no error/email.

---

## Two-Tier Data Strategy (SQLite vs. Supabase)

As of July 2026, the two backends are **deliberately divergent**, not just a
dev/prod pair running identical logic:

- **Supabase (production)** — lean, today-only. Step 5b prunes every
  non-today row from all 11 fact/matchup tables on every full run, to control
  metered storage and disk-IO burst budget (Supabase Micro/Small compute has a
  30-minute daily burst allowance; this pruning, plus the read/write efficiency
  work below, is what keeps the pipeline within it).
- **SQLite (local)** — the historical/analytical store. Step 5b is skipped
  entirely, so every day's snapshot accumulates indefinitely. This is what
  `backtest/*.py` requires for point-in-time comparisons against
  `fact_player_game_results`.

**Known limitation:** SQLite's `bulk_upsert` path uses whole-row
`INSERT OR REPLACE` (vs. Supabase's column-targeted `ON CONFLICT ... DO UPDATE
SET`). For any table written by two different steps in sequence
(`fact_matchup_batter_pitcher`, written by both `build_matchups()` and
`compute_match_scores()`), the second writer's `INSERT OR REPLACE` on SQLite
silently resets columns it doesn't itself write — e.g. running
`compute_match_scores` after `build_matchups` locally will null out
`team_id` and the baseline split columns. **This is SQLite-only and does not
happen on Supabase.** It has caused real, since-resolved confusion more than
once — if a local matchup row is missing data that should be there, check
whether this is the cause before assuming a real bug.

---

## Database Schema

**Production:** Supabase Postgres. **Local:** `data/mlb_pregame.db` (SQLite, WAL
mode). Schema is the same shape on both; DDL originated from
`docs/design/mlb_pregame_data_dictionary_and_sql_schema.xlsx` and was migrated to
Supabase via `scripts/migrate_schema_to_supabase.py` and its two incremental
follow-ups.

**Note:** `PRAGMA foreign_keys=OFF` used throughout on SQLite — snapshot tables
are inserted/replaced independently; FK enforcement would require strict
insertion ordering that conflicts with the pipeline's modular step structure.

### Dimension Tables

| Table | Grain | Key Columns |
|---|---|---|
| `dim_teams` | One row per team | `team_id`, `team_abbr`, `team_name` |
| `dim_players` | One row per player | `player_id`, `full_name`, `bats`, `throws`, `primary_position`, `current_team_id`, `active_flag`, `mlb_debut_date` |
| `dim_venues` | One row per ballpark | `venue_id`, `venue_name`, `lat`, `lon`, `park_run_factor`, `park_hr_factor_rhb`, `park_hr_factor_lhb` |
| `dim_pitch_types` | One row per pitch type | `pitch_type_code`, `pitch_type_name`, `pitch_group` |
| `dim_zones` | One row per zone | `zone_code`, `zone_name`, `in_strike_zone_flag` |
| `dim_split_windows` | One row per time window | `window_code`, `regression_weight` |

`dim_players` refreshes weekly (Monday seed run). Players who debut or get
recalled mid-week won't have a name until the following Monday — self-heals.
As of mid-July 2026, ~8% of active batters were in this lag state at any given
time; a handful of specific player_ids were found genuinely, persistently
absent despite real season activity — root cause not yet investigated (likely a
roster-status edge case — optioned/recalled churn, IL, waiver claim — falling
outside whatever roster snapshot the seed query scopes to).

### Staging Tables

| Table | Grain | Key Columns |
|---|---|---|
| `stg_mlb_schedule_games` | One row per scheduled game | `game_pk`, `game_date`, `home_team_id`, `away_team_id` |
| `stg_statcast_pitches` | One row per pitch | `game_pk`, `game_date`, `pitcher_id`, `batter_id`, `pitch_type_code`, `zone`, `release_speed`, `release_spin_rate`, `release_extension`, `release_pos_x`, `release_pos_z`, `pfx_x`, `pfx_z`, `plate_x`, `plate_z`, `launch_speed`, `launch_angle`, `events`, `description`, `estimated_ba_using_speedangle`, `estimated_woba_using_speedangle`, `hc_x`, `hc_y` |
| `stg_weather_hourly` | One row per venue per hour | `venue_id`, `valid_time`, `temperature_f`, `wind_speed_mph`, `wind_direction_deg` |

### Game + Lineup Facts

| Table | Grain | Key Columns |
|---|---|---|
| `fact_games` | One row per game per date | `game_id`, `as_of_date`, `game_datetime_utc`, `home_team_id`, `away_team_id`, `venue_id` |
| `fact_game_lineups` | One row per batter per game per date | `as_of_date`, `game_id`, `player_id`, `team_id`, `lineup_slot`, `opponent_pitcher_id`, `opponent_pitcher_hand`, `confirmed_flag`, `projected_flag` |
| `fact_game_weather` | One row per game per date | `as_of_date`, `game_id`, `temperature_f`, `wind_speed_mph`, `wind_direction_deg` |
| `fact_player_game_results` | One row per batter per game | `game_date`, `game_id`, `player_id`, `at_bats`, `hits`, `home_runs`, `total_bases`, `batting_avg`, `hr_flag`, `lineup_slot` |

`fact_player_game_results` is populated by `ingest/ingest_boxscores.py` and
serves as ground truth for all backtesting. **Not part of the automated daily
cadence** — run manually/periodically (`--last-n-days`, `--start`/`--end`,
`--season`) to keep it current for backtesting.

Early in the day, some lineups post before the opposing pitcher is officially
confirmed — `opponent_pitcher_id`/`opponent_pitcher_hand`/`team_id` can be
temporarily null for those rows, resolving once MLB confirms (usually well
before first pitch). Not a bug; confirmed via direct observation July 2026.

The 2026 All-Star Game uses synthetic, non-franchise `team_id` values (159=AL,
160=NL) that don't exist in `dim_teams` — any exports that inner-join on
`team_id` will correctly, silently exclude that one game's rows. Once-a-year,
not worth special-casing.

### Batter Split Facts

All batter split tables are keyed by `(as_of_date, player_id, window_code)`.
On Supabase, only the current day's row exists per key (Step 5b prunes older
ones). On SQLite, every day's snapshot accumulates, enabling point-in-time
backtesting (see "Two-Tier Data Strategy" above).

| Table | Grain | Key Columns |
|---|---|---|
| `fact_batter_overall` | Batter × date × window | `batting_avg`, `slugging_pct`, `at_bats`, `ab_per_game`, `games_played` |
| `fact_batter_hand_splits` | Batter × pitcher hand × date × window | `split_hand`, `batting_avg`, `slugging_pct`, `on_base_pct`, `plate_appearances`, `xba`, `xwoba`, `contact_rate` |
| `fact_batter_pitch_type_splits` | Batter × pitch type × pitcher hand × date × window | `pitch_type_code`, `split_hand`, `batting_avg`, `slugging_pct`, `pitches_seen`, `total_bases` |
| `fact_batter_zone_splits` | Batter × zone × pitcher hand × date × window | `zone_code`, `split_hand`, `batting_avg`, `slugging_pct`, `pitches_seen` |
| `fact_batter_power_profile` | Batter × date × window | `hr_per_pa`, `barrels_per_pa`, `barrels_per_pa_vs_rhp`, `barrels_per_pa_vs_lhp`, `hard_hit_rate_vs_rhp`, `hard_hit_rate_vs_lhp`, `batted_ball_events` |

~2.5% of active batters are missing at least one hand's split at any given
time (genuinely thin sample — rarely-used bench players, recent callups,
extreme platoon usage). Handled gracefully by the regression fallback chain
below; not a bug.

### Pitcher Split Facts

| Table | Grain | Key Columns |
|---|---|---|
| `fact_pitcher_overall` | Pitcher × date × window | `hits_allowed`, `xba_allowed`, `whiff_rate` |
| `fact_pitcher_hand_splits` | Pitcher × batter hand × date × window | `split_hand`, `batting_avg_allowed`, `slugging_pct_allowed`, `k_rate`, `batters_faced` |
| `fact_pitcher_pitch_mix` | Pitcher × pitch type × batter hand × date × window | `pitch_type_code`, `split_hand`, `usage_pct`, `pitches_thrown`, `avg_velocity`, `batting_avg_allowed`, `whiff_rate` |
| `fact_pitcher_zone_profile` | Pitcher × zone × batter hand × date × window | `zone_code`, `split_hand`, `pitches_thrown` |
| `fact_pitcher_hr_vulnerability` | Pitcher × batter hand × date × window | `split_hand`, `hr_per_bf_allowed`, `barrel_rate_allowed`, `batted_ball_events` |

### Central Output Table

**`fact_matchup_batter_pitcher`** — one row per batter-pitcher pairing per game
per date per window

| Column Group | Columns |
|---|---|
| Primary key | `as_of_date`, `game_id`, `batter_id`, `pitcher_id`, `window_code` |
| Metadata | `ingested_at` (write timestamp — latest wins in dedup logic, e.g. for pitcher scratches) |
| Team context | `team_id`, `opponent_team_id` |
| Baseline splits | `batter_vs_hand_batting_avg`, `pitcher_vs_hand_batting_avg_allowed`, `batter_vs_hand_woba`, `pitcher_vs_hand_k_rate` |
| Adjustments | `park_adjustment_factor`, `weather_adjustment_factor` |
| BA scores | `pitch_type_match_score`, `zone_match_score` |
| SLG scores | `pt_slg_score`, `zone_slg_score`, `projected_slugging` |
| Output projections | `projected_batting_avg`, `projected_total_bases`, `proj_at_bats_per_game`, `projected_hr_probability` |
| Power context | `batter_barrel_rate`, `pitcher_barrel_rate_allowed` |

---

## Projection Model

All constants below are confirmed against `BACKTESTING.md`'s "Implemented
Parameters" tables and the live `compute_match_scores.py` source (checked
July 2026) — not carried forward from an older doc unverified. Note: the
`backtest_baseline_split.py` and `backtest_blend_weights.py` scripts' own
hardcoded "LIVE" constants (40/60 and 0.40/0.35/0.25) do *not* reflect
current production values — those are frozen pre-test comparison baselines
from when each optimization round was designed, not current-truth labels.
Trust `BACKTESTING.md` and the live code over any individual backtest
script's docstring.

### Projected Batting Average

**Step 1 — Baseline (30/70 batter/pitcher split):**
```
b_avg    = batter BA vs pitcher hand  (regressed toward 0.22 if pitches < tau)
p_avg    = pitcher BA-allowed vs batter hand  (same regression)
baseline = (b_avg x 0.30) + (p_avg x 0.70)
```

**Step 2 — Pitch type match score:**
```
PT_score = Sum (usage_pct / Sum usage) x BA*_batter,pitch_type,hand
           (NULL if total coverage < 0.25)
```

**Step 3 — Zone match score:**
```
Z_score = Sum (pitches_in_zone / total_pitches) x BA*_batter,zone,hand
          (NULL if coverage < 0.25)
```

**Step 4 — Blend:**
```
if PT and Z:    blended = (baseline x 0.70) + (PT x 0.20) + (Z x 0.10)
if PT only:     blended = (baseline x 0.80) + (PT x 0.20)
if Z only:      blended = (baseline x 0.90) + (Z x 0.10)
if neither:     blended = baseline

projected_batting_avg = blended x park_adj x weather_adj
```

Regression: `regressed = observed x (n/tau) + target x (1 - n/tau)` where tau=150,
target=0.22.

### Projected Total Bases

```
projected_slugging     = same blend structure as BA using SLG components
                         (SLG regression target = 0.380, blend weights = 70/20/10)
                         x park_hr_factor x weather_adj

proj_at_bats_per_game  = SLOT_AB[lineup_slot]  (empirical 2026 slot averages)
                         fallback: historical ab_per_game -> 3.502

projected_total_bases  = projected_slugging x proj_at_bats_per_game
```

Empirical slot AB averages:
`{1:3.888, 2:3.781, 3:3.708, 4:3.652, 5:3.549, 6:3.456, 7:3.339, 8:3.113, 9:3.031}`

### Projected HR Probability (per game)

```
batter_hr_rate   = hr_per_pa from fact_batter_power_profile
                   (regressed toward 0.0293 if BBE < 100)
pitcher_hr_rate  = hr_per_bf_allowed from fact_pitcher_hr_vulnerability
                   (same regression)
barrel_context   = LEAGUE_AVG_HR_PA x (batter_bpp / 0.0708) x (pitcher_bpp / 0.0708)

blended_per_pa   = (batter_hr_rate x 0.70) + (pitcher_hr_rate x 0.20) + (barrel_context x 0.10)

projected_hr_probability = blended_per_pa x ab_per_game x park_hr_factor x weather_adj
```

Output is per-game probability (typical range 0.05–0.20).

**Note on `LEAGUE_AVG_BA` (0.243, in `compute_match_scores.py`):** unlike
`LEAGUE_AVG_HR_PER_PA`/`LEAGUE_AVG_BARREL_RATE` above, this constant does
*not* appear in `BACKTESTING.md` and was never part of the formal
backtesting process. It's used exactly once, as a fallback for
`dynamic_league_ba` — a query that computes a fresh, `as_of_date`-scoped
league BA from `fact_batter_overall` on every run (the same query fixed for
a missing date filter earlier this refactor). Since that table is always
populated in normal operation, this fallback essentially never fires in
practice — it is not a load-bearing daily value, just an unreachable-in-
practice safety net. Cosmetic-only backlog item at most (a direct query
recently measured the real dynamic value at ~0.240, close to the constant
already).

### Adjustment Factors

```
weather_adj = 1.0 + temp_adj x wind_adj
  temp_adj  = clamp((temp_f - 70) x 0.001, -0.05, +0.05)
  wind_adj  = clamp(cos(wind_dir - 180deg) x wind_speed x (0.02/15), -0.03, +0.03)

park_adj    = park_run_factor (from dim_venues, all 30 venues seeded)
park_hr_adj = park_hr_factor_rhb or _lhb (hand-specific, normalized from integer to multiplier)
```

**Not yet in production, validated and scoped for integration** (see Active
Backlog): a hard-hit-rate-based recency signal for `projected_total_bases`,
reliability-shrinkage weighted (measured N=10-game-block lag-1 autocorrelation
approx 0.36) against last-10-games vs. prior-season hard-hit rate, with
sample-size floors on both windows (min 12 last-10 batted balls, min 30 prior
batted balls). Fully designed and validated locally across three iterations;
not yet wired into `compute_match_scores.py`. **Must fold into the existing
single SEASON pitch read rather than adding a second full-table scan** — this
is a hard architectural constraint, not a preference, given the IO-cost work
this whole pipeline has been built around.

---

## build_matchups() Fallback Chain

`build_matchups()` in `transform_splits.py` never drops a game due to missing
pitcher data. Implemented via dicts preloaded once per data source (not a
per-lineup-row SQL query — this was a significant IO-reduction rewrite; the
fallback *behavior* below is unchanged and was verified identical to the
prior per-row-query implementation via direct old-code-vs-new-code comparison
on live Supabase data):

1. Full data available -> baseline + pt_score + zone_score
2. Pitcher hand NULL in lineup -> falls back to `dim_players.throws` lookup
3. No pitcher Statcast data -> baseline only (pt/zone = NULL)
4. No pitcher hand splits -> batter split only
5. No batter data either -> REGRESSION_TARGET (0.22) for both components

---

## Exports

### Primary — Supabase daily flat tables (Looker Studio)

`ingest/export_to_daily_tables.py`, Step 7b. Writes `daily_top_batting`,
`daily_top_bases`, `daily_top_hrs` — simple flat tables with no joins,
designed for direct Looker Studio consumption. **Hard-fatal**: if all three
queries return zero rows, aborts *before* truncating, to avoid blanking live
dashboards on an upstream data gap. Looker Studio connects via a direct
PostgreSQL connection (not the Supabase Data API/PostgREST) using **Owner's
Credentials**, so it authenticates once and isn't affected by Row-Level
Security policy state on the underlying tables (the `postgres` role bypasses
RLS entirely). The Supabase Data API (PostgREST/REST layer) is **disabled** at
the project level as of July 2026, since nothing in this project uses it —
Row-Level Security is additionally enabled on every public table as a
defense-in-depth backstop should the Data API ever be re-enabled.

### Secondary — Google Sheets

`ingest/export_to_sheets.py`, Step 7. 28-column export, non-fatal on failure.
**[VERIFY: confirm current consumers of this sheet, if any — it may be
legacy from the pre-Looker era.]**

---

## Data Source Characteristics

| Source | Lag | Notes |
|--------|-----|-------|
| MLB Stats API — schedule/lineups | ~0 min | Real-time as lineups post (typically 2-3hrs pre-game) |
| MLB Stats API — hand splits | ~2-4 hrs | Step 2b closes the gap vs Statcast for baseline_avg |
| Baseball Savant — Statcast | 24-48 hrs | Pitch mix and zone profiles; acceptable since tendencies don't shift game-to-game |
| Open-Meteo — weather | Real-time | 7-day hourly forecast, no auth required |

---

## Known Behaviors

- Statcast lags 24-48hrs; MLB Stats API hand splits (Step 2b) close this gap for baseline_avg
- Pitchers with no current-season Statcast data fall back through the `build_matchups()` chain gracefully — baseline-only projection still produced
- Matchup rows are only built for confirmed lineups; probable-pitcher-only games (no lineup yet) produce rows once the lineup posts on the next refresh cycle
- Only the SEASON window is built by default (L30D/L14D/L7D removed from the default in July 2026 — they were computed but never consumed downstream; still buildable via `--windows` if ever needed)
- The Step 5b stale-data cleanup only runs on Supabase — local SQLite intentionally accumulates full history (see "Two-Tier Data Strategy")
- The SQLite whole-row-replace clobber on multi-writer tables (see "Two-Tier Data Strategy") has caused real confusion more than once — check this before assuming a local matchup data gap is a real bug
- `__pycache__` can cause silent module resolution failures after file replacements — clear as first debug step

---

## Local Development / Analytical Tooling

Two folders exist purely for local, non-production use — both gitignored,
neither part of the deployed pipeline:

- **`testing/`** — ad-hoc query scripts, a local SQLite-backed fallback
  dashboard tool (`daily_board.py`, appends a running xlsx log of top
  projections — built during a Supabase platform outage in July 2026 and kept
  as a standing casual-use tool), and one-off diagnostic scripts.
- Historical design/reference material lives in **`docs/design/`** (the
  original schema data dictionary) and **`docs/research/`** (exploratory
  metrics research), and empirical model-tuning history in **`backtest/logs/`**
  — all tracked, not gitignored, since they're genuine project documentation
  rather than scratch work.

---

## Active Backlog

**High priority:**
- [ ] Recency-weighted `projected_total_bases` integration (hard-hit-rate
      signal, fully validated and scoped — see "Projection Model" above).
      Reprioritized ahead of the SQLite historical backfill below, since it's
      an immediate improvement to live production data.
- [ ] Walk-rate-based enhancement to the Top Batters selection metric
      (currently `projected_batting_avg`, which doesn't credit walks or
      account for plate-appearance volume by lineup slot — `proj_at_bats_per_game`
      is already computed but not currently factored into batter selection).
- [ ] Career/multi-year Statcast fallback for debut pitchers with no current-season data

**Medium priority:**
- [ ] SQLite historical backfill/reconstruction — regenerate point-in-time
      projection snapshots for the multi-week gap where Step 5b was pruning
      local history too (fixed July 2026; history accumulates going forward,
      but the past gap needs a deliberate, larger reconstruction effort:
      point-in-time-correct re-runs per historical date, a real question about
      whether historical lineups are reconstructable at all vs. using
      boxscore actuals, and a historical-weather-API gap). Deliberately
      deferred behind the recency-weighting work above.
- [ ] Investigate root cause of the small number of `dim_players` entries
      that are persistently (not just weekly-lag) missing despite real
      season activity
- [ ] Pitcher workload/fatigue signals — days of rest, recent pitch counts
- [ ] Projected wOBA layer (xwOBA already in stg_statcast_pitches)
- [ ] Looker Studio mobile session/re-authentication issue — investigated at
      length (credentials mode, Google account permissions) without a
      conclusive fix; likely a native-PostgreSQL-connector limitation.
      Decision pending between accepting it or migrating to a directly-
      Postgres-connected BI tool (Metabase) as a follow-on.

**Low priority / future:**
- [ ] Re-derive empirical constants (tau, regression targets, blend weights,
      slot AB) — a backtest re-run against ~7 weeks of accumulated
      `fact_player_game_results` ground truth is planned for mid-season 2026,
      not season-end as originally planned
- [ ] Resolve remaining `[VERIFY]` items in this document (Google Sheets
      export consumers; postmortem file's final location)

---

## Recently Completed (July 2026)

For context on why the architecture looks the way it does: a multi-day Disk
IO exhaustion incident (Supabase burst budget saturation from IO-inefficient
read/write patterns, compounded by an unrelated concurrent Supabase platform
outage) drove a full refactor — see `POSTMORTEM_io_outage_refactor.md`
**[VERIFY: confirm final location of this file]** for the complete writeup.
Summary of what changed: Supabase migration, GitHub Actions orchestration,
Looker Studio migration, `ingested_at` on matchup writes, the SEASON-only
window default, projected (not `SELECT *`) pitch reads, batched Statcast
inserts, de-N+1'd matchup building, the Step 5b Supabase-only gate, the
zero-matchup early-exit guard, and Row-Level Security + Data API hardening.
