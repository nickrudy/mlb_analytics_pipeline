# MLB Pre-Game Analytics Pipeline — Architecture

**Project path:** `C:\Python310\Projects\mlb_model_2026\`
**Stack:** Python 3.10 · SQLite · pybaseball · MLB Stats API · Open-Meteo · gspread · Tableau Public
**Last updated:** June 8, 2026

---

## Pipeline Flow

`run_pipeline.py` orchestrates all steps in sequence.

```
Step 1   init_db.py                          Schema DDL, seed dimensions
Step 2   ingest_mlb_statsapi.py              Schedule, probable pitchers, lineups, rosters
Step 2b  ingest_batter_splits_statsapi.py    Hand splits from MLB Stats API (~2-4hr lag)
                                              → writes fact_batter_hand_splits
                                              → refreshes matchup baseline averages
Step 3   ingest_statcast.py                  Pitch-level Statcast via pybaseball (24-48hr lag)
                                              → writes stg_statcast_pitches
Step 4   ingest_weather.py                   Open-Meteo forecast per venue
                                              → writes fact_game_weather
Step 5   transform_splits.py                 Aggregates stg_statcast_pitches into all
                                              fact_batter_* and fact_pitcher_* tables
                                              Builds fact_matchup_batter_pitcher rows
                                              via build_matchups() with full fallback chain
Step 6   compute_match_scores.py             Computes all scores and projections:
                                              projected_batting_avg, projected_total_bases,
                                              projected_hr_probability (per game),
                                              pt_slg_score, zone_slg_score
Step 7   export_to_sheets.py                 Writes 28-column output to Google Sheet
```

**Common commands:**
```powershell
python scheduler.py                                                   # Daily scheduler
python scheduler.py --dry-run                                         # Preview schedule
python run_pipeline.py --today --db-path data/mlb_pregame.db          # Full pipeline
python run_pipeline.py --today --skip-statcast --db-path data/mlb_pregame.db
python ingest/compute_match_scores.py --today --db-path data/mlb_pregame.db
python ingest/export_to_sheets.py --today --db-path data/mlb_pregame.db
```

---

## Scheduler

`scheduler.py` runs autonomously once launched. Stop with Ctrl+C.

- Runs full pipeline (with Statcast) at startup
- Queries `fact_games.game_datetime_utc` for today's first pitch times
- Logs the full game schedule with team pairings at startup
- Builds dynamic refresh schedule: 2hrs before first game, every 30min, stops 30min before last game
- Each interval: logs expected teams, runs `run_pipeline --skip-statcast`, logs pulled teams, flags missing teams
- Logs to `logs/scheduler.log`

```python
LEAD_MINUTES_FIRST_GAME  = 120   # begin refreshes 2hrs before first pitch
REFRESH_INTERVAL_MINUTES = 30    # refresh cadence (minutes)
TRAIL_MINUTES_LAST_GAME  = 30    # stop 30min before last first pitch
```

---

## Database Schema

**Database:** `data/mlb_pregame.db` (SQLite, WAL mode)
**Note:** `PRAGMA foreign_keys=OFF` used throughout — snapshot tables are inserted/replaced independently; FK enforcement would require strict insertion ordering that conflicts with the pipeline's modular step structure.

### Dimension Tables

| Table | Grain | Key Columns |
|---|---|---|
| `dim_teams` | One row per team | `team_id`, `team_abbr`, `team_name` |
| `dim_players` | One row per player | `player_id`, `full_name`, `bats`, `throws`, `primary_position`, `current_team_id`, `active_flag` |
| `dim_venues` | One row per ballpark | `venue_id`, `venue_name`, `lat`, `lon`, `park_run_factor`, `park_hr_factor_rhb`, `park_hr_factor_lhb` |
| `dim_pitch_types` | One row per pitch type | `pitch_type_code`, `pitch_type_name`, `pitch_group` |
| `dim_zones` | One row per zone | `zone_code`, `zone_name`, `in_strike_zone_flag` |
| `dim_split_windows` | One row per time window | `window_code`, `regression_weight` |

Park HR factors are seeded for all 30 MLB venues. Hand-specific LHB/RHB factors used in HR probability calculation.

### Staging Tables

| Table | Grain | Key Columns |
|---|---|---|
| `stg_mlb_schedule_games` | One row per scheduled game | `game_pk`, `game_date`, `home_team_id`, `away_team_id` |
| `stg_statcast_pitches` | One row per pitch | `game_pk`, `game_date`, `pitcher_id`, `batter_id`, `pitch_type_code`, `zone`, `release_speed`, `launch_speed`, `launch_angle`, `events`, `description`, `estimated_ba_using_speedangle`, `estimated_woba_using_speedangle`, `hc_x`, `hc_y` |
| `stg_weather_hourly` | One row per venue per hour | `venue_id`, `valid_time`, `temperature_f`, `wind_speed_mph`, `wind_direction_deg` |

### Game + Lineup Facts

| Table | Grain | Key Columns |
|---|---|---|
| `fact_games` | One row per game per date | `game_id`, `as_of_date`, `game_datetime_utc`, `home_team_id`, `away_team_id`, `venue_id` |
| `fact_game_lineups` | One row per batter per game per date | `as_of_date`, `game_id`, `player_id`, `team_id`, `lineup_slot`, `opponent_pitcher_id`, `opponent_pitcher_hand` |
| `fact_game_weather` | One row per game per date | `as_of_date`, `game_id`, `temperature_f`, `wind_speed_mph`, `wind_direction_deg` |
| `fact_player_game_results` | One row per batter per game | `game_date`, `game_id`, `player_id`, `at_bats`, `hits`, `home_runs`, `total_bases`, `batting_avg`, `hr_flag`, `lineup_slot` |

`fact_player_game_results` is populated by `ingest_boxscores.py` and serves as ground truth for all backtesting.

### Batter Split Facts

All batter split tables are keyed by `(as_of_date, player_id, window_code)` — each daily pipeline run writes a complete snapshot, enabling point-in-time backtesting.

| Table | Grain | Key Columns |
|---|---|---|
| `fact_batter_overall` | Batter × date × window | `batting_avg`, `slugging_pct`, `at_bats`, `ab_per_game` |
| `fact_batter_hand_splits` | Batter × pitcher hand × date × window | `split_hand`, `batting_avg`, `slugging_pct`, `on_base_pct`, `plate_appearances` |
| `fact_batter_pitch_type_splits` | Batter × pitch type × pitcher hand × date × window | `pitch_type_code`, `split_hand`, `batting_avg`, `slugging_pct`, `pitches_seen`, `total_bases` |
| `fact_batter_zone_splits` | Batter × zone × pitcher hand × date × window | `zone_code`, `split_hand`, `batting_avg`, `slugging_pct`, `pitches_seen` |
| `fact_batter_power_profile` | Batter × date × window | `hr_per_pa`, `barrels_per_pa`, `barrels_per_pa_vs_rhp`, `barrels_per_pa_vs_lhp`, `hard_hit_rate_vs_rhp`, `hard_hit_rate_vs_lhp`, `batted_ball_events` |

### Pitcher Split Facts

| Table | Grain | Key Columns |
|---|---|---|
| `fact_pitcher_overall` | Pitcher × date × window | `hits_allowed`, `xba_allowed`, `whiff_rate` |
| `fact_pitcher_hand_splits` | Pitcher × batter hand × date × window | `split_hand`, `batting_avg_allowed`, `slugging_pct_allowed`, `k_rate`, `batters_faced` |
| `fact_pitcher_pitch_mix` | Pitcher × pitch type × batter hand × date × window | `pitch_type_code`, `split_hand`, `usage_pct`, `pitches_thrown`, `avg_velocity`, `batting_avg_allowed`, `whiff_rate` |
| `fact_pitcher_zone_profile` | Pitcher × zone × batter hand × date × window | `zone_code`, `split_hand`, `pitches_thrown` |
| `fact_pitcher_hr_vulnerability` | Pitcher × batter hand × date × window | `split_hand`, `hr_per_bf_allowed`, `barrel_rate_allowed`, `batted_ball_events` |

### Central Output Table

**`fact_matchup_batter_pitcher`** — one row per batter-pitcher pairing per game per date per window

| Column Group | Columns |
|---|---|
| Primary key | `as_of_date`, `game_id`, `batter_id`, `pitcher_id`, `window_code` |
| Team context | `team_id`, `opponent_team_id` |
| Baseline splits | `batter_vs_hand_batting_avg`, `pitcher_vs_hand_batting_avg_allowed`, `batter_vs_hand_woba`, `pitcher_vs_hand_k_rate` |
| Adjustments | `park_adjustment_factor`, `weather_adjustment_factor` |
| BA scores | `pitch_type_match_score`, `zone_match_score` |
| SLG scores | `pt_slg_score`, `zone_slg_score`, `projected_slugging` |
| Output projections | `projected_batting_avg`, `projected_total_bases`, `proj_at_bats_per_game`, `projected_hr_probability` |
| Power context | `batter_barrel_rate`, `pitcher_barrel_rate_allowed` |

---

## Projection Model

### Projected Batting Average

**Step 1 — Baseline (30/70 batter/pitcher split):**
```
b_avg    = batter BA vs pitcher hand  (regressed toward 0.22 if pitches < τ)
p_avg    = pitcher BA-allowed vs batter hand  (same regression)
baseline = (b_avg × 0.30) + (p_avg × 0.70)
```

**Step 2 — Pitch type match score:**
```
PT_score = Σ (usage_pct / Σusage) × BA*_batter,pitch_type,hand
           (NULL if total coverage < 0.25)
```

**Step 3 — Zone match score:**
```
Z_score = Σ (pitches_in_zone / total_pitches) × BA*_batter,zone,hand
          (NULL if coverage < 0.25)
```

**Step 4 — Blend (backtested optimal weights):**
```
if PT and Z:    blended = (baseline × 0.70) + (PT × 0.20) + (Z × 0.10)
if PT only:     blended = (baseline × 0.80) + (PT × 0.20)
if Z only:      blended = (baseline × 0.90) + (Z × 0.10)
if neither:     blended = baseline

projected_batting_avg = blended × park_adj × weather_adj
```

Regression: `regressed = observed × (n/τ) + target × (1 - n/τ)` where τ=150, target=0.22.

### Projected Total Bases

```
projected_slugging     = same blend structure as BA using SLG components
                         (SLG regression target = 0.380, blend weights = 70/20/10)
                         × park_hr_factor × weather_adj

proj_at_bats_per_game  = SLOT_AB[lineup_slot]  (empirical 2026 slot averages)
                         fallback: historical ab_per_game → 3.502

projected_total_bases  = projected_slugging × proj_at_bats_per_game
```

Empirical slot AB averages: `{1:3.888, 2:3.781, 3:3.708, 4:3.652, 5:3.549, 6:3.456, 7:3.339, 8:3.113, 9:3.031}`

### Projected HR Probability (per game)

```
batter_hr_rate   = hr_per_pa from fact_batter_power_profile
                   (regressed toward 0.0293 if BBE < 100)
pitcher_hr_rate  = hr_per_bf_allowed from fact_pitcher_hr_vulnerability
                   (same regression)
barrel_context   = LEAGUE_AVG_HR_PA × (batter_bpp / 0.0708) × (pitcher_bpp / 0.0708)

blended_per_pa   = (batter_hr_rate × 0.70) + (pitcher_hr_rate × 0.20) + (barrel_context × 0.10)

projected_hr_probability = blended_per_pa × ab_per_game × park_hr_factor × weather_adj
```

Output is per-game probability (typical range 0.05–0.20).

### Adjustment Factors

```
weather_adj = 1.0 + temp_adj × wind_adj
  temp_adj  = clamp((temp_f - 70) × 0.001, -0.05, +0.05)
  wind_adj  = clamp(cos(wind_dir - 180°) × wind_speed × (0.02/15), -0.03, +0.03)

park_adj    = park_run_factor (from dim_venues, all 30 venues seeded)
park_hr_adj = park_hr_factor_rhb or _lhb (hand-specific, normalized from integer to multiplier)
```

---

## build_matchups() Fallback Chain

`build_matchups()` in `transform_splits.py` never drops a game due to missing pitcher data:

1. Full data available → baseline + pt_score + zone_score
2. Pitcher hand NULL in lineup → falls back to `dim_players.throws` lookup
3. No pitcher Statcast data → baseline only (pt/zone = NULL)
4. No pitcher hand splits → batter split only
5. No batter data either → REGRESSION_TARGET (0.22) for both components

---

## Google Sheets Export

**Sheet:** `mlb_projections`
**Credentials:** `config/sheets_credentials.json`
**Sheet ID:** stored in `.env` as `GOOGLE_SHEET_ID`

**28 export columns:**
`as_of_date`, `game_datetime_utc`, `home_team`, `away_team`, `batter_team`, `batter_name`, `bats`, `pitcher_name`, `pitcher_hand`, `window_code`, `baseline_avg`, `pt_score`, `zone_score`, `final_projection`, `delta`, `park_adjustment_factor`, `weather_adjustment_factor`, `temperature_f`, `wind_speed_mph`, `wind_direction_deg`, `proj_at_bats_per_game`, `pt_slg_score`, `zone_slg_score`, `projected_slugging`, `projected_total_bases`, `projected_hr_probability`, `batter_barrel_rate`, `pitcher_barrel_rate_allowed`

`delta` = `final_projection - baseline_avg` (matchup edge vs handedness baseline).

---

## Tableau Dashboard

**Workbook:** `tableau/mlb_dash_v1.twb` (Tableau Public)
**Connection:** Live Google Sheets (manual F5 refresh required — Tableau Public has no auto-refresh)

**Key calculated fields:**
- `Is Latest Pitcher`: `[pitcher_name] = { FIXED [batter_team], [home_team], [away_team] : MAX([pitcher_name]) }` — handles pitcher scratches
- `Game Time (CT)`: `DATEADD('hour', -5, DATEPARSE("yyyy-MM-dd'T'HH:mm:ss'Z'", [game_datetime_utc]))` — UTC-5 CDT conversion

**Dashboard views:** Top Batter/Pitcher Matchups · Top Projected Bases · Top HR Upside · Full League View

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
- L30D/L14D/L7D window matchup rows are not currently built — only SEASON window
- DB Browser for SQLite is the primary SQL tool; paste via keyboard only (CP1252 encoding issues with clipboard paste cause red highlighting errors)
- Stale `__pycache__` can cause silent module resolution failures after file replacements — clear as first debug step
- `scheduler.py` must remain running in the terminal (Ctrl+C to stop)

---

## Active Backlog

**High priority:**
- [ ] Career/multi-year Statcast fallback for debut pitchers with no current-season data
- [ ] `ingested_at` timestamp on `fact_matchup_batter_pitcher` — enables definitive latest-pitcher filtering in Tableau without relying on alphabetical MAX()
- [ ] Supabase migration (SQLite → PostgreSQL) for cloud pipeline hosting
- [ ] GitHub Actions workflow for laptop-independent daily automation

**Medium priority:**
- [ ] Recency trend infrastructure (`fact_batter_recency_trends`) — exit velocity, chase rate, walk rate rolling windows
- [ ] Pitcher workload/fatigue signals — days of rest, recent pitch counts
- [ ] L30D/L14D matchup rows (currently only SEASON window built)
- [ ] Projected wOBA layer (xwOBA already in stg_statcast_pitches)
- [ ] Looker Studio migration (live refresh vs Tableau Public manual refresh)

**Low priority / future:**
- [ ] Re-derive empirical constants at 2026 season end (τ, regression targets, blend weights, slot AB)
- [ ] Windows Task Scheduler integration for scheduler.py
- [ ] Phase 2 cloud migration (Supabase + GitHub Actions)
