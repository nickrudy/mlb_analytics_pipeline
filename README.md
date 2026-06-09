# MLB Pre-Game Analytics Pipeline

Player-level pre-game matchup projections using free public data sources. Generates per-batter projected batting average, total bases, and HR probability for every confirmed lineup, exported daily to Google Sheets and visualized in Looker Studio / Tableau.

---

## What It Does

Each morning the pipeline:

1. Fetches today's schedule, lineups, and probable pitchers from the MLB Stats API
2. Pulls pitch-by-pitch Statcast data via pybaseball (24-48hr lag)
3. Ingests game-time weather forecasts from Open-Meteo
4. Builds handedness splits, pitch type profiles, and zone profiles for every active batter and pitcher
5. Computes three projections per batter-pitcher matchup:
   - **Projected batting average** — blend of handedness baseline, pitch type match score, and zone match score
   - **Projected total bases** — projected slugging × empirical AB/game by lineup slot
   - **Projected HR probability (per game)** — blend of batter HR rate, pitcher HR vulnerability, and barrel context
6. Exports 28-column projection table to Google Sheets → Tableau Public dashboard

A game-aware scheduler runs the full pipeline at startup and refreshes lineups every 30 minutes through the game window, with team-level tracking and missing team alerts.

---

## Data Sources

| Source | What | Auth |
|--------|------|------|
| [MLB Stats API](https://github.com/toddrob99/MLB-StatsAPI/wiki/Endpoints) | Schedule, lineups, probable starters, rosters, hand splits | None required |
| [Baseball Savant / Statcast](https://baseballsavant.mlb.com/csv-docs) | Pitch-by-pitch: type, location, exit velocity, xwOBA | None required |
| [Open-Meteo](https://open-meteo.com/en/docs) | Game-time weather forecasts per venue | None required (10k calls/day free) |

All three sources are entirely free with no API keys required.

---

## Stack

- **Python 3.10** — pipeline orchestration, ingestion, transforms, scoring
- **SQLite** — star schema database (`data/mlb_pregame.db`)
- **pybaseball** — Statcast ingestion
- **gspread** — Google Sheets export
- **Tableau Public** — dashboard visualization (live Google Sheets connection)

---

## Setup

```bash
pip install pybaseball pandas numpy gspread google-auth
```

Copy `.env.template` to `.env` and fill in your Google Sheets credentials:

```
DB_PATH=data/mlb_pregame.db
GOOGLE_SHEET_ID=your_google_sheet_id_here
GOOGLE_SHEETS_CREDENTIALS_PATH=config/sheets_credentials.json
DEFAULT_SEASON=2026
DEFAULT_WINDOW=SEASON
```

Place your Google service account JSON at `config/sheets_credentials.json`.

---

## Running the Pipeline

### Daily automated run (recommended)
```powershell
python scheduler.py
```
Runs full pipeline at startup, then refreshes lineups every 30 minutes through the game window. Stop with Ctrl+C.

### Manual full pipeline
```powershell
python run_pipeline.py --today --db-path data/mlb_pregame.db
```

### Skip Statcast (fast intraday refresh)
```powershell
python run_pipeline.py --today --skip-statcast --db-path data/mlb_pregame.db
```

### First run only (seeds teams, venues, rosters)
```powershell
python run_pipeline.py --today --db-path data/mlb_pregame.db --seed-dimensions
```

### Dry-run scheduler (preview refresh schedule without executing)
```powershell
python scheduler.py --dry-run
```

---

## Running Individual Steps

```powershell
python ingest/compute_match_scores.py --today --db-path data/mlb_pregame.db
python ingest/export_to_sheets.py --today --db-path data/mlb_pregame.db
python ingest/ingest_batter_splits_statsapi.py --today --db-path data/mlb_pregame.db
```

---

## Projection Model

All three projection metrics were empirically backtested against 2026 season actual outcomes. See `BACKTESTING.md` for full methodology and results. Key parameters:

| Parameter | Value | Notes |
|-----------|-------|-------|
| Regression threshold τ | 150 pitches | BA zero-crossing bias point |
| BA regression target | 0.220 | Observed small-sample 2026 BA |
| SLG regression target | 0.380 | Observed SLG zero-crossing |
| Blend weights (BA + SLG) | 70 / 20 / 10 | Baseline / PT score / Zone score |
| Baseline split | 30 / 70 | Batter / Pitcher weighting |
| AB/game | Slot-based | Empirical 2026 averages by lineup position |
| HR/PA league avg | 0.0293 | Observed 2026 rate |
| Barrel rate league avg | 0.0708 | Observed 2026 corrected barrel rate |
| BBE threshold | 100 | HR model regression threshold |

---

## File Structure

```
mlb_model_2026/
├── run_pipeline.py                         # Main orchestrator
├── scheduler.py                            # Game-aware daily scheduler
├── query_cubs_pitchers.py                  # Ad-hoc pitcher analysis utility
├── README.md
├── ARCHITECTURE.md                         # Database schema + data flow
├── BACKTESTING.md                          # Methodology + results
├── CHANGELOG.md
├── .env                                    # Credentials (not in git)
├── .env.template
├── .gitignore
├── requirements.txt
├── db/
│   ├── init_db.py                          # Schema DDL + seed data
│   ├── migrate_add_power_profile.py
│   ├── migrate_seed_park_factors.py
│   └── migrate_add_boxscore_table.py
├── ingest/
│   ├── ingest_mlb_statsapi.py              # Schedule, lineups, rosters
│   ├── ingest_batter_splits_statsapi.py    # Near-real-time hand splits
│   ├── ingest_statcast.py                  # Pitch-level data via pybaseball
│   ├── ingest_weather.py                   # Open-Meteo forecasts
│   ├── ingest_boxscores.py                 # Actual game results (backtesting ground truth)
│   ├── transform_splits.py                 # All split fact tables + matchup rows
│   ├── compute_match_scores.py             # Match scores + all projections
│   └── export_to_sheets.py                 # Writes 28-column output to Google Sheets
├── backtest/
│   ├── backtest_final_projection.py        # BA projection grid search
│   ├── backtest_baseline_split.py          # Baseline split weighting test
│   ├── backtest_blend_weights.py           # Blend weight grid search
│   ├── backtest_total_bases.py             # SLG target + TB blend weight test
│   └── backtest_hr_probability.py          # BBE threshold + HR blend test
├── utils/
│   └── config.py                           # Loads .env, shared constants
├── data/
│   ├── mlb_pregame.db                      # SQLite database (auto-created)
│   └── cubs_analysis/                      # Ad-hoc pitcher analysis output
└── logs/
    └── scheduler.log
```

---

## Key SQL Patterns

### Tonight's top projected batters
```sql
SELECT p.full_name, t.team_abbr,
       m.projected_batting_avg,
       m.projected_total_bases,
       m.projected_hr_probability
FROM   fact_matchup_batter_pitcher m
JOIN   dim_players p ON p.player_id = m.batter_id
JOIN   dim_teams   t ON t.team_id   = m.team_id
WHERE  m.as_of_date  = date('now')
  AND  m.window_code = 'SEASON'
ORDER  BY m.projected_batting_avg DESC
LIMIT  20;
```

### Pitcher pitch mix vs righties
```sql
SELECT pt.pitch_type_name, pm.usage_pct,
       pm.avg_velocity, pm.batting_avg_allowed
FROM   fact_pitcher_pitch_mix pm
JOIN   dim_pitch_types pt ON pt.pitch_type_code = pm.pitch_type_code
WHERE  pm.pitcher_id   = 123456
  AND  pm.split_hand   = 'R'
  AND  pm.window_code  = 'SEASON'
  AND  pm.as_of_date   = date('now')
ORDER  BY pm.usage_pct DESC;
```

---

## Backtesting

All model parameters were empirically validated against 2026 season actual outcomes. Backtesting scripts live in `backtest/` and are read-only — safe to run while the scheduler is active. See `BACKTESTING.md` for full results.
