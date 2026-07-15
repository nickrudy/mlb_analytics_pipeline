# MLB Pre-Game Analytics Pipeline

Player-level pre-game matchup projections using free public data sources.
Generates per-batter projected batting average, total bases, and HR
probability for every confirmed lineup, exported daily to Supabase and
visualized in Looker Studio.

---

## What It Does

Each day, on a GitHub Actions schedule:

1. Fetches today's schedule, lineups, and probable pitchers from the MLB Stats API
2. Pulls pitch-by-pitch Statcast data via pybaseball (24-48hr lag) — full runs only
3. Ingests game-time weather forecasts from Open-Meteo
4. Builds handedness splits, pitch type profiles, and zone profiles for every active batter and pitcher
5. Computes three projections per batter-pitcher matchup:
   - **Projected batting average** — blend of handedness baseline, pitch type match score, and zone match score
   - **Projected total bases** — projected slugging × empirical AB/game by lineup slot
   - **Projected HR probability (per game)** — blend of batter HR rate, pitcher HR vulnerability, and barrel context
6. Writes the three leaderboard tables Looker Studio reads directly
   (`daily_top_batting`, `daily_top_bases`, `daily_top_hrs`), plus a
   secondary 28-column export to Google Sheets

GitHub Actions runs a full pipeline pass in the early morning (Statcast +
weekly roster refresh on Mondays) and seven lighter, lineup-watching passes
through the day as games approach — see `.github/workflows/daily_pipeline.yml`
and `ARCHITECTURE.md` for the exact schedule. There is no long-running local
process to keep open; automation is fully headless.

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
- **PostgreSQL via Supabase** — production database
- **SQLite** — local development database, and the intentional historical/
  backtesting store (see `ARCHITECTURE.md`'s "Two-Tier Data Strategy")
- **GitHub Actions** — scheduled orchestration (headless, no local process required)
- **Looker Studio** — primary dashboard (direct PostgreSQL connection to Supabase)
- **pybaseball** — Statcast ingestion
- **gspread** — secondary Google Sheets export
- **psycopg2 / SQLAlchemy** — Postgres connectivity

---

## Setup

```bash
pip install -r requirements.txt
```

Copy `.env.template` to `.env`:

```
DB_BACKEND=sqlite                              # or "supabase"
SUPABASE_DB_URL=                                # required if DB_BACKEND=supabase
GOOGLE_SHEET_ID=your_google_sheet_id_here
GOOGLE_SHEETS_CREDENTIALS_PATH=config/sheets_credentials.json
DEFAULT_SEASON=2026
DEFAULT_WINDOW=SEASON
```

Place your Google service account JSON at `config/sheets_credentials.json`.

For production (GitHub Actions), the equivalent values are supplied as repo
secrets (`SUPABASE_DB_URL`, `GSHEETS_SERVICE_ACCOUNT_JSON`, `GOOGLE_SHEET_ID`)
rather than a local `.env` — these are independent stores; updating one does
not update the other (worth remembering after any credential rotation).

---

## Running the Pipeline

`DB_BACKEND` controls which database every command below talks to — set it
once per terminal session before running anything:

```powershell
$env:DB_BACKEND = "sqlite"      # or "supabase"
```

### Full pipeline
```powershell
python run_pipeline.py --today
```

### Skip Statcast (fast intraday refresh)
```powershell
python run_pipeline.py --today --skip-statcast
```

### Seed/refresh dimension data (teams, venues, rosters)
```powershell
python run_pipeline.py --today --seed-dimensions
```
Runs automatically every Monday in production; only needed by hand for a
fresh local database or an out-of-cycle refresh.

### Other useful flags
```powershell
python run_pipeline.py --date 2026-07-15        # explicit date instead of --today
python run_pipeline.py --today --statcast-days 5  # widen the Statcast lookback window
python run_pipeline.py --today --windows SEASON,L30D,L14D,L7D  # build sub-windows (not built by default)
```

---

## Running Individual Steps

```powershell
python ingest/compute_match_scores.py --today
python ingest/export_to_sheets.py --today
python ingest/ingest_batter_splits_statsapi.py --today
python ingest/ingest_boxscores.py --last-n-days 7   # backfills backtesting ground truth; not part of the automated cadence
```

---

## Projection Model

All three projection metrics were empirically backtested against 2026 season
actual outcomes — see the `backtest/` folder and `backtest/logs/` for
methodology and results, and `BACKTESTING.md` (confirmed current, July 2026)
for the full writeup with implemented parameters, results tables, and a
re-evaluation checklist. Key parameters:

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

**Note:** the hardcoded "current live" values still present in a couple of the
`backtest/` scripts' docstrings (e.g. 40/60 baseline split, 0.40/0.35/0.25
blend) are frozen pre-test comparison baselines from when each optimization
round was designed — not current production values. `BACKTESTING.md` and the
live `compute_match_scores.py` source are the authoritative values; the
table above matches both.

---

## File Structure

Confidence is high for anything touched or read directly this session;
lower-confidence/unverified paths are marked.

```
mlb_model_2026/
├── run_pipeline.py                         # Main orchestrator (no --db-path flag; DB_BACKEND env var controls backend)
├── scheduler.py                            # Local manual fallback -- game-aware refresh loop, used only if GitHub Actions/Supabase are fully unavailable
├── README.md
├── ARCHITECTURE.md                         # Database schema + data flow
├── BACKTESTING.md                          # Methodology, results, implemented parameters (confirmed current)
├── CHANGELOG.md
├── POSTMORTEM_io_outage_refactor.md        # July 2026 IO incident writeup [VERIFY final location]
├── .env                                    # Credentials (not in git)
├── .env.template
├── .gitignore
├── requirements.txt
├── .github/
│   └── workflows/
│       └── daily_pipeline.yml              # GitHub Actions cron — the actual production automation
├── db/
│   └── init_db.py                          # Schema DDL + seed data (SQLite path) [VERIFY exact contents current]
├── ingest/
│   ├── ingest_mlb_statsapi.py              # Schedule, lineups, rosters
│   ├── ingest_batter_splits_statsapi.py    # Near-real-time hand splits
│   ├── ingest_statcast.py                  # Pitch-level data via pybaseball, bulk_upsert
│   ├── ingest_weather.py                   # Open-Meteo forecasts
│   ├── ingest_boxscores.py                 # Actual game results (backtesting ground truth) — manual/periodic, not automated
│   ├── transform_splits.py                 # All split fact tables + matchup rows (build_matchups)
│   ├── compute_match_scores.py             # Match scores + all projections
│   ├── export_to_sheets.py                 # Secondary: 28-column output to Google Sheets
│   └── export_to_daily_tables.py           # PRIMARY: daily_top_* flat tables -> Looker Studio
├── backtest/
│   ├── backtest_final_projection.py        # BA projection grid search
│   ├── backtest_baseline_split.py          # Baseline split weighting test
│   ├── backtest_blend_weights.py           # Blend weight grid search
│   ├── backtest_total_bases.py             # SLG target + TB blend weight test
│   ├── backtest_hr_probability.py          # BBE threshold + HR blend test
│   └── logs/
│       └── refinement_log_060826.docx      # Empirical findings behind the live model constants
├── docs/
│   ├── design/
│   │   └── mlb_pregame_data_dictionary_and_sql_schema.xlsx   # Original schema design doc
│   └── research/
│       └── unsolved_metrics_deep_research.docx                # Exploratory metrics research
├── scripts/
│   ├── migrate_schema_to_supabase.py       # Original full SQLite->Postgres schema migration (run once)
│   ├── migrate_add_power_profile_supabase.py
│   └── migrate_remaining_supabase.py
├── utils/
│   ├── db.py                               # Backend connection handling (DB_BACKEND-driven)
│   ├── db_bulk.py                          # bulk_upsert — shared batched write helper
│   └── config.py                           # Loads .env, shared constants [VERIFY exact contents current]
├── data/
│   ├── mlb_pregame.db                      # SQLite database (auto-created, gitignored)
│   └── fallback_xslx/                      # Local daily_board.py output (gitignored)
├── testing/                                 # Gitignored — ad-hoc local tools, not deployed:
│   ├── daily_board.py                       #   local SQLite-backed fallback board + xlsx log
│   ├── query_cubs_pitchers.py                #   one-off editorial-content query
│   ├── query_hr_prob_filtered.py             #   one-off filtered HR query [known stale vs. current schema]
│   └── query_today.py                        #   scratch query template
└── logs/
    └── pipeline.log
```

---

## Key SQL Patterns

Written for Postgres/Supabase syntax (production). On SQLite, `date('now')`
replaces Postgres's `CURRENT_DATE`; parameter binding also differs slightly
between the two backends via `utils/db.py`'s translation layer.

### Tonight's top projected batters
```sql
SELECT p.full_name, t.team_abbr,
       m.projected_batting_avg,
       m.projected_total_bases,
       m.projected_hr_probability
FROM   fact_matchup_batter_pitcher m
JOIN   dim_players p ON p.player_id = m.batter_id
JOIN   dim_teams   t ON t.team_id   = m.team_id
WHERE  m.as_of_date  = CURRENT_DATE
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
  AND  pm.as_of_date   = CURRENT_DATE
ORDER  BY pm.usage_pct DESC;
```

---

## Backtesting

All model parameters were empirically validated against 2026 season actual
outcomes. Backtesting scripts live in `backtest/` and are read-only — safe to
run alongside the live pipeline on either backend. Findings and implemented
parameters are documented in full in `BACKTESTING.md`; a couple of the
individual backtest scripts' own docstrings still reference pre-optimization
baseline values as "current live" — these are historical comparison labels,
not current production values (see "Projection Model" above).

---

## Security

Row-Level Security is enabled on every table in the Supabase project, and the
Supabase Data API (PostgREST/REST layer) is disabled entirely — nothing in
this project uses it; the pipeline and Looker Studio both connect via direct
PostgreSQL connections. This is defense-in-depth, not a functional
requirement of the current architecture — see `ARCHITECTURE.md`'s Exports
section for the full reasoning.
