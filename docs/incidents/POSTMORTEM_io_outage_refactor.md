# Postmortem: Disk IO Exhaustion, Platform Outage, and Pipeline Refactor
**Period covered:** ~July 2 – July 14, 2026
**Status:** Resolved. Refactor merged to `main`, verified in production.

---

## 1. Summary

Over roughly four days of full-cadence running (~10 runs/day), the pipeline's
Supabase Micro instance climbed from ~0% to 100% daily Disk IO burst
consumption and stayed pinned there. The proximate cause was genuine
inefficiency in the pipeline's own read/write patterns (documented in
`ARCHITECTURAL_REVIEW.md`). That investigation was then complicated by an
unrelated, concurrent Supabase platform-wide capacity incident, which made it
difficult for several days to tell which symptoms were "us" and which were
"them." The pipeline was refactored (9 commits, P0 correctness fixes + P1
performance fixes), verified against real Supabase data, and merged. Compute
was upgraded one tier (Micro → Small, +$5/mo) as a parallel mitigation after
a Supabase support engagement whose root-cause theory (memory/swap
exhaustion) was only partially convincing on the evidence, but the upgrade
resolved the symptom regardless of which explanation was fully correct.

**Net financial cost:** ~$5/month (Small compute) on top of the existing
$25/mo Pro plan. No data loss at any point.

---

## 2. Timeline

**Pre-incident:** Solo/casual pipeline (Python ingestion + transforms →
Supabase Postgres → Looker Studio), running on a GitHub Actions cron at
~10x/day cadence, previously paused and restarted.

**Day 1 (~Jul 6):** Disk IO consumption observed climbing sharply after the
cron was re-enabled following a pause. Workflow disabled to investigate.
`ARCHITECTURAL_REVIEW.md` (pre-existing, from an earlier session) identified
as the primary source of truth for known inefficiencies — treated as a
checklist rather than re-discovered from scratch.

**Day 1-2:** Refactor branch (`refactor/p0-p1-io`) created. Nine commits
landed over the following days, each individually verified before merge:

- **P0 #4** — dynamic league-BA query missing an `as_of_date` filter (silent
  cross-date contamination, masked in production only because a separate
  cleanup step happened to delete other dates each run).
- **P0 #2** — Step 7b (daily flat-table export) truncated Looker's source
  tables *before* confirming new data existed, so an upstream failure could
  silently blank the dashboards while still logging "complete." Fixed to
  fail loud before truncating.
- **P0 #3** — `bulk_upsert`'s `update_cols` defaulted to updating a
  hand-picked subset of columns, not all non-key columns — so re-runs left
  many computed columns silently stale on conflict. Fixed to default to
  "all non-key columns present in the row," audited across all 12 call
  sites (10 split-builder tables + `build_matchups` + `compute_match_scores`).
- **P0 #1** — turned out to already be fixed from a prior, forgotten session
  (`ingested_at` on matchup writes) — discovered via git log archaeology,
  not re-implemented.
- **P1 #8** — the pipeline was building and scoring SEASON + L30D + L14D +
  L7D windows every run, but only SEASON was ever consumed downstream.
  Defaulted to SEASON-only (windows still buildable via flag), roughly
  quartering Steps 5/6 work.
- **P1 #6** — `ingest_statcast.py` inserted rows one at a time via
  `executemany`-style per-row calls; converted to the shared `bulk_upsert`
  helper (single batched `execute_values` call).
- **P1 #5-B** — `transform_splits.py` read `SELECT *` on the 358k-row pitch
  table every run; projected down to the ~22 columns actually consumed by
  the aggregators (verified column-complete against every builder,
  including the release-speed/spin-rate columns two builders needed that a
  naive projection would have dropped). Two O(n²) builders
  (`batter_power_profile`, `pitcher_hr_vulnerability`) rewritten to
  pre-group once instead of re-scanning the full frame per batter/pitcher.
- **P1 #7** — `build_matchups()` issued up to 6 per-row SQL SELECTs per
  lineup row (~1,000+ round-trips per slate); rewritten to preload each
  lookup source once into a dict, keyed by primary key, with the
  `update_cols` fix folded in since it touched the same function.

**Mid-week:** Supabase multi-region capacity incident began, overlapping
directly with the refactor/verification work. This significantly
complicated diagnosis — connection timeouts, statement-timeout cancellations,
and pooler auth failures were happening for *both* platform-incident reasons
and the pipeline's own pre-refactor IO load, and for several days it was
genuinely unclear which was which. Verification work was repeatedly paused
and resumed based on the platform's own status page.

**Mid-week, separately:** A Supabase database credential was accidentally
exposed in a chat transcript; rotated immediately, `.env` and the GitHub
Actions secret both updated (the secret update was initially missed, causing
one later production auth failure — see §4).

**Late week:** Platform incident resolved per Supabase's status page. Full
parity verification completed for #5-B and #7 against live Supabase data
(not just local SQLite) — a real, byte-for-byte comparison of old-code vs.
new-code output on the same underlying data, including one deliberately
investigated discrepancy that turned out to be #3's fix correctly
un-staling a column, not a #5-B bug. Branch merged to `main`.

**Post-merge:** GitHub Actions re-enabled. First production run failed on
the (unrelated, already-known) credential-rotation gap — Actions secret
still held the pre-rotation password. Fixed. Pipeline then ran successfully
in production for several days, including surfacing and closing a
genuinely new, real production bug (see §5) and validating a second fix
(the zero-lineup cron guard, see §6) live through the All-Star break.

**All-Star break (Jul 13-14):** Used as a low-stakes window (zero real game
data at stake) to build and validate the zero-matchup early-exit fix, close
several long-open backlog items with real evidence, and produce this
document.

---

## 3. Root Cause — Two Separate, Overlapping Problems

It's important to keep these distinct, because they were frequently
conflated in the moment and have different owners and different fixes:

**3a. The pipeline's own IO footprint (fully within our control, fixed).**
Documented exhaustively in `ARCHITECTURAL_REVIEW.md` and addressed by the
nine refactor commits above. This was real, measurable, and the fix is
verified. At full ~10x/day cadence, the *un-refactored* code was reading
the full pitch table (with all columns) up to four times per run (once per
window), issuing ~1,000+ round-trips per matchup build, and inserting
Statcast data one row at a time — a workload profile that would strain any
small compute tier regardless of platform health.

**3b. A concurrent Supabase platform incident (outside our control,
resolved by Supabase).** A multi-region capacity issue that began
independently and overlapped with the investigation window. Contributed
real, separate symptoms (connection resets, statement-timeout
cancellations tied to Supabase's own internal `pg_stat_statements`
monitoring query, not our code) that were initially hard to distinguish
from 3a's IO pressure.

**Never fully resolved, and worth naming honestly:** Supabase support's
own diagnosis (sustained memory/swap exhaustion, unrelated to the platform
incident) was engaged with in detail and never fully agreed with — their
own supporting evidence (RAM Free consistently >50% of total; the EBS IO
balance chart showing normal recharge cycles through the exact days the
consumption chart was pinned at 100%; the IO drain persisting through
extended periods of zero client-side activity) was, on close reading,
inconsistent with their own conclusion. The compute upgrade (Micro→Small)
was pursued anyway as a cheap, low-risk mitigation regardless of which
theory was correct — and it did resolve the symptom. Whether that's because
(a) Supabase's theory was right and more RAM genuinely relieved pressure,
or (b) the extra baseline IO throughput that comes bundled with the Small
tier absorbed background load unrelated to RAM, was never conclusively
determined. Worth remembering if the symptom ever recurs: don't assume the
support-ticket explanation was confirmed just because the fix worked.

---

## 4. What Went Wrong Along the Way (process notes, not code bugs)

- **A credential rotation didn't propagate to GitHub Actions.** Rotating
  the Supabase password and updating `.env` is necessary but not
  sufficient — the Actions secret is a separate, independent store. First
  post-merge production run failed on exactly this gap. Worth a standing
  habit: any credential rotation gets a checklist of *every* place that
  credential lives, not just the most obvious one.
- **The architectural review and various docs drifted from reality
  repeatedly during the refactor.** Several files (`scheduler.py`, a
  Tableau workbook, `transform_splits.py`'s timezone handling, `ingested_at`
  population) had already been partially or fully fixed in a prior,
  forgotten session — discovered only by reading the current file / git log
  before editing, not by trusting the review document. This became a
  deliberate practice for the rest of the refactor: never edit based on a
  document's description of a file: always read the file first.
- **Local SQLite testing has a real, permanent limitation that bit us
  once.** `bulk_upsert`'s SQLite path uses whole-row `INSERT OR REPLACE`
  (vs. Supabase's column-targeted `DO UPDATE SET`), so a table written by
  two different steps (build_matchups + compute_match_scores) can have one
  writer's columns silently reset by the other's write, *on SQLite only*.
  This produced a full false-alarm investigation (suspected #7 regression;
  actually a SQLite-only artifact, confirmed by comparing old-code vs.
  new-code output directly) and, independently, a local-only display bug
  in the ad-hoc `daily_board.py` tool caused by giving it a redundant
  manual `compute_match_scores` call that defeated its own built-in
  clobber workaround. Neither was a real Supabase issue. Lesson: SQLite
  parity is a useful smoke test but is *not* a substitute for verifying
  against Supabase for anything touching a shared, multi-writer table.

---

## 5. Real Production Bug Found and Closed This Week (unrelated to the
   refactor)

While building a local fallback tool to keep working during the platform
outage, a genuine discrepancy was found: matchup rows sometimes carried a
null `team_id`, which cascaded into dropped rows in both Looker export
queries (inner-joined on `dim_teams`). Initially suspected as a
long-standing, silent production data-loss bug. On investigation, this
turned out to be **entirely the SQLite whole-row-replace artifact
described above** — confirmed false on production by direct query against
live Supabase data on 2026-07-14 (18/18 matchup rows retained a correct,
non-null `team_id` after both writers ran). The original "production
data-loss" note has been downgraded/closed. No fix was needed; nothing was
ever actually broken in production. This is a good example of §4's lesson
in practice — a real, reproducible-looking symptom that was fully a local
testing artifact.

---

## 6. Post-Merge Fix: Zero-Matchup Guard

A second, smaller issue surfaced once the refactored pipeline was live and
GitHub Actions was watching it run on a real schedule: every trigger (the
6 AM full run and each of seven intraday lineup-watch runs) unconditionally
proceeded through scoring/export even when lineups hadn't posted yet for
the day, hitting the (correct, by-design) Step 7b guard's `RuntimeError`
and failing the whole job loudly — generating a failure-notification email
for an entirely expected, routine condition, on a variable number of
triggers per day. Fixed with a single, data-driven check (matchup count,
not time-of-day) inserted after Step 5b: if zero matchups exist for today,
log and exit cleanly instead of proceeding. Verified locally against a
guaranteed-empty date, then validated for real in production across
several genuinely-zero-lineup days during the All-Star break — including
one edge case (the All-Star Game itself using synthetic, non-`dim_teams`
team IDs) that correctly exercised the *original* Step 7b guard instead,
confirming both safety nets work independently and correctly.

---

## 7. Current State

- Refactor branch merged to `main`, all P0/P1 items from
  `ARCHITECTURAL_REVIEW.md` closed.
- Compute: Small tier (upgraded from Micro).
- Connection: session pooler (port 5432), not the transaction pooler
  (6543) that was involved in several of the week's connection failures.
- GitHub Actions: live, running the refactored pipeline on the original
  9-trigger schedule, now with the zero-matchup guard.
- Disk IO consumption: returned to normal (single-digit %) on the first
  fully clean post-upgrade day and has stayed there since.
- A local SQLite-backed fallback toolchain (`testing/daily_board.py` and
  supporting scripts) was built during the outage and is being kept as a
  standing, casual-use tool — not part of the production pipeline, useful
  independent of Supabase's health.

---

## 8. Open Items / Not Yet Done

- Looker Studio mobile re-authentication issue — investigated at length
  (credentials mode, Google account permissions) without a conclusive fix;
  likely a native-PostgreSQL-connector limitation. Decision pending
  between accepting it, or migrating to a directly-Postgres-connected BI
  tool (Metabase) as a learning exercise. Not a data-integrity issue.
- Two small, real data gaps identified and sized (not fixed): ~8% of
  active batters temporarily missing from `dim_players` between weekly
  seed runs (mostly self-healing; 3 specific longer-tenured players
  confirmed genuinely absent, root cause not yet investigated in code);
  ~2.5% of active batters missing one hand's split (expected, thin-sample,
  already handled gracefully by the existing regression fallback).
- Root project documentation (`README.md`, `ARCHITECTURE.md`) confirmed
  stale — describes the pre-Supabase, pre-Actions, SQLite/Tableau-only
  architecture and doesn't mention any of this week's work. `requirements.txt`
  is, by contrast, already current. Repo root also has some accumulated
  clutter worth a pass (ad-hoc query scripts, unfamiliar folders) — see
  separate audit.
- Recency-weighted Total Bases signal (hard-hit-rate-based, validated
  through three iterations of local testing this week) and a walk-rate-based
  Top Batters enhancement are both fully specced but not yet integrated
  into the production pipeline — scoped for a dedicated future session,
  with an explicit architectural constraint already flagged: must reuse
  the existing single SEASON read rather than adding a second full-table
  scan, or it will reintroduce the exact IO problem this whole effort
  fixed.
