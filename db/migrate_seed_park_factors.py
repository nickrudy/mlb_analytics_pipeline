"""
migrate_seed_park_factors.py
-----------------------------
Seeds park_run_factor, park_hr_factor_rhb, and park_hr_factor_lhb into
dim_venues using multi-year (2023-2025) Fangraphs park factor estimates.

Values expressed as integers where 100 = league average:
    120 = 20% more HRs than league average at this park
     85 = 15% fewer HRs than league average at this park

LHB vs RHB factors are separate because parks like Yankee Stadium
(short right-field porch) and Fenway Park (Green Monster) have strongly
asymmetric HR profiles by batter handedness.

park_run_factor is also seeded here for consistency — previously NULL,
which meant BA projections and HR projections were using inconsistent
park adjustments (one nulled out at 1.0, one populated).

Venue IDs are sourced from the MLB Stats API (same IDs in dim_venues).
Safe to run multiple times — uses UPDATE, not INSERT.

Usage:
    python db/migrate_seed_park_factors.py --db-path data/mlb_pregame.db

To update a single park mid-season:
    UPDATE dim_venues
    SET park_hr_factor_rhb = 115, park_hr_factor_lhb = 112, park_run_factor = 1.08
    WHERE venue_name LIKE '%Great American%';
"""

import sqlite3
import logging
import argparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ── Park factor data ───────────────────────────────────────────────────────
# Format: (venue_name_fragment, hr_rhb, hr_lhb, run_factor)
#
# venue_name_fragment: partial match against dim_venues.venue_name (LIKE).
# hr_rhb / hr_lhb: integer, 100 = league average.
# run_factor: decimal multiplier used in BA projection (1.0 = neutral).
#
# Sources: Fangraphs Park Factors (multi-year 2023-2025 average),
#          ESPN Park Factors, Baseball Reference park adjustments.
# Note: Tropicana Field is being phased out mid-2026; TB factor reflects
#       their current home situation — update when permanent venue confirmed.

PARK_FACTORS = [
    # venue_name_fragment        hr_rhb  hr_lhb  run_factor
    # ── Strong hitter parks ───────────────────────────────
    ("Coors Field",               123,    118,    1.15),   # altitude + thin air
    ("Great American Ball Park",  118,    115,    1.10),   # consistent HR haven
    ("Yankee Stadium",            120,    108,    1.08),   # short right porch, favors LHB
    ("Fenway Park",               105,    120,    1.06),   # Green Monster favors RHB, Pesky Pole LHB
    ("Globe Life Field",          112,    110,    1.07),   # retractable roof, hitter friendly
    ("Camden Yards",              110,    108,    1.06),   # hitter friendly
    ("Citizens Bank Park",        110,    108,    1.06),   # consistent hitter park
    ("Wrigley Field",             108,    110,    1.05),   # wind-dependent; LHB slight edge
    ("Rogers Centre",             108,    105,    1.06),   # dome, turf, hitter friendly
    ("Guaranteed Rate Field",     105,    103,    1.04),   # moderate hitter lean
    ("Chase Field",               105,    103,    1.04),   # retractable roof, moderate hitter lean
    ("Truist Park",               105,    105,    1.03),   # slight hitter lean
    # ── Near-neutral parks ────────────────────────────────
    ("Nationals Park",            100,    100,    1.00),
    ("Minute Maid Park",          100,    103,    1.01),   # Crawford Boxes favor LHB
    ("Dodger Stadium",             98,    100,    0.99),   # slight LHB edge
    ("Angel Stadium",              97,     96,    0.98),
    ("Target Field",               97,     98,    0.98),   # cold weather suppresses slightly
    ("Progressive Field",          97,     97,    0.98),
    ("American Family Field",     100,     98,    0.99),
    # ── Pitcher-friendly parks ────────────────────────────
    ("Kauffman Stadium",           95,     96,    0.97),
    ("Busch Stadium",              95,     97,    0.97),
    ("Comerica Park",              93,     95,    0.96),
    ("PNC Park",                   93,     95,    0.96),
    ("Tropicana Field",            93,     93,    0.96),   # dome, slight pitcher lean
    ("loanDepot Park",             92,     92,    0.96),   # pitcher-friendly dome
    ("Oakland Coliseum",           92,     92,    0.96),   # pitcher friendly; OAK situation fluid
    ("Citi Field",                 96,     98,    0.97),
    ("T-Mobile Park",              90,     92,    0.95),   # pitcher friendly
    ("Petco Park",                 88,     90,    0.94),   # pitcher friendly
    ("Oracle Park",                85,     88,    0.93),   # strongest pitcher park; McCovey Cove
]


def run_migration(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=OFF;")
    conn.execute("PRAGMA journal_mode=WAL;")

    log.info("Seeding park factors into dim_venues at %s", db_path)

    updated   = 0
    not_found = []

    for venue_fragment, hr_rhb, hr_lhb, run_factor in PARK_FACTORS:
        result = conn.execute(
            """
            UPDATE dim_venues
            SET    park_hr_factor_rhb = ?,
                   park_hr_factor_lhb = ?,
                   park_run_factor    = ?
            WHERE  venue_name LIKE ?
            """,
            (hr_rhb, hr_lhb, run_factor, f"%{venue_fragment}%"),
        )
        if result.rowcount > 0:
            log.info("  Updated: %-35s  HR(RHB=%d, LHB=%d)  run=%.2f",
                     venue_fragment, hr_rhb, hr_lhb, run_factor)
            updated += result.rowcount
        else:
            log.warning("  NOT FOUND in dim_venues: %s", venue_fragment)
            not_found.append(venue_fragment)

    conn.commit()

    # ── Validation ─────────────────────────────────────────────────────────
    rows = conn.execute(
        """
        SELECT venue_name, park_hr_factor_rhb, park_hr_factor_lhb, park_run_factor
        FROM   dim_venues
        WHERE  park_hr_factor_rhb IS NOT NULL
        ORDER  BY venue_name
        """
    ).fetchall()

    null_rows = conn.execute(
        """
        SELECT COUNT(*) FROM dim_venues
        WHERE  park_hr_factor_rhb IS NULL
        """
    ).fetchone()[0]

    conn.close()

    log.info("--- Results ---")
    log.info("Venues updated: %d", updated)
    log.info("Venues still NULL (will default to 100/1.0 in model): %d", null_rows)

    if not_found:
        log.warning("Venues not matched — check venue_name spelling in dim_venues:")
        for v in not_found:
            log.warning("  ! %s", v)
        log.warning("Run this query in DB Browser to see actual venue names:")
        log.warning("  SELECT venue_id, venue_name FROM dim_venues ORDER BY venue_name;")

    log.info("Seeded venues:")
    for venue_name, hr_rhb, hr_lhb, run_factor in rows:
        log.info("  %-40s  HR(R=%s, L=%s)  run=%s",
                 venue_name, hr_rhb, hr_lhb, run_factor)

    log.info("Migration complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Seed park HR and run factors into dim_venues"
    )
    parser.add_argument("--db-path", default="data/mlb_pregame.db")
    args = parser.parse_args()
    run_migration(args.db_path)
