"""
utils/db_bulk.py
----------------
Bulk insert helper for Supabase using psycopg2.extras.execute_values().
Sends all rows in a single SQL statement — dramatically faster than
executemany() for large inserts (32k rows: 20 min → ~5 sec).

Usage:
    from utils.db_bulk import bulk_upsert
    bulk_upsert(conn, "fact_pitcher_zone_profile", rows, conflict_cols)
"""
from utils.db import DB_BACKEND


def bulk_upsert(conn, table: str, rows: list, conflict_cols: str = None,
                update_cols: list = None) -> int:
    """
    Insert rows into table using the fastest available method:
      - Supabase: psycopg2.extras.execute_values() — single round-trip
      - SQLite:   executemany() with INSERT OR REPLACE / INSERT OR IGNORE

    rows: list of dicts, all with the same keys
    conflict_cols: comma-separated unique key cols for ON CONFLICT

    update_cols semantics (only meaningful when conflict_cols is set):
      - None  -> update ALL non-key columns present in the row (default).
                 This is the intended §2.4 behavior: a conflict refreshes
                 every written column, never a stale subset.
      - [...]  -> update exactly these columns on conflict.
      - []     -> DO NOTHING on conflict (explicit; e.g. immutable ingest).
      - no conflict_cols at all -> plain INSERT (used after TRUNCATE).

    Returns number of rows sent (attempted), not rows actually written —
    with DO NOTHING, conflicting rows are counted but not inserted.
    """
    if not rows:
        return 0

    cols = list(rows[0].keys())
    col_str = ", ".join(cols)

    if DB_BACKEND == "supabase":
        from psycopg2.extras import execute_values

        # Build values template — psycopg2 execute_values uses %s per row
        val_template = "(" + ", ".join(f"%({c})s" for c in cols) + ")"

        if conflict_cols is not None and update_cols == []:
            # explicit DO NOTHING
            conflict_clause = f"ON CONFLICT ({conflict_cols}) DO NOTHING"
        elif conflict_cols:
            key_set = {c.strip() for c in conflict_cols.split(",")}
            # update_cols=None -> all non-key cols present in the row
            cols_to_update = update_cols if update_cols else [
                c for c in cols if c not in key_set
            ]
            updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols_to_update)
            conflict_clause = f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {updates}"
        else:
            conflict_clause = ""

        sql = f"INSERT INTO {table} ({col_str}) VALUES %s {conflict_clause}"

        # execute_values sends all rows in one round-trip
        raw_conn = conn._c if hasattr(conn, "_c") else conn
        cur = raw_conn.cursor()
        execute_values(cur, sql, rows, template=val_template, page_size=1000)
        return len(rows)

    else:
        # SQLite path.
        #   update_cols == [] with conflict_cols -> INSERT OR IGNORE, to mirror
        #   Supabase DO NOTHING (keep existing row on conflict).
        #   Otherwise -> INSERT OR REPLACE (whole-row replace). NOTE: SQLite
        #   cannot do partial per-column updates here, so a second partial
        #   writer to an existing PK resets the first writer's columns. This
        #   is pre-existing behavior; SQLite is test-only. Do not validate the
        #   matchup two-writer path on SQLite. (See refactor plan §0a.)
        placeholders = ", ".join(f":{c}" for c in cols)
        verb = "INSERT OR IGNORE" if (conflict_cols is not None and update_cols == []) \
            else "INSERT OR REPLACE"
        sql = f"{verb} INTO {table} ({col_str}) VALUES ({placeholders})"
        conn.executemany(sql, rows)
        return len(rows)
