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
      - SQLite:   executemany() with INSERT OR REPLACE

    rows: list of dicts, all with the same keys
    conflict_cols: comma-separated unique key cols for ON CONFLICT (Supabase only)
    update_cols: list of col names to update on conflict (Supabase only)
                 if None, uses DO NOTHING

    Returns number of rows inserted.
    """
    if not rows:
        return 0

    cols = list(rows[0].keys())
    col_str = ", ".join(cols)

    if DB_BACKEND == "supabase":
        from psycopg2.extras import execute_values

        # Build values template — psycopg2 execute_values uses %s per row
        val_template = "(" + ", ".join(f"%({c})s" for c in cols) + ")"

        if conflict_cols and update_cols:
            updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in update_cols)
            conflict_clause = f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {updates}"
        elif conflict_cols:
            conflict_clause = f"ON CONFLICT ({conflict_cols}) DO NOTHING"
        else:
            conflict_clause = ""

        sql = f"INSERT INTO {table} ({col_str}) VALUES %s {conflict_clause}"

        # execute_values sends all rows in one round-trip
        raw_conn = conn._c if hasattr(conn, "_c") else conn
        cur = raw_conn.cursor()
        execute_values(cur, sql, rows, template=val_template, page_size=1000)
        return len(rows)

    else:
        # SQLite path — executemany with INSERT OR REPLACE
        placeholders = ", ".join(f":{c}" for c in cols)
        sql = f"INSERT OR REPLACE INTO {table} ({col_str}) VALUES ({placeholders})"
        conn.executemany(sql, rows)
        return len(rows)
