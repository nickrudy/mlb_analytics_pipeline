"""
utils/db.py
-----------
Connection factory for the MLB analytics pipeline.

Controls backend via DB_BACKEND env var:
  - "sqlite"   → local SQLite file (default, used for staging/testing)
  - "supabase" → Supabase PostgreSQL (used in GitHub Actions / cloud)

Usage in any pipeline script:
    from utils.db import get_connection, get_engine, DB_BACKEND

    # SQLAlchemy engine (for pandas read_sql / to_sql)
    engine = get_engine()
    df.to_sql("game_logs", engine, if_exists="append", index=False)

    # Raw connection (for manual cursor work)
    with get_connection() as conn:
        conn.execute(...)
"""

import os
import sqlite3
import contextlib
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_BACKEND = os.getenv("DB_BACKEND", "sqlite").lower()

class _wrap:
    """Give psycopg2 connections a sqlite3-compatible execute() method."""
    def __init__(self, c): self._c = c

    @staticmethod
    def _translate(sql):
        import re
        return re.sub(r'(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)', r'%(\1)s', sql)

    def execute(self, sql, p=None):
        cur = self._c.cursor()
        cur.execute(self._translate(sql), p or {})
        return cur

    def executemany(self, sql, s):
        cur = self._c.cursor()
        cur.executemany(self._translate(sql), s or [])
        return cur

    def cursor(self):
        return _cursor_wrap(self._c.cursor())

    def commit(self): self._c.commit()
    def rollback(self): self._c.rollback()
    def close(self): self._c.close()
    def __getattr__(self, n): return getattr(self._c, n)


class _cursor_wrap:
    """Wraps a psycopg2 cursor to translate :named params automatically."""
    def __init__(self, cur): self._cur = cur

    @staticmethod
    def _translate(sql):
        import re
        return re.sub(r'(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)', r'%(\1)s', sql)

    def execute(self, sql, p=None):
        self._cur.execute(self._translate(sql), p or {})
        return self

    def executemany(self, sql, s):
        self._cur.executemany(self._translate(sql), s or [])
        return self

    def fetchone(self): return self._cur.fetchone()
    def fetchall(self): return self._cur.fetchall()
    def __getattr__(self, n): return getattr(self._cur, n)

# ---------------------------------------------------------------------------
# SQLite config
# ---------------------------------------------------------------------------
_SQLITE_PATH = Path(os.getenv("SQLITE_PATH", "data/mlb_pipeline.db"))

# ---------------------------------------------------------------------------
# Supabase / PostgreSQL config
# ---------------------------------------------------------------------------
# Supabase exposes two connection modes:
#   - Transaction pooler (port 6543) → preferred for short-lived serverless/CI runs
#   - Session pooler  (port 5432)   → needed for LISTEN/NOTIFY, advisory locks, etc.
# Set SUPABASE_DB_URL to the full connection string, e.g.:
#   postgresql://postgres.<project-ref>:<password>@aws-0-us-east-1.pooler.supabase.com:6543/postgres
_SUPABASE_URL = os.getenv("SUPABASE_DB_URL")


# ---------------------------------------------------------------------------
# Engine factory (SQLAlchemy — for pandas integration)
# ---------------------------------------------------------------------------
def get_engine():
    """Return a SQLAlchemy engine for the configured backend."""
    if DB_BACKEND == "supabase":
        if not _SUPABASE_URL:
            raise EnvironmentError(
                "DB_BACKEND=supabase but SUPABASE_DB_URL is not set. "
                "Add it to .env or GitHub Actions secrets."
            )
        # connect_args: keep_alive prevents pooler timeouts on long fetches
        return create_engine(
            _SUPABASE_URL,
            connect_args={"connect_timeout": 30, "keepalives_idle": 30},
            pool_pre_ping=True,
        )
    else:
        _SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
        return create_engine(f"sqlite:///{_SQLITE_PATH}")


# ---------------------------------------------------------------------------
# Raw connection context manager
# ---------------------------------------------------------------------------
@contextlib.contextmanager
def get_connection():
    """
    Yield a raw DB-API connection (sqlite3 or psycopg2).
    Commits on clean exit, rolls back on exception.

    Example:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
    """
    if DB_BACKEND == "supabase":
        import psycopg2
        from urllib.parse import urlparse

        if not _SUPABASE_URL:
            raise EnvironmentError("SUPABASE_DB_URL is not set.")

        parsed = urlparse(_SUPABASE_URL)
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            dbname=parsed.path.lstrip("/"),
            user=parsed.username,
            password=parsed.password,
            connect_timeout=30,
            keepalives_idle=30,
            sslmode="require",
        )
    else:
        _SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(_SQLITE_PATH)

    if DB_BACKEND == "supabase":
        conn = _wrap(conn)
    try:
        yield conn
        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Convenience: execute a single statement (DDL / DML)
# ---------------------------------------------------------------------------
def execute(sql: str, params=None):
    """Run a single SQL statement. Useful for DDL in init scripts."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})


# ---------------------------------------------------------------------------
# Health-check helper
# ---------------------------------------------------------------------------
def ping():
    """Return True if the configured backend is reachable."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        print(f"[db.ping] Backend '{DB_BACKEND}' not reachable: {exc}")
        return False


if __name__ == "__main__":
    ok = ping()
    print(f"Backend: {DB_BACKEND} | Reachable: {ok}")
