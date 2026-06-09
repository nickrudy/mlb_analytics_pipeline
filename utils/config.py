"""
config.py
---------
Central configuration loader. All scripts import from here
rather than reading environment variables directly.

Usage in any script:
    from utils.config import DB_PATH, GOOGLE_SHEET_ID
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root regardless of where script is run from
_project_root = Path(__file__).parent.parent
load_dotenv(_project_root / ".env")

# ── Database ───────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "data/mlb_pregame.db")

# ── Google Sheets ──────────────────────────────────────────────────────────
GOOGLE_SHEET_ID            = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SHEETS_CREDENTIALS  = os.getenv(
    "GOOGLE_SHEETS_CREDENTIALS_PATH",
    "config/sheets_credentials.json"
)

# ── Pipeline defaults ──────────────────────────────────────────────────────
LEAGUE_AVG_BA  = float(os.getenv("LEAGUE_AVG_BA",  "0.243"))
DEFAULT_SEASON = int(os.getenv("DEFAULT_SEASON",   "2026"))
DEFAULT_WINDOW = os.getenv("DEFAULT_WINDOW", "SEASON")
