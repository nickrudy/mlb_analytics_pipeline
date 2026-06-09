"""
ingest_weather.py
-----------------
Pulls hourly weather forecasts for each scheduled game venue from the
free Open-Meteo API and populates stg_weather_hourly + fact_game_weather.

Free API docs: https://open-meteo.com/en/docs
No API key required for the free tier (up to 10,000 calls/day).

Usage:
    python ingest/ingest_weather.py --date 2025-04-15 --db-path data/mlb_pregame.db
    python ingest/ingest_weather.py --today --db-path data/mlb_pregame.db
"""

import sqlite3
import json
import time
import logging
import argparse
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
import urllib.request
import urllib.parse
import urllib.error

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Open-Meteo variables we pull per hour
HOURLY_VARS = [
    "temperature_2m",
    "windspeed_10m",
    "winddirection_10m",
    "relativehumidity_2m",
    "precipitation_probability",
    "surface_pressure",
]

# Conversion: Open-Meteo returns °C and km/h → we convert to °F and mph
def _c_to_f(c): return round(c * 9/5 + 32, 1) if c is not None else None
def _kmh_to_mph(k): return round(k * 0.621371, 1) if k is not None else None


def _get(url: str, params: dict) -> dict:
    qs  = urllib.parse.urlencode(params)
    req = urllib.request.Request(f"{url}?{qs}")
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode())


def _fetch_hourly_forecast(lat: float, lon: float, forecast_date: str) -> list[dict]:
    """
    Returns a list of hourly dicts for the given date.
    Open-Meteo forecast endpoint returns up to 16 days ahead.
    """
    params = {
        "latitude":           round(lat, 4),
        "longitude":          round(lon, 4),
        "hourly":             ",".join(HOURLY_VARS),
        "temperature_unit":   "celsius",
        "windspeed_unit":     "kmh",
        "timezone":           "UTC",
        "start_date":         forecast_date,
        "end_date":           forecast_date,
    }
    data = _get(OPEN_METEO_URL, params)
    hourly = data.get("hourly", {})
    times  = hourly.get("time", [])
    rows   = []
    for i, ts in enumerate(times):
        rows.append({
            "forecast_timestamp_utc": ts + ":00Z",
            "temperature_f":          _c_to_f(_safe_float(hourly.get("temperature_2m", [])[i:i+1])),
            "wind_speed_mph":         _kmh_to_mph(_safe_float(hourly.get("windspeed_10m", [])[i:i+1])),
            "wind_direction_deg":     _safe_float(hourly.get("winddirection_10m", [])[i:i+1]),
            "humidity_pct":           _safe_float(hourly.get("relativehumidity_2m", [])[i:i+1]),
            "precipitation_probability_pct": _safe_float(hourly.get("precipitation_probability", [])[i:i+1]),
            "pressure_hpa":           _safe_float(hourly.get("surface_pressure", [])[i:i+1]),
            "raw_payload_json":       json.dumps(data),
        })
    return rows


def _safe_float(lst: list):
    if not lst:
        return None
    v = lst[0]
    try:
        f = float(v)
        return None if f != f else f
    except (TypeError, ValueError):
        return None


def _nearest_forecast(rows: list[dict], target_utc: str) -> dict | None:
    """Return the hourly row closest to target_utc (game first-pitch time)."""
    if not rows or not target_utc:
        return rows[0] if rows else None
    try:
        target = datetime.fromisoformat(target_utc.replace("Z", "+00:00"))
    except ValueError:
        return rows[0] if rows else None

    best, best_delta = None, None
    for r in rows:
        try:
            ts = datetime.fromisoformat(r["forecast_timestamp_utc"].replace("Z", "+00:00"))
            delta = abs((ts - target).total_seconds())
            if best_delta is None or delta < best_delta:
                best, best_delta = r, delta
        except ValueError:
            continue
    return best


def ingest_weather(conn: sqlite3.Connection, game_date: str, as_of_date: str) -> None:
    """
    For each game scheduled on game_date:
      1. Pull venue lat/lon from dim_venues.
      2. Fetch Open-Meteo hourly forecast.
      3. Load all hours into stg_weather_hourly.
      4. Pick the hour nearest first-pitch for fact_game_weather.
    """
    games = conn.execute(
        """
        SELECT g.game_id, g.game_datetime_utc, v.venue_id, v.lat, v.lon
        FROM   fact_games g
        JOIN   dim_venues v ON v.venue_id = g.venue_id
        WHERE  g.as_of_date = ? AND g.game_date = ?
          AND  v.lat IS NOT NULL AND v.lon IS NOT NULL
        """,
        (as_of_date, game_date),
    ).fetchall()

    if not games:
        log.warning("No games with venue coordinates found for %s.", game_date)
        return

    for gid, game_dt_utc, venue_id, lat, lon in games:
        log.info("  Weather: game_id=%s venue_id=%s (%.4f, %.4f)", gid, venue_id, lat, lon)
        try:
            hourly_rows = _fetch_hourly_forecast(lat, lon, game_date)
        except Exception as e:
            log.warning("  Open-Meteo error for venue %s: %s", venue_id, e)
            time.sleep(1)
            continue

        # Upsert all hours into staging
        for hr in hourly_rows:
            conn.execute(
                """
                INSERT OR REPLACE INTO stg_weather_hourly
                    (as_of_date, venue_id, forecast_timestamp_utc,
                     temperature_f, wind_speed_mph, wind_direction_deg,
                     humidity_pct, precipitation_probability_pct,
                     pressure_hpa, raw_payload_json)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    as_of_date,
                    venue_id,
                    hr["forecast_timestamp_utc"],
                    hr["temperature_f"],
                    hr["wind_speed_mph"],
                    hr["wind_direction_deg"],
                    hr["humidity_pct"],
                    hr["precipitation_probability_pct"],
                    hr["pressure_hpa"],
                    hr["raw_payload_json"],
                ),
            )

        # Pick nearest-to-first-pitch row for fact_game_weather
        nearest = _nearest_forecast(hourly_rows, game_dt_utc)
        if nearest:
            conn.execute(
                """
                INSERT OR REPLACE INTO fact_game_weather
                    (as_of_date, game_id, venue_id, forecast_timestamp_utc,
                     temperature_f, wind_speed_mph, wind_direction_deg,
                     humidity_pct, precipitation_probability_pct, pressure_hpa)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    as_of_date,
                    gid,
                    venue_id,
                    nearest["forecast_timestamp_utc"],
                    nearest["temperature_f"],
                    nearest["wind_speed_mph"],
                    nearest["wind_direction_deg"],
                    nearest["humidity_pct"],
                    nearest["precipitation_probability_pct"],
                    nearest["pressure_hpa"],
                ),
            )

        conn.commit()
        # Open-Meteo free tier: be polite with rate limits
        time.sleep(0.5)

    log.info("Weather ingestion complete for %s.", game_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="data/mlb_pregame.db")
    parser.add_argument("--date",  help="Game date YYYY-MM-DD")
    parser.add_argument("--today", action="store_true")
    args = parser.parse_args()

    gdate = date.today().isoformat() if args.today else args.date
    if not gdate:
        parser.error("Provide --date YYYY-MM-DD or --today")

    conn = sqlite3.connect(args.db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    ingest_weather(conn, game_date=gdate, as_of_date=gdate)
    conn.close()
