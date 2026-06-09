"""
ingest_weather.py
-----------------
Pulls hourly weather forecasts for each scheduled game venue from the
free Open-Meteo API and populates stg_weather_hourly + fact_game_weather.

Works with both SQLite (local) and Supabase (cloud) via utils/db.py.

Free API docs: https://open-meteo.com/en/docs
No API key required for the free tier (up to 10,000 calls/day).

Usage:
    python ingest/ingest_weather.py --date 2025-04-15
    python ingest/ingest_weather.py --today
"""
import json
import time
import logging
import argparse
from datetime import date, datetime, timezone
import urllib.request
import urllib.parse
import urllib.error

from utils.db import get_connection, DB_BACKEND

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARS = [
    "temperature_2m",
    "windspeed_10m",
    "winddirection_10m",
    "relativehumidity_2m",
    "precipitation_probability",
    "surface_pressure",
]


# ── Unit conversions ───────────────────────────────────────────────────────

def _c_to_f(c):   return round(c * 9/5 + 32, 1) if c is not None else None
def _kmh_to_mph(k): return round(k * 0.621371, 1) if k is not None else None


# ── HTTP helper ────────────────────────────────────────────────────────────

def _get(url: str, params: dict) -> dict:
    qs  = urllib.parse.urlencode(params)
    req = urllib.request.Request(f"{url}?{qs}")
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode())


def _safe_float(lst: list):
    if not lst:
        return None
    v = lst[0]
    try:
        f = float(v)
        return None if f != f else f
    except (TypeError, ValueError):
        return None


def _fetch_hourly_forecast(lat: float, lon: float, forecast_date: str) -> list:
    params = {
        "latitude":         round(lat, 4),
        "longitude":        round(lon, 4),
        "hourly":           ",".join(HOURLY_VARS),
        "temperature_unit": "celsius",
        "windspeed_unit":   "kmh",
        "timezone":         "UTC",
        "start_date":       forecast_date,
        "end_date":         forecast_date,
    }
    data   = _get(OPEN_METEO_URL, params)
    hourly = data.get("hourly", {})
    times  = hourly.get("time", [])
    rows   = []
    for i, ts in enumerate(times):
        rows.append({
            "forecast_timestamp_utc":       ts + ":00Z",
            "temperature_f":                _c_to_f(_safe_float(hourly.get("temperature_2m", [])[i:i+1])),
            "wind_speed_mph":               _kmh_to_mph(_safe_float(hourly.get("windspeed_10m", [])[i:i+1])),
            "wind_direction_deg":           _safe_float(hourly.get("winddirection_10m", [])[i:i+1]),
            "humidity_pct":                 _safe_float(hourly.get("relativehumidity_2m", [])[i:i+1]),
            "precipitation_probability_pct":_safe_float(hourly.get("precipitation_probability", [])[i:i+1]),
            "pressure_hpa":                 _safe_float(hourly.get("surface_pressure", [])[i:i+1]),
            "raw_payload_json":             json.dumps(data),
        })
    return rows


def _nearest_forecast(rows: list, target_utc: str):
    if not rows or not target_utc:
        return rows[0] if rows else None
    try:
        target = datetime.fromisoformat(target_utc.replace("Z", "+00:00"))
    except ValueError:
        return rows[0] if rows else None
    best, best_delta = None, None
    for r in rows:
        try:
            ts    = datetime.fromisoformat(r["forecast_timestamp_utc"].replace("Z", "+00:00"))
            delta = abs((ts - target).total_seconds())
            if best_delta is None or delta < best_delta:
                best, best_delta = r, delta
        except ValueError:
            continue
    return best


# ── SQL helpers ────────────────────────────────────────────────────────────

def _upsert_stg_weather_sql():
    if DB_BACKEND == "supabase":
        return """
            INSERT INTO stg_weather_hourly
                (as_of_date, venue_id, forecast_timestamp_utc,
                 temperature_f, wind_speed_mph, wind_direction_deg,
                 humidity_pct, precipitation_probability_pct,
                 pressure_hpa, raw_payload_json)
            VALUES
                (:as_of_date,:venue_id,:forecast_timestamp_utc,
                 :temperature_f,:wind_speed_mph,:wind_direction_deg,
                 :humidity_pct,:precipitation_probability_pct,
                 :pressure_hpa,:raw_payload_json)
            ON CONFLICT (as_of_date, venue_id, forecast_timestamp_utc) DO UPDATE SET
                temperature_f                 = EXCLUDED.temperature_f,
                wind_speed_mph                = EXCLUDED.wind_speed_mph,
                wind_direction_deg            = EXCLUDED.wind_direction_deg,
                humidity_pct                  = EXCLUDED.humidity_pct,
                precipitation_probability_pct = EXCLUDED.precipitation_probability_pct,
                pressure_hpa                  = EXCLUDED.pressure_hpa,
                raw_payload_json              = EXCLUDED.raw_payload_json
        """
    return """
        INSERT OR REPLACE INTO stg_weather_hourly
            (as_of_date, venue_id, forecast_timestamp_utc,
             temperature_f, wind_speed_mph, wind_direction_deg,
             humidity_pct, precipitation_probability_pct,
             pressure_hpa, raw_payload_json)
        VALUES
            (:as_of_date,:venue_id,:forecast_timestamp_utc,
             :temperature_f,:wind_speed_mph,:wind_direction_deg,
             :humidity_pct,:precipitation_probability_pct,
             :pressure_hpa,:raw_payload_json)
    """

def _upsert_fact_weather_sql():
    if DB_BACKEND == "supabase":
        return """
            INSERT INTO fact_game_weather
                (as_of_date, game_id, venue_id, forecast_timestamp_utc,
                 temperature_f, wind_speed_mph, wind_direction_deg,
                 humidity_pct, precipitation_probability_pct, pressure_hpa)
            VALUES
                (:as_of_date,:game_id,:venue_id,:forecast_timestamp_utc,
                 :temperature_f,:wind_speed_mph,:wind_direction_deg,
                 :humidity_pct,:precipitation_probability_pct,:pressure_hpa)
            ON CONFLICT (as_of_date, game_id) DO UPDATE SET
                forecast_timestamp_utc        = EXCLUDED.forecast_timestamp_utc,
                temperature_f                 = EXCLUDED.temperature_f,
                wind_speed_mph                = EXCLUDED.wind_speed_mph,
                wind_direction_deg            = EXCLUDED.wind_direction_deg,
                humidity_pct                  = EXCLUDED.humidity_pct,
                precipitation_probability_pct = EXCLUDED.precipitation_probability_pct,
                pressure_hpa                  = EXCLUDED.pressure_hpa
        """
    return """
        INSERT OR REPLACE INTO fact_game_weather
            (as_of_date, game_id, venue_id, forecast_timestamp_utc,
             temperature_f, wind_speed_mph, wind_direction_deg,
             humidity_pct, precipitation_probability_pct, pressure_hpa)
        VALUES
            (:as_of_date,:game_id,:venue_id,:forecast_timestamp_utc,
             :temperature_f,:wind_speed_mph,:wind_direction_deg,
             :humidity_pct,:precipitation_probability_pct,:pressure_hpa)
    """


# ── Core ingest function ───────────────────────────────────────────────────

def ingest_weather(conn, game_date: str, as_of_date: str) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT g.game_id, g.game_datetime_utc, v.venue_id, v.lat, v.lon
        FROM   fact_games g
        JOIN   dim_venues v ON v.venue_id = g.venue_id
        WHERE  g.as_of_date = :aod AND g.game_date = :gd
          AND  v.lat IS NOT NULL AND v.lon IS NOT NULL
        """,
        {"aod": as_of_date, "gd": game_date},
    )
    games = cur.fetchall()

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

        for hr in hourly_rows:
            conn.execute(_upsert_stg_weather_sql(), {
                "as_of_date":                   as_of_date,
                "venue_id":                     venue_id,
                "forecast_timestamp_utc":       hr["forecast_timestamp_utc"],
                "temperature_f":                hr["temperature_f"],
                "wind_speed_mph":               hr["wind_speed_mph"],
                "wind_direction_deg":           hr["wind_direction_deg"],
                "humidity_pct":                 hr["humidity_pct"],
                "precipitation_probability_pct":hr["precipitation_probability_pct"],
                "pressure_hpa":                 hr["pressure_hpa"],
                "raw_payload_json":             hr["raw_payload_json"],
            })

        nearest = _nearest_forecast(hourly_rows, game_dt_utc)
        if nearest:
            conn.execute(_upsert_fact_weather_sql(), {
                "as_of_date":                   as_of_date,
                "game_id":                      gid,
                "venue_id":                     venue_id,
                "forecast_timestamp_utc":       nearest["forecast_timestamp_utc"],
                "temperature_f":                nearest["temperature_f"],
                "wind_speed_mph":               nearest["wind_speed_mph"],
                "wind_direction_deg":           nearest["wind_direction_deg"],
                "humidity_pct":                 nearest["humidity_pct"],
                "precipitation_probability_pct":nearest["precipitation_probability_pct"],
                "pressure_hpa":                 nearest["pressure_hpa"],
            })

        conn.commit()
        time.sleep(0.5)

    log.info("Weather ingestion complete for %s.", game_date)


# ── Entrypoint ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",  help="Game date YYYY-MM-DD")
    parser.add_argument("--today", action="store_true")
    args = parser.parse_args()

    gdate = date.today().isoformat() if args.today else args.date
    if not gdate:
        parser.error("Provide --date YYYY-MM-DD or --today")

    with get_connection() as conn:
        if DB_BACKEND == "sqlite":
            conn.execute("PRAGMA journal_mode=WAL;")
        ingest_weather(conn, game_date=gdate, as_of_date=gdate)