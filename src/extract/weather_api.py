"""Open-Meteo weather extractor.

Two endpoints, chosen by date:
  * archive  (ERA5 reanalysis)  - stable history, but lags real time by ~5 days
  * forecast (weather models)   - covers the last 92 days up to now, but values for
                                  recent days are model output and can be revised

Rule used here: dates older than ARCHIVE_LAG_DAYS -> archive, newer -> forecast.
A window that crosses the boundary becomes two requests.

The extractor does NOT clean values. It fetches, stores raw, checks the response
SHAPE, and counts rows. Value checks belong to validation (Phase 10).
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import psycopg

from src.common.config import City
from src.common.http_client import (ApiError, ApiResponse, ResilientHttpClient,
                                    SchemaError)
from src.common.run_tracking import TaskRecorder
from src.extract.raw_store import save_raw_response

log = logging.getLogger(__name__)

SOURCE = "open_meteo_weather"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
ARCHIVE_LAG_DAYS = 5
FORECAST_MAX_PAST_DAYS = 92
VOLATILE_KEYS = ("generationtime_ms",)

HOURLY_VARIABLES = (
    "temperature_2m", "relative_humidity_2m", "pressure_msl", "precipitation",
    "rain", "wind_speed_10m", "wind_direction_10m", "weather_code",
)
# Units we depend on later. If Open-Meteo changes one, fail loudly instead of
# silently loading Fahrenheit as Celsius.
EXPECTED_UNITS = {
    "temperature_2m": "°C",
    "pressure_msl": "hPa",
    "precipitation": "mm",
    "wind_speed_10m": "km/h",
}


class ExtractionError(Exception):
    """One or more entities failed. Raised AFTER the others were saved."""


@dataclass(frozen=True)
class RequestPlan:
    endpoint: str          # 'forecast' or 'archive'
    url: str
    start: date
    end: date


def plan_requests(start: date, end: date, today: date) -> list[RequestPlan]:
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    end = min(end, today)                                  # never ask for future days
    if start > end:
        return []
    archive_cutoff = today - timedelta(days=ARCHIVE_LAG_DAYS)   # first forecast-only day
    plans = []
    if start < archive_cutoff:
        plans.append(RequestPlan("archive", ARCHIVE_URL, start,
                                 min(end, archive_cutoff - timedelta(days=1))))
    if end >= archive_cutoff:
        fc_start = max(start, archive_cutoff)
        if fc_start < today - timedelta(days=FORECAST_MAX_PAST_DAYS):
            raise ValueError("forecast endpoint only covers the last 92 days")
        plans.append(RequestPlan("forecast", FORECAST_URL, fc_start, end))
    return plans


def build_params(city: City, plan: RequestPlan) -> dict:
    return {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "start_date": plan.start.isoformat(),
        "end_date": plan.end.isoformat(),
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "GMT",                 # store UTC; local time is derived later
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
    }


def validate_payload(data: dict) -> int:
    """Check the response shape. Returns number of hourly rows."""
    if not isinstance(data, dict):
        raise SchemaError("payload is not a JSON object")
    if data.get("error"):
        raise SchemaError(f"API error: {data.get('reason')}")
    if data.get("utc_offset_seconds", 0) != 0:
        raise SchemaError(f"expected UTC, got offset {data.get('utc_offset_seconds')}s")

    hourly = data.get("hourly")
    if not isinstance(hourly, dict) or not isinstance(hourly.get("time"), list):
        raise SchemaError("missing 'hourly.time'")
    n = len(hourly["time"])

    missing = [v for v in HOURLY_VARIABLES if v not in hourly]
    if missing:
        raise SchemaError(f"missing hourly variables: {missing}")
    bad_len = [v for v in HOURLY_VARIABLES if len(hourly[v]) != n]
    if bad_len:
        raise SchemaError(f"array length != {n} for: {bad_len}")

    units = data.get("hourly_units") or {}
    wrong = {v: units.get(v) for v, u in EXPECTED_UNITS.items() if units.get(v) != u}
    if wrong:
        raise SchemaError(f"unexpected units: {wrong} (expected {EXPECTED_UNITS})")
    return n


def extract_weather(conn: psycopg.Connection, client: ResilientHttpClient,
                    cities: list[City], start: date, end: date, run_id: uuid.UUID,
                    today: date | None = None) -> dict:
    """Fetch weather for every city. One city failing does not stop the others.

    Returns per-city stats. Raises ExtractionError at the end if any city failed,
    so the orchestrator marks the task failed and retries. Retrying is safe:
    identical raw responses are de-duplicated by payload hash.
    """
    today = today or datetime.now(timezone.utc).date()
    plans = plan_requests(start, end, today)
    failures: dict[str, str] = {}

    with TaskRecorder(conn, run_id, "extract_weather", SOURCE) as task:
        task.details = {"window": [start.isoformat(), end.isoformat()], "cities": {}}
        for city in cities:
            entity = f"location:{city.city_key}"
            city_stats = {"rows": 0, "requests": 0, "new_raw": 0}
            try:
                for plan in plans:
                    params = build_params(city, plan)
                    response = _fetch_and_store(conn, client, run_id, plan, entity, params)
                    city_stats["requests"] += 1
                    city_stats["new_raw"] += int(response[1])
                    city_stats["rows"] += response[0]
                task.rows_extracted += city_stats["rows"]
                city_stats["status"] = "success"
            except ApiError as exc:
                failures[city.city_key] = f"{type(exc).__name__}: {exc}"
                city_stats.update(status="failed", error=failures[city.city_key])
                log.error("weather extract failed for %s: %s", city.city_key, exc)
            task.details["cities"][city.city_key] = city_stats

        if failures:
            reasons = "; ".join(f"{k}: {v}" for k, v in sorted(failures.items()))
            raise ExtractionError(f"{len(failures)}/{len(cities)} cities failed -> {reasons}")
    return task.details


def _fetch_and_store(conn, client, run_id, plan: RequestPlan, entity: str,
                     params: dict) -> tuple[int, bool]:
    """Fetch one request, ALWAYS store what the server returned, then validate."""
    try:
        resp: ApiResponse = client.get_json(plan.url, params)
    except ApiError as exc:
        if exc.response is not None:          # server answered: keep the evidence
            save_raw_response(conn, run_id=run_id, source=SOURCE, endpoint=plan.endpoint,
                              entity_key=entity, params=params, response=exc.response)
        raise
    is_new = save_raw_response(conn, run_id=run_id, source=SOURCE, endpoint=plan.endpoint,
                               entity_key=entity, params=params, response=resp,
                               volatile_keys=VOLATILE_KEYS)
    try:
        rows = validate_payload(resp.data)
    except SchemaError as exc:
        exc.response = resp
        raise
    log.info("%s %s %s..%s: %d hourly rows (%s, attempts=%d)", entity, plan.endpoint,
             plan.start, plan.end, rows, "new" if is_new else "duplicate", resp.attempts)
    return rows, is_new
