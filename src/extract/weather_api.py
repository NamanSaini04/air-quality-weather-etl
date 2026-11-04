"""Open-Meteo weather extractor.

Two endpoints, chosen by date:
  * archive  (ERA5 reanalysis)  - stable history, but lags real time by ~5 days
  * forecast (weather models)   - covers the last 92 days up to now, but values for
                                  recent days are model output and can be revised

Rule used here: dates older than ARCHIVE_LAG_DAYS -> archive, newer -> forecast.
A window that crosses the boundary becomes two requests.

The extraction loop itself is shared with air quality (see open_meteo.py).
"""
from __future__ import annotations

import uuid
from datetime import date, timedelta

import psycopg

from src.common.config import City
from src.common.http_client import ResilientHttpClient
from src.extract.open_meteo import (VOLATILE_KEYS, ExtractionError, OpenMeteoSpec,  # noqa: F401
                                    RequestPlan, extract_open_meteo)
from src.extract import open_meteo

SOURCE = "open_meteo_weather"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
ARCHIVE_LAG_DAYS = 5
FORECAST_MAX_PAST_DAYS = 92

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


WEATHER_SPEC = OpenMeteoSpec(
    source=SOURCE,
    task_name="extract_weather",
    variables=HOURLY_VARIABLES,
    expected_units=EXPECTED_UNITS,
    planner=plan_requests,
    extra_params={"temperature_unit": "celsius", "wind_speed_unit": "kmh",
                  "precipitation_unit": "mm"},
)


def build_params(city: City, plan: RequestPlan) -> dict:
    return open_meteo.build_params(WEATHER_SPEC, city, plan)


def validate_payload(data: dict) -> int:
    return open_meteo.validate_hourly(WEATHER_SPEC, data)


def extract_weather(conn: psycopg.Connection, client: ResilientHttpClient,
                    cities: list[City], start: date, end: date, run_id: uuid.UUID,
                    today: date | None = None) -> dict:
    return extract_open_meteo(conn, client, WEATHER_SPEC, cities, start, end, run_id, today)
