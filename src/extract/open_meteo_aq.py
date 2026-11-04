"""Open-Meteo Air Quality extractor (Phase 3c) - the CONSISTENT baseline.

Why this exists (decision B): OpenAQ coverage differs by city - reference monitors in
London/Delhi/Mumbai, only low-cost sensors in Manchester/Birmingham (their government
monitors in OpenAQ stopped updating in May 2026). Comparing cities across different
instruments is unfair. Open-Meteo gives the SAME modelled method for every city, so
city rankings use this; OpenAQ supplies real measurements to compare against.

Caveat to state honestly: this is CAMS model output, not a measurement. Europe is
~11 km resolution; India comes from the global model (coarser, 3-hourly upstream).
"""
from __future__ import annotations

import uuid
from datetime import date

import psycopg

from src.common.config import City
from src.common.http_client import ResilientHttpClient
from src.extract.open_meteo import OpenMeteoSpec, RequestPlan, extract_open_meteo

SOURCE = "open_meteo_air_quality"
AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

HOURLY_VARIABLES = (
    "pm2_5", "pm10", "nitrogen_dioxide", "sulphur_dioxide", "ozone",
    "carbon_monoxide", "european_aqi", "us_aqi",
)
EXPECTED_UNITS = {
    "pm2_5": "µg/m³", "pm10": "µg/m³", "nitrogen_dioxide": "µg/m³",
    "sulphur_dioxide": "µg/m³", "ozone": "µg/m³", "carbon_monoxide": "µg/m³",
}


def plan_requests(start: date, end: date, today: date) -> list[RequestPlan]:
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    end = min(end, today)
    if start > end:
        return []
    return [RequestPlan("air_quality", AIR_QUALITY_URL, start, end)]


AQ_SPEC = OpenMeteoSpec(
    source=SOURCE,
    task_name="extract_open_meteo_aq",
    variables=HOURLY_VARIABLES,
    expected_units=EXPECTED_UNITS,
    planner=plan_requests,
)


def extract_open_meteo_aq(conn: psycopg.Connection, client: ResilientHttpClient,
                          cities: list[City], start: date, end: date, run_id: uuid.UUID,
                          today: date | None = None) -> dict:
    return extract_open_meteo(conn, client, AQ_SPEC, cities, start, end, run_id, today)
