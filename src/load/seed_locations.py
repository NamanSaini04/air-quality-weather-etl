"""Seed core.dim_location from config/cities.yaml.

This is the first UPSERT in the project. Pattern:
    INSERT ... ON CONFLICT (natural_key) DO UPDATE SET ...
Running it once or ten times leaves the table in the same state (idempotent).
"""
from __future__ import annotations

import logging

import psycopg

from src.common.config import City

log = logging.getLogger(__name__)

UPSERT_LOCATION_SQL = """
INSERT INTO core.dim_location
    (city_key, city_name, region, country_code, latitude, longitude, timezone, climate_zone)
VALUES
    (%(city_key)s, %(city_name)s, %(region)s, %(country_code)s,
     %(latitude)s, %(longitude)s, %(timezone)s, %(climate_zone)s)
ON CONFLICT (city_key) DO UPDATE SET
    city_name    = EXCLUDED.city_name,
    region       = EXCLUDED.region,
    country_code = EXCLUDED.country_code,
    latitude     = EXCLUDED.latitude,
    longitude    = EXCLUDED.longitude,
    timezone     = EXCLUDED.timezone,
    climate_zone = EXCLUDED.climate_zone,
    updated_at   = now()
-- Only touch the row if something actually changed (keeps updated_at honest).
WHERE (core.dim_location.city_name, core.dim_location.region,
       core.dim_location.country_code, core.dim_location.latitude,
       core.dim_location.longitude, core.dim_location.timezone,
       core.dim_location.climate_zone)
   IS DISTINCT FROM
      (EXCLUDED.city_name, EXCLUDED.region, EXCLUDED.country_code,
       EXCLUDED.latitude, EXCLUDED.longitude, EXCLUDED.timezone,
       EXCLUDED.climate_zone)
"""


def seed_locations(conn: psycopg.Connection, cities: list[City]) -> int:
    """Upsert cities. Returns number of rows inserted or changed."""
    changed = 0
    with conn.transaction(), conn.cursor() as cur:
        for city in cities:
            cur.execute(UPSERT_LOCATION_SQL, city.__dict__)
            changed += cur.rowcount
    log.info("dim_location: %d of %d cities inserted/updated", changed, len(cities))
    return changed
