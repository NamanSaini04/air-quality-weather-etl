"""Read raw responses from PostgreSQL and transform them (no loading yet - Phase 6).

Re-processing from raw is the point of the raw layer: if a transform rule changes,
we re-run this over stored responses instead of calling the APIs again.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
import yaml

from src.common.config import City, ConfigError
from src.extract.openaq_measurements import STATIONS_PATH, SensorTarget
from src.transform.air_quality_transform import (OPEN_METEO_SOURCE, OPENAQ_SOURCE,
                                                 StationInfo, transform_open_meteo_aq,
                                                 transform_openaq)
from src.transform.common import Rejection, TransformResult, dedupe_latest
from src.transform.weather_transform import SOURCE as WEATHER_SOURCE
from src.transform.weather_transform import transform_weather

log = logging.getLogger(__name__)

SOURCES = (WEATHER_SOURCE, OPEN_METEO_SOURCE, OPENAQ_SOURCE)


@dataclass
class SourceReport:
    source: str
    responses: int = 0
    result: TransformResult = field(default_factory=TransformResult)
    duplicates_dropped: int = 0

    def rejected_by_rule(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for r in self.result.rejected:
            out[r.rule_name] = out.get(r.rule_name, 0) + 1
        return out


def read_raw(conn: psycopg.Connection, source: str) -> list[tuple]:
    """Successful raw responses for one source: (response_id, entity_key, ingested_at, payload)."""
    return conn.execute(
        """SELECT response_id, entity_key, ingested_at, payload
           FROM raw.api_response
           WHERE source = %s AND http_status = 200 AND payload IS NOT NULL
           ORDER BY response_id""", (source,)).fetchall()


def load_station_infos(cities: list[City], path: Path | str = STATIONS_PATH
                       ) -> dict[int, StationInfo]:
    """sensor_id -> StationInfo, from stations.yaml."""
    by_key = {c.city_key: c for c in cities}
    try:
        cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    except FileNotFoundError as exc:
        raise ConfigError(f"{path} not found - run discover-stations first") from exc
    infos = {}
    for city_key, city_cfg in (cfg.get("cities") or {}).items():
        if city_key not in by_key:
            continue
        for st in city_cfg.get("stations") or []:
            for param, sensor in (st.get("sensors") or {}).items():
                target = SensorTarget(city_key, int(st["openaq_location_id"]),
                                      str(st.get("name", "")), param,
                                      int(sensor["sensor_id"]), str(sensor.get("units", "")))
                infos[target.sensor_id] = StationInfo(
                    target, by_key[city_key], float(st["latitude"]), float(st["longitude"]),
                    st.get("station_type", "low_cost_sensor"))
    return infos


def transform_source(conn: psycopg.Connection, source: str, cities: list[City],
                     station_infos: dict[int, StationInfo] | None = None) -> SourceReport:
    report = SourceReport(source)
    by_key = {c.city_key: c for c in cities}
    combined = TransformResult()

    for response_id, entity, ingested_at, payload in read_raw(conn, source):
        report.responses += 1
        try:
            if source in (WEATHER_SOURCE, OPEN_METEO_SOURCE):
                city = by_key.get(entity.split(":", 1)[1])
                if city is None:
                    combined.rejected.append(Rejection(source, entity, "unknown_entity",
                                                       "city not in cities.yaml", {}))
                    continue
                fn = transform_weather if source == WEATHER_SOURCE else transform_open_meteo_aq
                part = fn(payload, city, response_id, ingested_at)
            else:
                info = (station_infos or {}).get(int(entity.split(":", 1)[1]))
                if info is None:
                    combined.rejected.append(Rejection(source, entity, "unknown_sensor",
                                                       "sensor not in stations.yaml", {}))
                    continue
                part = transform_openaq(payload, info, response_id, ingested_at)
        except (KeyError, TypeError, ValueError) as exc:
            # A raw row whose shape the transform can't read (e.g. stored during a
            # SchemaError). Reject the whole response, keep going with the others.
            combined.rejected.append(Rejection(source, entity, "unreadable_payload",
                                               f"{type(exc).__name__}: {exc}",
                                               {"response_id": response_id}))
            continue
        combined.extend(part)

    if source == WEATHER_SOURCE:
        key = lambda r: (r.city_key, r.observed_at_utc)                      # noqa: E731
    else:
        key = lambda r: (r.source, r.source_station_id, r.pollutant_code,     # noqa: E731
                         r.observed_at_utc)
    combined.kept, report.duplicates_dropped = dedupe_latest(
        combined.kept, key, version=lambda r: (r.ingested_at, r.raw_response_id))
    report.result = combined
    log.info("%s: %d responses -> %s, %d duplicates dropped", source, report.responses,
             combined.counts, report.duplicates_dropped)
    return report
