"""OpenAQ hourly measurements extractor (Phase 3b).

Reads config/stations.yaml (written by discover-stations) and, for every selected
sensor, pulls hourly averages from  GET /v3/sensors/{sensor_id}/hours.

Unit of failure isolation = one STATION. If one station errors, the others are still
saved; the task then raises so the orchestrator retries (safe: raw is de-duplicated).

Not done here (on purpose): unit conversion (ppb -> µg/m³), dedup of OpenAQ's
duplicate location records, AQI. Those are transform/load steps (Phases 4-6).
"""
from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import psycopg
import yaml

from src.common.config import PROJECT_ROOT, ConfigError
from src.common.http_client import ApiError, SchemaError
from src.common.run_tracking import TaskRecorder
from src.extract.open_meteo import ExtractionError
from src.extract.openaq_api import SOURCE, OpenAQClient, _results
from src.extract.raw_store import save_raw_response

log = logging.getLogger(__name__)

STATIONS_PATH = PROJECT_ROOT / "config" / "stations.yaml"
ENDPOINT = "sensors/hours"


@dataclass(frozen=True)
class SensorTarget:
    city_key: str
    openaq_location_id: int
    station_name: str
    parameter: str
    sensor_id: int
    units: str

    @property
    def station_key(self) -> str:
        return f"{self.city_key}/{self.openaq_location_id}"


def load_sensor_targets(path: Path | str = STATIONS_PATH,
                        city_keys: set[str] | None = None) -> list[SensorTarget]:
    """Flatten stations.yaml into one target per sensor.

    Cities marked `source: fallback_open_meteo` are skipped - Open-Meteo AQ covers them.
    """
    try:
        cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    except FileNotFoundError as exc:
        raise ConfigError(f"{path} not found - run `python main.py discover-stations` first") from exc

    targets = []
    for city_key, city in (cfg.get("cities") or {}).items():
        if city_keys and city_key not in city_keys:
            continue
        if city.get("source") != "openaq":
            log.info("skipping %s: source=%s", city_key, city.get("source"))
            continue
        for st in city.get("stations") or []:
            for param, sensor in (st.get("sensors") or {}).items():
                try:
                    targets.append(SensorTarget(
                        city_key, int(st["openaq_location_id"]), str(st.get("name", "")),
                        param, int(sensor["sensor_id"]), str(sensor.get("units", ""))))
                except (KeyError, TypeError, ValueError) as exc:
                    raise ConfigError(f"{path}: bad sensor entry under {city_key}: {exc!r}") from exc
    if not targets:
        raise ConfigError(f"{path}: no OpenAQ sensors selected for the requested cities")
    return targets


def window_params(start: date, end: date) -> dict:
    """[start 00:00Z, end+1 00:00Z) - whole days in UTC."""
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    return {"datetime_from": f"{start.isoformat()}T00:00:00Z",
            "datetime_to": f"{(end + timedelta(days=1)).isoformat()}T00:00:00Z"}


def validate_measurements(data: dict, expected_parameter: str) -> int:
    """Shape check for one page of /sensors/{id}/hours. Returns row count."""
    results = _results(data)
    for i, r in enumerate(results):
        if not isinstance(r, dict) or "value" not in r:
            raise SchemaError(f"result {i}: missing 'value'")
        param = ((r.get("parameter") or {}).get("name") or "").lower()
        if param and param != expected_parameter:
            raise SchemaError(f"sensor returned '{param}' but stations.yaml says "
                              f"'{expected_parameter}' - re-run discover-stations")
        if not ((r.get("period") or {}).get("datetimeTo") or {}).get("utc"):
            raise SchemaError(f"result {i}: missing period.datetimeTo.utc")
    return len(results)


def extract_openaq(conn: psycopg.Connection, client: OpenAQClient,
                   targets: list[SensorTarget], start: date, end: date,
                   run_id: uuid.UUID) -> dict:
    params = window_params(start, end)
    by_station: dict[str, list[SensorTarget]] = defaultdict(list)
    for t in targets:
        by_station[t.station_key].append(t)

    failures: dict[str, str] = {}
    with TaskRecorder(conn, run_id, "extract_openaq", SOURCE) as task:
        task.details = {"window": [start.isoformat(), end.isoformat()], "stations": {}}
        for station_key, sensors in by_station.items():
            stats = {"name": sensors[0].station_name, "sensors": len(sensors),
                     "rows": 0, "requests": 0, "new_raw": 0}
            try:
                for t in sensors:
                    rows, requests, new_raw = _extract_sensor(conn, client, t, params, run_id)
                    stats["rows"] += rows
                    stats["requests"] += requests
                    stats["new_raw"] += new_raw
                task.rows_extracted += stats["rows"]
                stats["status"] = "success"
            except ApiError as exc:
                failures[station_key] = f"{type(exc).__name__}: {exc}"
                stats.update(status="failed", error=failures[station_key])
                log.error("openaq failed for %s: %s", station_key, exc)
            task.details["stations"][station_key] = stats

        if failures:
            reasons = "; ".join(f"{k}: {v}" for k, v in sorted(failures.items()))
            raise ExtractionError(
                f"{len(failures)}/{len(by_station)} stations failed -> {reasons}")
    return task.details


def _extract_sensor(conn, client: OpenAQClient, t: SensorTarget, params: dict,
                    run_id) -> tuple[int, int, int]:
    entity = f"sensor:{t.sensor_id}"
    rows = requests = new_raw = 0
    pages = client.get_pages(f"/sensors/{t.sensor_id}/hours", params)
    while True:
        try:
            page_params, resp = next(pages)
        except StopIteration:
            break
        except ApiError as exc:
            if exc.response is not None:
                save_raw_response(conn, run_id=run_id, source=SOURCE, endpoint=ENDPOINT,
                                  entity_key=entity, params=params, response=exc.response)
            raise
        requests += 1
        new_raw += int(save_raw_response(conn, run_id=run_id, source=SOURCE,
                                         endpoint=ENDPOINT, entity_key=entity,
                                         params=page_params, response=resp))
        rows += validate_measurements(resp.data, t.parameter)
    log.info("%s %s (%s) %s: %d hourly rows, %d page(s)", t.city_key, t.station_name,
             t.parameter, entity, rows, requests)
    return rows, requests, new_raw
