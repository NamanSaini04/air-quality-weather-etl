"""Shared Open-Meteo extraction logic (weather AND air quality).

Both Open-Meteo APIs answer with the same shape: per-city `hourly` arrays plus
`hourly_units`. Only the URL, the variables and the expected units differ, so each
dataset is described by an OpenMeteoSpec and runs through ONE extraction loop.
Adding a third Open-Meteo dataset later = a new spec, not new code.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Callable

import psycopg

from src.common.config import City
from src.common.http_client import (ApiError, ApiResponse, ResilientHttpClient,
                                    SchemaError)
from src.common.run_tracking import TaskRecorder
from src.extract.raw_store import save_raw_response

log = logging.getLogger(__name__)

VOLATILE_KEYS = ("generationtime_ms",)


class ExtractionError(Exception):
    """One or more entities failed. Raised AFTER the others were saved."""


@dataclass(frozen=True)
class RequestPlan:
    endpoint: str          # short label stored in raw.api_response.endpoint
    url: str
    start: date
    end: date


@dataclass(frozen=True)
class OpenMeteoSpec:
    source: str                                  # raw.api_response.source
    task_name: str                               # ops.task_run.task_name
    variables: tuple[str, ...]
    expected_units: dict[str, str]               # only units we convert/depend on
    planner: Callable[[date, date, date], list[RequestPlan]]
    extra_params: dict = field(default_factory=dict)


def normalise_unit(unit: str | None) -> str:
    """'µ' (micro sign, U+00B5) and 'μ' (Greek mu, U+03BC) look identical but differ.
    Different APIs use different ones; compare them as equal."""
    return (unit or "").replace("μ", "µ").strip()


def build_params(spec: OpenMeteoSpec, city: City, plan: RequestPlan) -> dict:
    return {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "start_date": plan.start.isoformat(),
        "end_date": plan.end.isoformat(),
        "hourly": ",".join(spec.variables),
        "timezone": "GMT",                 # store UTC; local time is derived later
        **spec.extra_params,
    }


def validate_hourly(spec: OpenMeteoSpec, data: dict) -> int:
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

    missing = [v for v in spec.variables if v not in hourly]
    if missing:
        raise SchemaError(f"missing hourly variables: {missing}")
    bad_len = [v for v in spec.variables if len(hourly[v]) != n]
    if bad_len:
        raise SchemaError(f"array length != {n} for: {bad_len}")

    units = data.get("hourly_units") or {}
    wrong = {v: units.get(v) for v, u in spec.expected_units.items()
             if normalise_unit(units.get(v)) != normalise_unit(u)}
    if wrong:
        raise SchemaError(f"unexpected units: {wrong} (expected {spec.expected_units})")
    return n


def extract_open_meteo(conn: psycopg.Connection, client: ResilientHttpClient,
                       spec: OpenMeteoSpec, cities: list[City], start: date, end: date,
                       run_id: uuid.UUID, today: date | None = None) -> dict:
    """Fetch one Open-Meteo dataset for every city. One city failing doesn't stop others.

    Raises ExtractionError at the end if any city failed, so the orchestrator marks the
    task failed and retries. Retrying is safe: identical payloads are de-duplicated.
    """
    today = today or datetime.now(timezone.utc).date()
    plans = spec.planner(start, end, today)
    failures: dict[str, str] = {}

    with TaskRecorder(conn, run_id, spec.task_name, spec.source) as task:
        task.details = {"window": [start.isoformat(), end.isoformat()], "cities": {}}
        for city in cities:
            entity = f"location:{city.city_key}"
            stats = {"rows": 0, "requests": 0, "new_raw": 0}
            try:
                for plan in plans:
                    rows, is_new = _fetch_and_store(conn, client, spec, run_id, plan, entity,
                                                    build_params(spec, city, plan))
                    stats["requests"] += 1
                    stats["new_raw"] += int(is_new)
                    stats["rows"] += rows
                task.rows_extracted += stats["rows"]
                stats["status"] = "success"
            except ApiError as exc:
                failures[city.city_key] = f"{type(exc).__name__}: {exc}"
                stats.update(status="failed", error=failures[city.city_key])
                log.error("%s failed for %s: %s", spec.task_name, city.city_key, exc)
            task.details["cities"][city.city_key] = stats

        if failures:
            reasons = "; ".join(f"{k}: {v}" for k, v in sorted(failures.items()))
            raise ExtractionError(f"{len(failures)}/{len(cities)} cities failed -> {reasons}")
    return task.details


def _fetch_and_store(conn, client, spec: OpenMeteoSpec, run_id, plan: RequestPlan,
                     entity: str, params: dict) -> tuple[int, bool]:
    """Fetch one request, ALWAYS store what the server returned, then validate."""
    try:
        resp: ApiResponse = client.get_json(plan.url, params)
    except ApiError as exc:
        if exc.response is not None:          # server answered: keep the evidence
            save_raw_response(conn, run_id=run_id, source=spec.source, endpoint=plan.endpoint,
                              entity_key=entity, params=params, response=exc.response)
        raise
    is_new = save_raw_response(conn, run_id=run_id, source=spec.source, endpoint=plan.endpoint,
                               entity_key=entity, params=params, response=resp,
                               volatile_keys=VOLATILE_KEYS)
    try:
        rows = validate_hourly(spec, resp.data)
    except SchemaError as exc:
        exc.response = resp
        raise
    log.info("%s %s %s..%s: %d hourly rows (%s, attempts=%d)", entity, plan.endpoint,
             plan.start, plan.end, rows, "new" if is_new else "duplicate", resp.attempts)
    return rows, is_new
