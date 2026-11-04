"""Unit tests for weather request planning, parameters and response-shape checks."""
from datetime import date

import pytest

from src.common.config import load_cities
from src.common.http_client import SchemaError
from src.extract.raw_store import payload_hash
from src.extract.weather_api import (HOURLY_VARIABLES, VOLATILE_KEYS, RequestPlan,
                                     build_params, plan_requests, validate_payload)
from tests.fakes import open_meteo_payload

TODAY = date(2026, 11, 3)       # archive cutoff = 2026-10-29


# ---------------- request planning ----------------

def test_recent_window_uses_forecast():
    [p] = plan_requests(date(2026, 11, 2), date(2026, 11, 3), TODAY)
    assert (p.endpoint, p.start, p.end) == ("forecast", date(2026, 11, 2), date(2026, 11, 3))


def test_old_window_uses_archive():
    [p] = plan_requests(date(2026, 1, 1), date(2026, 1, 31), TODAY)
    assert p.endpoint == "archive"


def test_window_crossing_cutoff_is_split_without_gap_or_overlap():
    a, f = plan_requests(date(2026, 10, 20), date(2026, 11, 3), TODAY)
    assert (a.endpoint, a.start, a.end) == ("archive", date(2026, 10, 20), date(2026, 10, 28))
    assert (f.endpoint, f.start, f.end) == ("forecast", date(2026, 10, 29), date(2026, 11, 3))


def test_future_end_is_clipped_to_today():
    [p] = plan_requests(date(2026, 11, 3), date(2026, 11, 10), TODAY)
    assert p.end == TODAY


def test_fully_future_window_returns_nothing():
    assert plan_requests(date(2026, 11, 5), date(2026, 11, 6), TODAY) == []


def test_start_after_end_raises():
    with pytest.raises(ValueError):
        plan_requests(date(2026, 11, 3), date(2026, 11, 1), TODAY)


# ---------------- parameters ----------------

def test_params_request_utc_and_explicit_units():
    london = next(c for c in load_cities() if c.city_key == "london")
    params = build_params(london, RequestPlan("forecast", "u", date(2026, 11, 2),
                                              date(2026, 11, 3)))
    assert params["timezone"] == "GMT"
    assert params["temperature_unit"] == "celsius" and params["wind_speed_unit"] == "kmh"
    assert params["hourly"].split(",") == list(HOURLY_VARIABLES)
    assert (params["start_date"], params["end_date"]) == ("2026-11-02", "2026-11-03")


# ---------------- response shape ----------------

def payload():
    return open_meteo_payload(51.5, -0.12, date(2026, 11, 2), date(2026, 11, 3))


def test_valid_payload_counts_rows():
    assert validate_payload(payload()) == 48


def test_missing_variable_is_schema_error():
    p = payload()
    del p["hourly"]["pressure_msl"]
    with pytest.raises(SchemaError, match="pressure_msl"):
        validate_payload(p)


def test_array_length_mismatch_is_schema_error():
    p = payload()
    p["hourly"]["temperature_2m"].pop()
    with pytest.raises(SchemaError, match="length"):
        validate_payload(p)


def test_unit_change_is_schema_error():
    p = payload()
    p["hourly_units"]["temperature_2m"] = "°F"
    with pytest.raises(SchemaError, match="units"):
        validate_payload(p)


def test_non_utc_response_is_schema_error():
    p = payload()
    p["utc_offset_seconds"] = 19800
    with pytest.raises(SchemaError, match="UTC"):
        validate_payload(p)


def test_api_level_error_is_schema_error():
    with pytest.raises(SchemaError, match="bad"):
        validate_payload({"error": True, "reason": "bad"})


# ---------------- raw dedup hash ----------------

def test_hash_ignores_server_timing_but_not_data():
    a, b = payload(), payload()
    b["generationtime_ms"] = 99.9
    assert payload_hash(a, volatile_keys=VOLATILE_KEYS) == payload_hash(b, volatile_keys=VOLATILE_KEYS)
    assert payload_hash(a) != payload_hash(b)          # proves the volatile key matters
    b["hourly"]["temperature_2m"][0] = -5
    assert payload_hash(a, volatile_keys=VOLATILE_KEYS) != payload_hash(b, volatile_keys=VOLATILE_KEYS)
