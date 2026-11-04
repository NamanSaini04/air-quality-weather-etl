"""Integration: fake API -> extractor -> PostgreSQL (raw + ops tables)."""
from datetime import date

import pytest

from src.common.http_client import ResilientHttpClient, RetryPolicy
from src.extract.weather_api import ExtractionError, extract_weather
from tests.fakes import FakeOpenMeteo

TODAY = date(2026, 11, 3)
START, END = date(2026, 11, 2), date(2026, 11, 3)


def client_for(fake):
    return ResilientHttpClient(transport=fake.transport(),
                               retry=RetryPolicy(max_attempts=3, base_delay_s=0, jitter_s=0),
                               sleep=lambda s: None)


def pick(cities, *keys):
    return [c for c in cities if c.city_key in keys]


def lat(cities, key):
    return str(next(c.latitude for c in cities if c.city_key == key))


def count(conn, sql, *args):
    return conn.execute(sql, args).fetchone()[0]


def task_row(conn, run_id):
    return conn.execute(
        "SELECT status, rows_extracted, error_message, details FROM ops.task_run "
        "WHERE run_id = %s", (run_id,)).fetchone()


def test_happy_path_stores_raw_and_logs_task(conn, cities, run_id):
    fake = FakeOpenMeteo()
    two = pick(cities, "london", "delhi")
    details = extract_weather(conn, client_for(fake), two, START, END, run_id, today=TODAY)

    assert len(fake.requests) == 2
    assert count(conn, "SELECT count(*) FROM raw.api_response WHERE run_id=%s", run_id) == 2
    status, rows, error, task_details = task_row(conn, run_id)
    assert (status, rows, error) == ("success", 96, None)       # 2 cities x 48 hours
    assert task_details["cities"]["delhi"]["status"] == "success"
    assert details["cities"]["london"]["rows"] == 48


def test_rerun_same_data_does_not_duplicate_raw(conn, cities, run_id):
    london = pick(cities, "london")
    # second response has a different generationtime_ms but identical data
    fake = FakeOpenMeteo({lat(cities, "london"): ["ok", "ok_new_gen"]})
    extract_weather(conn, client_for(fake), london, START, END, run_id, today=TODAY)
    extract_weather(conn, client_for(fake), london, START, END, run_id, today=TODAY)
    assert count(conn, "SELECT count(*) FROM raw.api_response") == 1


def test_transient_errors_are_retried_transparently(conn, cities, run_id):
    fake = FakeOpenMeteo({lat(cities, "london"): [503, "timeout", "ok"]})
    extract_weather(conn, client_for(fake), pick(cities, "london"), START, END, run_id,
                    today=TODAY)
    assert len(fake.requests) == 3
    assert task_row(conn, run_id)[0] == "success"


def test_one_city_down_is_partial_and_others_are_kept(conn, cities, run_id):
    fake = FakeOpenMeteo({lat(cities, "delhi"): [500, 500, 500]})   # all 3 attempts fail
    with pytest.raises(ExtractionError, match="delhi"):
        extract_weather(conn, client_for(fake), pick(cities, "london", "delhi"),
                        START, END, run_id, today=TODAY)

    status, rows, error, details = task_row(conn, run_id)
    assert status == "partial" and rows == 48
    assert "delhi" in error
    assert details["cities"]["delhi"]["status"] == "failed"
    assert details["cities"]["london"]["status"] == "success"
    # London's raw data survived Delhi's failure
    assert count(conn, "SELECT count(*) FROM raw.api_response WHERE entity_key=%s",
                 "location:london") == 1


def test_all_cities_down_is_failed(conn, cities, run_id):
    fake = FakeOpenMeteo({lat(cities, "london"): [503] * 3})
    with pytest.raises(ExtractionError):
        extract_weather(conn, client_for(fake), pick(cities, "london"), START, END, run_id,
                        today=TODAY)
    assert task_row(conn, run_id)[0] == "failed"


def test_invalid_json_is_kept_as_text_for_debugging(conn, cities, run_id):
    fake = FakeOpenMeteo({lat(cities, "london"): ["bad_json"]})
    with pytest.raises(ExtractionError, match="InvalidResponseError"):
        extract_weather(conn, client_for(fake), pick(cities, "london"), START, END, run_id,
                        today=TODAY)
    payload, text = conn.execute(
        "SELECT payload, payload_text FROM raw.api_response").fetchone()
    assert payload is None and "Gateway error" in text


def test_schema_change_fails_loudly_but_raw_is_kept(conn, cities, run_id):
    def drop_humidity(p):
        del p["hourly"]["relative_humidity_2m"]
        return p
    fake = FakeOpenMeteo({lat(cities, "london"): [drop_humidity]})
    with pytest.raises(ExtractionError, match="SchemaError"):
        extract_weather(conn, client_for(fake), pick(cities, "london"), START, END, run_id,
                        today=TODAY)
    assert count(conn, "SELECT count(*) FROM raw.api_response") == 1


def test_bad_request_is_not_retried_and_body_is_stored(conn, cities, run_id):
    fake = FakeOpenMeteo({lat(cities, "london"): [400]})
    with pytest.raises(ExtractionError, match="PermanentApiError"):
        extract_weather(conn, client_for(fake), pick(cities, "london"), START, END, run_id,
                        today=TODAY)
    assert len(fake.requests) == 1
    assert count(conn, "SELECT http_status FROM raw.api_response") == 400


def test_window_crossing_archive_cutoff_makes_two_requests(conn, cities, run_id):
    fake = FakeOpenMeteo()
    extract_weather(conn, client_for(fake), pick(cities, "london"),
                    date(2026, 10, 27), END, run_id, today=TODAY)
    hosts = sorted(r.url.host for r in fake.requests)
    assert hosts == ["api.open-meteo.com", "archive-api.open-meteo.com"]
    assert task_row(conn, run_id)[1] == 8 * 24        # 27 Oct .. 3 Nov = 8 days, no gaps
