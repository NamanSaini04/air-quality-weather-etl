"""Integration: fake air-quality APIs -> extractors -> PostgreSQL (raw + ops)."""
from datetime import date

import pytest

from src.common.http_client import ResilientHttpClient, RetryPolicy
from src.extract.open_meteo import ExtractionError
from src.extract.open_meteo_aq import extract_open_meteo_aq
from src.extract.openaq_api import OpenAQClient
from src.extract.openaq_measurements import SensorTarget, extract_openaq
from tests.fakes import FakeOpenAQ, FakeOpenMeteo, open_meteo_aq_payload

TODAY = date(2026, 11, 4)
START, END = date(2026, 11, 3), date(2026, 11, 4)


def http_for(fake):
    return ResilientHttpClient(transport=fake.transport(),
                               retry=RetryPolicy(max_attempts=2, base_delay_s=0, jitter_s=0),
                               sleep=lambda s: None)


def task(conn, run_id):
    return conn.execute("SELECT task_name, status, rows_extracted, details FROM ops.task_run "
                        "WHERE run_id=%s", (run_id,)).fetchone()


def count(conn, sql, *args):
    return conn.execute(sql, args).fetchone()[0]


# ---------------- Open-Meteo air quality ----------------

def test_open_meteo_aq_all_cities(conn, cities, run_id):
    fake = FakeOpenMeteo(payload_fn=open_meteo_aq_payload)
    extract_open_meteo_aq(conn, http_for(fake), cities, START, END, run_id, today=TODAY)
    name, status, rows, _ = task(conn, run_id)
    assert (name, status, rows) == ("extract_open_meteo_aq", "success", 5 * 48)
    assert count(conn, "SELECT count(*) FROM raw.api_response "
                       "WHERE source='open_meteo_air_quality'") == 5
    assert all(r.url.host == "air-quality-api.open-meteo.com" for r in fake.requests)


# ---------------- OpenAQ ----------------

def targets():
    return [SensorTarget("london", 1, "Site 1", "pm25", 11, "µg/m³"),
            SensorTarget("london", 1, "Site 1", "no2", 12, "ppb"),
            SensorTarget("delhi", 5, "Site 5", "pm25", 51, "µg/m³")]


def openaq_client(fake):
    return OpenAQClient("test-key", http=http_for(fake))


def test_openaq_happy_path_with_pagination(conn, run_id, monkeypatch):
    monkeypatch.setattr("src.extract.openaq_api.PAGE_LIMIT", 20)     # force 48h -> 3 pages
    fake = FakeOpenAQ({11: ("pm25", 48), 12: ("no2", 48), 51: ("pm25", 10)})
    details = extract_openaq(conn, openaq_client(fake), targets(), START, END, run_id)

    name, status, rows, _ = task(conn, run_id)
    assert (name, status, rows) == ("extract_openaq", "success", 48 + 48 + 10)
    assert details["stations"]["london/1"]["requests"] == 6       # 3 pages x 2 sensors
    assert count(conn, "SELECT count(*) FROM raw.api_response WHERE source='openaq'") == 7
    first = fake.requests[0]
    assert first.url.path == "/v3/sensors/11/hours"
    assert first.url.params["datetime_from"] == "2026-11-03T00:00:00Z"
    assert first.headers["X-API-Key"] == "test-key"


def test_openaq_rerun_does_not_duplicate_raw(conn, run_id):
    fake = FakeOpenAQ({11: ("pm25", 5), 12: ("no2", 5), 51: ("pm25", 5)})
    extract_openaq(conn, openaq_client(fake), targets(), START, END, run_id)
    extract_openaq(conn, openaq_client(fake), targets(), START, END, run_id)
    assert count(conn, "SELECT count(*) FROM raw.api_response WHERE source='openaq'") == 3


def test_identical_bodies_from_different_sensors_are_both_kept(conn, run_id):
    # Regression for the Phase 3b bug: OpenAQ results carry no sensor id, so two sensors
    # with no data return the same body. Both must be stored, one per sensor.
    fake = FakeOpenAQ({11: ("pm25", 0), 51: ("pm25", 0)})
    two = [targets()[0], targets()[2]]
    extract_openaq(conn, openaq_client(fake), two, START, END, run_id)
    keys = conn.execute("SELECT entity_key FROM raw.api_response ORDER BY 1").fetchall()
    assert keys == [("sensor:11",), ("sensor:51",)]


def test_openaq_one_station_down_keeps_others(conn, run_id):
    fake = FakeOpenAQ({11: ("pm25", 5), 12: ("no2", 5), 51: ("pm25", 5)}, failing={51: 503})
    with pytest.raises(ExtractionError, match="delhi/5"):
        extract_openaq(conn, openaq_client(fake), targets(), START, END, run_id)
    _, status, rows, details = task(conn, run_id)
    assert status == "partial" and rows == 10
    assert details["stations"]["delhi/5"]["status"] == "failed"
    assert count(conn, "SELECT count(*) FROM raw.api_response "
                       "WHERE entity_key IN ('sensor:11','sensor:12')") == 2


def test_openaq_stale_config_fails_loudly(conn, run_id):
    # config says sensor 51 is pm25, API says it's now o3
    fake = FakeOpenAQ({11: ("pm25", 5), 12: ("no2", 5), 51: ("o3", 5)})
    with pytest.raises(ExtractionError, match="re-run discover-stations"):
        extract_openaq(conn, openaq_client(fake), targets(), START, END, run_id)
    # the evidence is still in raw
    assert count(conn, "SELECT count(*) FROM raw.api_response WHERE entity_key='sensor:51'") == 1


def test_openaq_bad_key_body_is_stored(conn, run_id):
    fake = FakeOpenAQ({11: ("pm25", 5)}, failing={11: 401})
    with pytest.raises(ExtractionError, match="PermanentApiError"):
        extract_openaq(conn, openaq_client(fake), targets()[:1], START, END, run_id)
    assert count(conn, "SELECT http_status FROM raw.api_response") == 401
    assert len(fake.requests) == 1                     # 401 is never retried
