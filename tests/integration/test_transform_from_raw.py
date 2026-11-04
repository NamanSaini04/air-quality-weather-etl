"""Integration: raw.api_response rows -> transform_source -> counts and dedup."""
import json
from datetime import date, datetime, timezone

from src.common.http_client import ApiResponse
from src.extract.raw_store import save_raw_response
from src.transform.common import MOLECULAR_WEIGHT
from src.transform.run_transform import load_station_infos, transform_source
from tests.fakes import open_meteo_payload, openaq_hour

UTC = timezone.utc


ENDPOINT = {"open_meteo_weather": "forecast", "open_meteo_air_quality": "air_quality",
            "openaq": "sensors/hours"}


def store(conn, run_id, source, entity, payload, endpoint=None):
    endpoint = endpoint or ENDPOINT[source]
    body = json.dumps(payload).encode()
    save_raw_response(conn, run_id=run_id, source=source, endpoint=endpoint,
                      entity_key=entity, params={},
                      response=ApiResponse("u", 200, body, payload))


def test_molecular_weights_match_database(conn):
    db = dict(conn.execute("SELECT pollutant_code, molecular_weight_g_mol::float "
                           "FROM core.dim_pollutant WHERE molecular_weight_g_mol IS NOT NULL"))
    assert db == MOLECULAR_WEIGHT


def test_overlapping_weather_responses_deduplicated_latest_wins(conn, cities, run_id):
    old = open_meteo_payload(51.5, -0.12, date(2026, 11, 2), date(2026, 11, 3))
    new = open_meteo_payload(51.5, -0.12, date(2026, 11, 3), date(2026, 11, 3))
    new["hourly"]["temperature_2m"] = [99.0 - 50] * 24          # revised values: 49.0
    store(conn, run_id, "open_meteo_weather", "location:london", old)
    store(conn, run_id, "open_meteo_weather", "location:london", new)

    rep = transform_source(conn, "open_meteo_weather", cities)
    assert rep.responses == 2
    assert len(rep.result.kept) == 48 and rep.duplicates_dropped == 24   # Nov 3 seen twice
    nov3 = [r for r in rep.result.kept if r.observed_at_utc.day == 3]
    assert all(r.temperature_c == 49.0 for r in nov3)                   # newer fetch won


def test_unreadable_payload_rejected_others_continue(conn, cities, run_id):
    store(conn, run_id, "open_meteo_weather", "location:london",
          open_meteo_payload(51.5, -0.12, date(2026, 11, 3), date(2026, 11, 3)))
    store(conn, run_id, "open_meteo_weather", "location:delhi", {"hourly": {}})   # broken
    rep = transform_source(conn, "open_meteo_weather", cities)
    assert len(rep.result.kept) == 24
    assert [r.rule_name for r in rep.result.rejected] == ["unreadable_payload"]


def test_openaq_from_raw_uses_stations_yaml(conn, cities, run_id):
    infos = load_station_infos(cities)
    sensor_id, info = next((sid, i) for sid, i in infos.items() if i.target.parameter == "pm25")
    page = {"results": [openaq_hour("pm25", datetime(2026, 11, 3, h)) for h in (1, 2, 3)]}
    store(conn, run_id, "openaq", f"sensor:{sensor_id}", page)
    store(conn, run_id, "openaq", "sensor:1", page)                     # not in config
    rep = transform_source(conn, "openaq", cities, infos)
    assert len(rep.result.kept) == 3
    assert rep.result.kept[0].city_key == info.target.city_key
    assert [r.rule_name for r in rep.result.rejected] == ["unknown_sensor"]


def test_metadata_responses_are_not_read_as_measurements(conn, cities, run_id):
    # Regression: discover-stations stores OpenAQ 'locations' responses under source
    # 'openaq'. They are station metadata, not measurements, and must be ignored.
    store(conn, run_id, "openaq", "location:london",
          {"meta": {}, "results": [{"id": 1, "name": "Site"}]}, endpoint="locations")
    rep = transform_source(conn, "openaq", cities, load_station_infos(cities))
    assert rep.responses == 0 and rep.result.rejected == []
