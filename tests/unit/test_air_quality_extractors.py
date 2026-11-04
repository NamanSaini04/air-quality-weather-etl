"""Unit tests: Open-Meteo AQ spec, OpenAQ station config loading and payload checks."""
from datetime import date

import pytest
import yaml

from src.common.config import ConfigError, load_cities
from src.common.http_client import SchemaError
from src.extract import open_meteo
from src.extract.open_meteo_aq import AQ_SPEC, HOURLY_VARIABLES, plan_requests
from src.extract.openaq_measurements import (load_sensor_targets, validate_measurements,
                                             window_params)
from tests.fakes import open_meteo_aq_payload, openaq_hour

LONDON = next(c for c in load_cities() if c.city_key == "london")
TODAY = date(2026, 11, 4)


# ---------------- Open-Meteo air quality ----------------

def test_aq_params_request_all_pollutants_in_utc():
    [plan] = plan_requests(date(2026, 11, 3), date(2026, 11, 4), TODAY)
    params = open_meteo.build_params(AQ_SPEC, LONDON, plan)
    assert params["hourly"].split(",") == list(HOURLY_VARIABLES)
    assert params["timezone"] == "GMT"
    assert "air-quality-api.open-meteo.com" in plan.url


def test_aq_future_end_clipped():
    [plan] = plan_requests(date(2026, 11, 4), date(2026, 11, 9), TODAY)
    assert plan.end == TODAY


def test_micro_sign_and_greek_mu_treated_as_same_unit():
    # payload uses Greek mu (U+03BC); spec expects micro sign (U+00B5)
    p = open_meteo_aq_payload(51.5, -0.12, date(2026, 11, 3), date(2026, 11, 4))
    assert open_meteo.validate_hourly(AQ_SPEC, p) == 48
    assert open_meteo.normalise_unit("μg/m³") == open_meteo.normalise_unit("µg/m³")


def test_aq_real_unit_change_still_caught():
    p = open_meteo_aq_payload(51.5, -0.12, date(2026, 11, 3), date(2026, 11, 4))
    p["hourly_units"]["pm2_5"] = "ppb"
    with pytest.raises(SchemaError, match="units"):
        open_meteo.validate_hourly(AQ_SPEC, p)


# ---------------- OpenAQ station config ----------------

def write_stations(tmp_path, cities: dict):
    path = tmp_path / "stations.yaml"
    path.write_text(yaml.safe_dump({"cities": cities}))
    return path


def station(loc_id, **sensors):
    return {"openaq_location_id": loc_id, "name": f"Site {loc_id}",
            "sensors": {p: {"sensor_id": sid, "units": "µg/m³"} for p, sid in sensors.items()}}


def test_targets_flatten_one_per_sensor(tmp_path):
    path = write_stations(tmp_path, {
        "london": {"source": "openaq", "stations": [station(1, pm25=11, no2=12), station(2, pm25=21)]}})
    targets = load_sensor_targets(path)
    assert sorted(t.sensor_id for t in targets) == [11, 12, 21]
    assert {t.station_key for t in targets} == {"london/1", "london/2"}


def test_fallback_cities_and_unrequested_cities_skipped(tmp_path):
    path = write_stations(tmp_path, {
        "london": {"source": "openaq", "stations": [station(1, pm25=11)]},
        "delhi": {"source": "openaq", "stations": [station(5, pm25=51)]},
        "mumbai": {"source": "fallback_open_meteo", "stations": []}})
    assert sorted(t.city_key for t in load_sensor_targets(path)) == ["delhi", "london"]
    assert [t.city_key for t in load_sensor_targets(path, {"delhi"})] == ["delhi"]


def test_missing_stations_file_tells_you_what_to_run(tmp_path):
    with pytest.raises(ConfigError, match="discover-stations"):
        load_sensor_targets(tmp_path / "nope.yaml")


def test_nothing_selected_is_an_error(tmp_path):
    path = write_stations(tmp_path, {"mumbai": {"source": "fallback_open_meteo"}})
    with pytest.raises(ConfigError, match="no OpenAQ sensors"):
        load_sensor_targets(path)


def test_real_committed_config_loads():
    targets = load_sensor_targets()
    assert {t.city_key for t in targets} == {"london", "manchester", "birmingham",
                                             "delhi", "mumbai"}
    assert all(t.parameter in {"pm25", "pm10", "no2", "so2", "o3", "co"} for t in targets)


# ---------------- OpenAQ payload checks ----------------

def test_window_covers_whole_utc_days():
    assert window_params(date(2026, 11, 3), date(2026, 11, 4)) == {
        "datetime_from": "2026-11-03T00:00:00Z", "datetime_to": "2026-11-05T00:00:00Z"}
    with pytest.raises(ValueError):
        window_params(date(2026, 11, 4), date(2026, 11, 3))


def test_valid_page_counts_rows():
    from datetime import datetime
    page = {"meta": {}, "results": [openaq_hour("pm25", datetime(2026, 11, 3, h)) for h in range(1, 4)]}
    assert validate_measurements(page, "pm25") == 3


def test_wrong_parameter_means_stale_config():
    from datetime import datetime
    page = {"results": [openaq_hour("no2", datetime(2026, 11, 3, 1))]}
    with pytest.raises(SchemaError, match="re-run discover-stations"):
        validate_measurements(page, "pm25")


def test_missing_period_or_value_is_schema_error():
    from datetime import datetime
    r = openaq_hour("pm25", datetime(2026, 11, 3, 1))
    del r["period"]["datetimeTo"]
    with pytest.raises(SchemaError, match="datetimeTo"):
        validate_measurements({"results": [r]}, "pm25")
    r2 = openaq_hour("pm25", datetime(2026, 11, 3, 1))
    del r2["value"]
    with pytest.raises(SchemaError, match="value"):
        validate_measurements({"results": [r2]}, "pm25")


def test_empty_page_is_valid():
    assert validate_measurements({"results": []}, "pm25") == 0
