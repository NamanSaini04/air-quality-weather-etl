"""Unit tests for Phase 4 transforms. Expected values are worked out by hand in comments."""
from datetime import date, datetime, timezone

import pytest

from src.common.config import load_cities
from src.extract.openaq_measurements import SensorTarget
from src.transform.air_quality_transform import (StationInfo, transform_open_meteo_aq,
                                                 transform_openaq)
from src.transform.common import (UnitError, dedupe_latest, parse_utc, to_local, to_ugm3)
from src.transform.weather_transform import transform_weather
from tests.fakes import open_meteo_aq_payload, open_meteo_payload, openaq_hour

UTC = timezone.utc
CITIES = {c.city_key: c for c in load_cities()}
LONDON, DELHI = CITIES["london"], CITIES["delhi"]


# ---------------- time ----------------

def test_open_meteo_naive_and_openaq_z_times_both_parse_as_utc():
    assert parse_utc("2026-11-03T05:00") == datetime(2026, 11, 3, 5, tzinfo=UTC)
    assert parse_utc("2026-11-03T05:00:00Z") == datetime(2026, 11, 3, 5, tzinfo=UTC)


def test_delhi_half_hour_offset_and_date_rollover():
    # 18:00 UTC = 23:30 IST (same day); 19:00 UTC = 00:30 IST (NEXT day)
    a = to_local(datetime(2026, 11, 3, 18, tzinfo=UTC), "Asia/Kolkata")
    b = to_local(datetime(2026, 11, 3, 19, tzinfo=UTC), "Asia/Kolkata")
    assert (a.date_key, a.local_hour) == (20261103, 23)
    assert (b.date_key, b.local_hour) == (20261104, 0)


def test_london_summer_time_is_handled():
    # July: London is BST (UTC+1). November: GMT (UTC+0).
    assert to_local(datetime(2026, 7, 1, 12, tzinfo=UTC), "Europe/London").local_hour == 13
    assert to_local(datetime(2026, 11, 3, 12, tzinfo=UTC), "Europe/London").local_hour == 12


# ---------------- units ----------------

@pytest.mark.parametrize("value,unit,pollutant,expected", [
    (10, "µg/m³", "pm25", 10),
    (10, "μg/m³", "pm25", 10),          # Greek mu
    (1, "ppb", "no2", 1.8818),          # 46.01 / 24.45
    (1, "ppb", "o3", 1.9632),           # 48.00 / 24.45
    (1, "ppm", "co", 1145.6),           # 1000 ppb x 28.01 / 24.45
    (2, "mg/m³", "co", 2000),
])
def test_unit_conversion(value, unit, pollutant, expected):
    assert to_ugm3(value, unit, pollutant) == pytest.approx(expected, rel=1e-3)


def test_ppb_for_particles_is_impossible():
    with pytest.raises(UnitError, match="molecular weight"):
        to_ugm3(5, "ppb", "pm25")


def test_unknown_unit_rejected():
    with pytest.raises(UnitError, match="unknown unit"):
        to_ugm3(5, "particles/cm³", "pm25")


# ---------------- dedup ----------------

def test_dedupe_keeps_latest_version_per_key():
    rows = [("a", 1, "old"), ("a", 2, "new"), ("b", 1, "only")]
    kept, dropped = dedupe_latest(rows, key=lambda r: r[0], version=lambda r: r[1])
    assert sorted(r[2] for r in kept) == ["new", "only"] and dropped == 1


# ---------------- weather ----------------

INGESTED = datetime(2026, 11, 4, 12, 30, tzinfo=UTC)


def weather_payload():
    return open_meteo_payload(51.5, -0.12, date(2026, 11, 3), date(2026, 11, 4))


def test_weather_columns_become_rows_and_future_hours_are_skipped():
    res = transform_weather(weather_payload(), LONDON, 1, INGESTED)
    # 48 hours returned; 00:00 Nov 3 .. 12:00 Nov 4 = 37 hours are in the past
    assert len(res.kept) == 37 and res.skipped == {"future_hour": 11}
    r = res.kept[0]
    assert r.observed_at_utc == datetime(2026, 11, 3, 0, tzinfo=UTC)
    assert r.temperature_c == 10.0 and r.wind_direction_deg == 220 and r.weather_code == 3


def test_weather_out_of_range_row_rejected_with_reason():
    p = weather_payload()
    p["hourly"]["relative_humidity_2m"][2] = 140
    res = transform_weather(p, LONDON, 1, INGESTED)
    assert len(res.rejected) == 1
    rej = res.rejected[0]
    assert rej.rule_name == "weather_range" and "relative_humidity_pct=140" in rej.reason
    assert rej.record["time"] == "2026-11-03T02:00"
    assert len(res.kept) == 36


def test_weather_nulls_kept_but_all_null_hour_skipped():
    p = weather_payload()
    p["hourly"]["temperature_2m"][0] = None                   # one null -> keep row
    for f in ("temperature_2m", "relative_humidity_2m", "pressure_msl", "precipitation",
              "wind_speed_10m", "wind_direction_10m"):
        p["hourly"][f][1] = None                              # all null -> skip row
    res = transform_weather(p, LONDON, 1, INGESTED)
    assert res.kept[0].temperature_c is None
    assert res.skipped["all_values_null"] == 1


def test_weather_text_in_numeric_field_rejected():
    p = weather_payload()
    p["hourly"]["pressure_msl"][0] = "n/a"
    res = transform_weather(p, LONDON, 1, INGESTED)
    assert res.rejected[0].rule_name == "weather_range"


# ---------------- Open-Meteo AQ ----------------

def test_open_meteo_aq_becomes_long_format_without_aqi():
    p = open_meteo_aq_payload(51.5, -0.12, date(2026, 11, 3), date(2026, 11, 4))
    res = transform_open_meteo_aq(p, LONDON, 7, INGESTED)
    assert len(res.kept) == 37 * 6                     # 37 past hours x 6 pollutants
    assert {r.pollutant_code for r in res.kept} == {"pm25", "pm10", "no2", "so2", "o3", "co"}
    r = res.kept[0]
    assert (r.station_type, r.source_station_id) == ("model_grid", "open_meteo:london")


def test_open_meteo_aq_negative_rejected_null_skipped():
    p = open_meteo_aq_payload(51.5, -0.12, date(2026, 11, 3), date(2026, 11, 3))
    p["hourly"]["pm2_5"][0] = -1.0
    p["hourly"]["ozone"][1] = None
    res = transform_open_meteo_aq(p, LONDON, 7, INGESTED)
    assert [r.rule_name for r in res.rejected] == ["negative_or_non_numeric"]
    assert res.skipped == {"null_value": 1}


# ---------------- OpenAQ ----------------

def info(param="no2", units="ppb", city=DELHI):
    t = SensorTarget(city.city_key, 6358, "Mandir Marg", param, 99, units)
    return StationInfo(t, city, 28.63, 77.20, "reference_monitor")


def test_openaq_ppb_converted_and_local_time_set():
    page = {"results": [openaq_hour("no2", datetime(2026, 11, 3, 19), value=20, units="ppb")]}
    [r] = transform_openaq(page, info(), 3, INGESTED).kept
    assert r.value_ugm3 == pytest.approx(37.636, abs=0.001)    # 20 x 46.01 / 24.45
    assert (r.original_value, r.original_unit) == (20, "ppb")
    assert (r.date_key, r.local_hour) == (20261104, 0)         # 19:00 UTC = 00:30 IST
    assert r.source_station_id == "6358" and r.station_type == "reference_monitor"


def test_openaq_negative_null_and_non_hourly():
    good = openaq_hour("no2", datetime(2026, 11, 3, 1), value=5, units="ppb")
    neg = openaq_hour("no2", datetime(2026, 11, 3, 2), value=-3, units="ppb")
    null = openaq_hour("no2", datetime(2026, 11, 3, 3), units="ppb")
    null["value"] = None
    daily = openaq_hour("no2", datetime(2026, 11, 3, 4), units="ppb")
    daily["period"]["interval"] = "24:00:00"
    res = transform_openaq({"results": [good, neg, null, daily]}, info(), 3, INGESTED)
    assert len(res.kept) == 1
    assert sorted(r.rule_name for r in res.rejected) == ["negative_or_non_numeric", "not_hourly"]
    assert res.skipped == {"null_value": 1}


def test_openaq_unit_falls_back_to_config_and_unknown_unit_rejected():
    r = openaq_hour("pm25", datetime(2026, 11, 3, 1), value=40)
    r["parameter"]["units"] = None                             # missing in payload
    assert transform_openaq({"results": [r]}, info("pm25", "µg/m³"), 3, INGESTED).kept
    bad = openaq_hour("pm25", datetime(2026, 11, 3, 1), value=40, units="ppb")
    res = transform_openaq({"results": [bad]}, info("pm25", "µg/m³"), 3, INGESTED)
    assert res.rejected[0].rule_name == "unknown_unit"
