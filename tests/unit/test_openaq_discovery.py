"""Unit tests for OpenAQ station discovery.

Payloads follow OpenAQ v3's documented 'location' shape. Built by hand (no network in
the build environment); `python main.py discover-stations` on a real key validates it.
"""
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src.common.config import ConfigError, load_cities
from src.common.http_client import (PermanentApiError, ResilientHttpClient, RetryPolicy,
                                    SchemaError)
from src.extract import openaq_api as oaq

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)
LONDON = next(c for c in load_cities() if c.city_key == "london")


def location(loc_id, *, lat=51.52, lon=-0.12, params=("pm25", "no2"), monitor=True,
             mobile=False, last=NOW - timedelta(hours=2), name=None):
    return {
        "id": loc_id, "name": name or f"Site {loc_id}",
        "coordinates": {"latitude": lat, "longitude": lon},
        "isMonitor": monitor, "isMobile": mobile,
        "provider": {"id": 1, "name": "AURN"},
        "datetimeLast": {"utc": last.strftime("%Y-%m-%dT%H:%M:%SZ"), "local": "x"} if last else None,
        "sensors": [{"id": loc_id * 10 + i, "name": f"{p} µg/m³",
                     "parameter": {"id": i, "name": p, "units": "µg/m³", "displayName": p}}
                    for i, p in enumerate(params)],
    }


def page(results, limit=1000, found=None):
    return {"meta": {"name": "openaq-api", "page": 1, "limit": limit,
                     "found": found if found is not None else len(results)},
            "results": results}


def make_client(pages, status=200):
    seen = []

    def handler(request):
        seen.append(request)
        if status != 200:
            return httpx.Response(status, json={"detail": "Invalid credentials"})
        n = int(request.url.params["page"])
        return httpx.Response(200, content=json.dumps(pages[n - 1]).encode())

    http = ResilientHttpClient(transport=httpx.MockTransport(handler),
                               retry=RetryPolicy(max_attempts=2, base_delay_s=0, jitter_s=0),
                               sleep=lambda s: None)
    return oaq.OpenAQClient("test-key", http=http), seen


# ---------------- client ----------------

def test_missing_key_fails_before_any_request():
    with pytest.raises(ConfigError, match="OPENAQ_API_KEY"):
        oaq.OpenAQClient(None)


def test_key_sent_in_header_and_query_uses_lat_lon():
    client, seen = make_client([page([location(1)])])
    oaq.discover_city(client, LONDON, 25_000, 3, now=NOW)
    req = seen[0]
    assert req.headers["X-API-Key"] == "test-key"
    assert req.url.params["coordinates"] == "51.5074,-0.1278"
    assert req.url.params["radius"] == "25000"


def test_pagination_follows_until_short_page_even_if_found_is_a_string():
    full = page([location(i) for i in range(1, 3)], limit=2, found=">1000")
    last = page([location(9)], limit=2, found=">1000")
    client, seen = make_client([full, last])
    pages = list(client.get_pages("/locations", {"limit": 2}))
    assert len(pages) == 2 and [r.url.params["page"] for r in seen] == ["1", "2"]


def test_bad_key_is_permanent_and_not_retried():
    client, seen = make_client([], status=401)
    with pytest.raises(PermanentApiError):
        oaq.discover_city(client, LONDON, 25_000, 3, now=NOW)
    assert len(seen) == 1


def test_radius_over_api_maximum_rejected_locally():
    with pytest.raises(ValueError):
        oaq.location_query(LONDON, 30_000)


# ---------------- parsing ----------------

def test_parse_station_computes_distance_and_parameters():
    s = oaq.parse_station(location(7, lat=51.5074, lon=-0.1278), LONDON)
    assert s.distance_km == 0.0 and s.parameters == {"pm25", "no2"}
    assert s.datetime_last_utc == NOW - timedelta(hours=2)


def test_parse_station_bad_shape_is_schema_error():
    bad = location(7)
    del bad["coordinates"]
    with pytest.raises(SchemaError):
        oaq.parse_station(bad, LONDON)


def test_haversine_known_distance():
    # London -> Manchester is ~262 km in a straight line
    assert 255 < oaq.haversine_km(51.5074, -0.1278, 53.4808, -2.2426) < 270


def test_swapped_coordinates_detected():
    # If the API had read our point as lon,lat it would return stations far away
    stations = [oaq.parse_station(location(1, lat=-0.12, lon=51.5), LONDON)]
    with pytest.raises(SchemaError, match="Coordinate order"):
        oaq.check_coordinate_order(stations, 25_000)


def test_discover_city_refuses_far_away_results():
    client, _ = make_client([page([location(1, lat=-0.12, lon=51.5)])])
    with pytest.raises(SchemaError, match="Coordinate order"):
        oaq.discover_city(client, LONDON, 25_000, 3, now=NOW)


# ---------------- selection rules ----------------

def stations(*locs):
    return [oaq.parse_station(l, LONDON) for l in locs]


def test_inactive_mobile_and_irrelevant_stations_dropped():
    s = stations(
        location(1, last=NOW - timedelta(days=30)),          # stale
        location(2, mobile=True),                            # mobile
        location(3, params=("temperature", "pm1")),          # nothing we need
        location(4, last=None),                              # never reported
        location(5),                                         # good
    )
    selected, rec, _ = oaq.select_stations(s, NOW, max_per_city=3)
    assert [x.location_id for x in selected] == [5] and rec == "openaq"


def test_monitors_preferred_over_closer_low_cost_sensors():
    s = stations(location(1, lat=51.5075, monitor=False),   # very close, low-cost
                 location(2, lat=51.60, monitor=True))      # ~10 km, monitor
    selected, _, _ = oaq.select_stations(s, NOW, max_per_city=1)
    assert selected[0].location_id == 2


def test_pm25_station_preferred_then_nearest():
    s = stations(location(1, lat=51.51, params=("no2",)),
                 location(2, lat=51.60, params=("pm25",)),
                 location(3, lat=51.55, params=("pm25",)))
    selected, _, _ = oaq.select_stations(s, NOW, max_per_city=2)
    assert [x.location_id for x in selected] == [3, 2]


def test_no_active_pm25_means_fallback():
    s = stations(location(1, params=("no2", "o3")))
    _, rec, reason = oaq.select_stations(s, NOW, max_per_city=3)
    assert rec == "fallback_open_meteo" and "PM2.5" in reason


def test_no_stations_at_all_means_fallback():
    assert oaq.select_stations([], NOW, 3)[1] == "fallback_open_meteo"


# ---------------- config output ----------------

def test_config_output_keeps_only_target_sensors():
    client, _ = make_client([page([location(1, params=("pm25", "temperature", "no2"))])])
    cov = oaq.discover_city(client, LONDON, 25_000, 3, now=NOW)
    cfg = oaq.coverage_to_config([cov], 25_000, NOW)
    station = cfg["cities"]["london"]["stations"][0]
    assert cfg["cities"]["london"]["source"] == "openaq"
    assert set(station["sensors"]) == {"pm25", "no2"}
    assert station["sensors"]["pm25"]["sensor_id"] == 10
    assert station["station_type"] == "reference_monitor"
