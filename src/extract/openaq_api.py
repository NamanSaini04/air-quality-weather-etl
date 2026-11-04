"""OpenAQ v3 client + station discovery (Phase 3a).

OpenAQ's data model (the part that trips people up):
    location (a monitoring site, has coordinates)
      └── sensors (one per pollutant measured at that site)
            └── measurements (hourly values)
You cannot ask "PM2.5 for London". You find locations near London, pick their
PM2.5 sensors, then pull each sensor's hourly values. Discovery does step 1-2.

Free-tier limits: 60 requests/minute, 2,000/hour -> client waits ~1.1 s between calls.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from src.common.config import City, ConfigError
from src.common.http_client import (ApiResponse, RateLimiter, ResilientHttpClient,
                                    SchemaError)

log = logging.getLogger(__name__)

SOURCE = "openaq"
BASE_URL = "https://api.openaq.org/v3"
MAX_RADIUS_M = 25_000
PAGE_LIMIT = 1000
MAX_PAGES = 20                     # hard stop: never loop forever on a bad 'found' value
TARGET_PARAMETERS = ("pm25", "pm10", "no2", "so2", "o3", "co")
ACTIVE_WITHIN = timedelta(days=7)


# ------------------------------------------------------------------ client
class OpenAQClient:
    def __init__(self, api_key: str | None, http: ResilientHttpClient | None = None,
                 min_interval_s: float = 1.1):
        if not api_key:
            raise ConfigError("OPENAQ_API_KEY is not set (add it to .env)")
        self.http = http or ResilientHttpClient(rate_limiter=RateLimiter(min_interval_s))
        self.http._client.headers["X-API-Key"] = api_key
        self.http._client.headers["Accept"] = "application/json"

    def close(self) -> None:
        self.http.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def get_pages(self, path: str, params: dict) -> Iterator[tuple[dict, ApiResponse]]:
        """Yield (request_params, response) per page until the last page.

        Stop rule: a page with fewer results than `limit` is the last page.
        We do NOT trust meta.found alone: OpenAQ can return it as a string like '>1000'.
        """
        for page in range(1, MAX_PAGES + 1):
            page_params = {**params, "limit": params.get("limit", PAGE_LIMIT), "page": page}
            resp = self.http.get_json(f"{BASE_URL}{path}", page_params)
            results = _results(resp.data)
            yield page_params, resp
            if len(results) < page_params["limit"]:
                return
        raise SchemaError(f"{path}: more than {MAX_PAGES} pages - refusing to continue")


def _results(data: Any) -> list:
    if not isinstance(data, dict) or not isinstance(data.get("results"), list):
        raise SchemaError("OpenAQ response has no 'results' list")
    return data["results"]


# ------------------------------------------------------------------ discovery
@dataclass
class Sensor:
    sensor_id: int
    parameter: str
    units: str


@dataclass
class Station:
    location_id: int
    name: str
    latitude: float
    longitude: float
    distance_km: float
    is_monitor: bool
    is_mobile: bool
    provider: str | None
    datetime_last_utc: datetime | None
    sensors: list[Sensor] = field(default_factory=list)

    @property
    def parameters(self) -> set[str]:
        return {s.parameter for s in self.sensors}

    def is_active(self, now: datetime) -> bool:
        return self.datetime_last_utc is not None and now - self.datetime_last_utc <= ACTIVE_WITHIN


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, dict) or not value.get("utc"):
        return None
    return datetime.fromisoformat(value["utc"].replace("Z", "+00:00"))


def parse_station(raw: dict, city: City) -> Station:
    """Turn one OpenAQ 'location' object into a Station. Raises SchemaError on bad shape."""
    try:
        coords = raw["coordinates"]
        lat, lon = float(coords["latitude"]), float(coords["longitude"])
        sensors = [
            Sensor(int(s["id"]), str(s["parameter"]["name"]).lower(),
                   str(s["parameter"].get("units", "")))
            for s in raw.get("sensors") or []
        ]
        return Station(
            location_id=int(raw["id"]),
            name=str(raw.get("name") or f"location {raw['id']}"),
            latitude=lat, longitude=lon,
            distance_km=round(haversine_km(city.latitude, city.longitude, lat, lon), 2),
            is_monitor=bool(raw.get("isMonitor")),
            is_mobile=bool(raw.get("isMobile")),
            provider=(raw.get("provider") or {}).get("name"),
            datetime_last_utc=_parse_utc(raw.get("datetimeLast")),
            sensors=sensors,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SchemaError(f"unexpected OpenAQ location shape: {exc!r}") from exc


def location_query(city: City, radius_m: int) -> dict:
    if not 0 < radius_m <= MAX_RADIUS_M:
        raise ValueError(f"radius must be 1..{MAX_RADIUS_M} m")
    # Docs text says latitude,longitude; one docs example shows the reverse.
    # We send lat,lon and VERIFY via distance_km (see check_coordinate_order).
    return {"coordinates": f"{city.latitude},{city.longitude}", "radius": radius_m}


def check_coordinate_order(stations: list[Station], radius_m: int) -> None:
    """If stations come back far outside the radius, the API read our point wrongly."""
    limit_km = radius_m / 1000 + 5           # 5 km tolerance for rounding
    far = [s for s in stations if s.distance_km > limit_km]
    if far:
        raise SchemaError(
            f"{len(far)} stations are >{limit_km:.0f} km away (e.g. {far[0].name} at "
            f"{far[0].distance_km} km). Coordinate order is probably wrong.")


@dataclass
class CityCoverage:
    city_key: str
    total: int
    active: int
    selected: list[Station]
    recommendation: str              # 'openaq' or 'fallback_open_meteo'
    reason: str


def select_stations(stations: list[Station], now: datetime,
                    max_per_city: int) -> tuple[list[Station], str, str]:
    """Pick the best stations and decide whether this city can use OpenAQ.

    Rules (simple on purpose; every rule is a test):
      1. drop mobile sensors, inactive stations (no data for 7 days),
         and stations measuring none of our six pollutants
      2. prefer reference monitors over low-cost sensors, then nearest first
      3. the city qualifies for OpenAQ only if a selected station has an active PM2.5 sensor
    """
    usable = [s for s in stations
              if not s.is_mobile and s.is_active(now) and s.parameters & set(TARGET_PARAMETERS)]
    usable.sort(key=lambda s: (not s.is_monitor, "pm25" not in s.parameters, s.distance_km))
    selected = usable[:max_per_city]
    if any("pm25" in s.parameters for s in selected):
        kinds = "reference monitors" if all(s.is_monitor for s in selected) else "incl. low-cost"
        return selected, "openaq", f"{len(selected)} active stations with PM2.5 ({kinds})"
    return selected, "fallback_open_meteo", "no active station with PM2.5 within radius"


def discover_city(client: OpenAQClient, city: City, radius_m: int, max_per_city: int,
                  now: datetime | None = None, on_response=None) -> CityCoverage:
    now = now or datetime.now(timezone.utc)
    stations: list[Station] = []
    for params, resp in client.get_pages("/locations", location_query(city, radius_m)):
        if on_response:
            on_response(params, resp)
        stations.extend(parse_station(r, city) for r in _results(resp.data))
    check_coordinate_order(stations, radius_m)
    selected, rec, reason = select_stations(stations, now, max_per_city)
    return CityCoverage(city.city_key, len(stations),
                        sum(s.is_active(now) for s in stations), selected, rec, reason)


def coverage_to_config(coverages: list[CityCoverage], radius_m: int,
                       generated_at: datetime) -> dict:
    """Shape written to config/stations.yaml - the input to the Phase 3b extractor."""
    return {
        "generated_at_utc": generated_at.isoformat(timespec="seconds"),
        "radius_m": radius_m,
        "cities": {
            c.city_key: {
                "source": c.recommendation,
                "reason": c.reason,
                "stations": [
                    {
                        "openaq_location_id": s.location_id,
                        "name": s.name,
                        "latitude": s.latitude,
                        "longitude": s.longitude,
                        "distance_km": s.distance_km,
                        "station_type": "reference_monitor" if s.is_monitor else "low_cost_sensor",
                        "provider": s.provider,
                        "sensors": {sn.parameter: {"sensor_id": sn.sensor_id, "units": sn.units}
                                    for sn in s.sensors if sn.parameter in TARGET_PARAMETERS},
                    }
                    for s in c.selected
                ],
            }
            for c in coverages
        },
    }
