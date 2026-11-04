"""Air-quality payloads -> clean, LONG-format measurement records (one per pollutant).

Two sources, one output shape:
  * Open-Meteo AQ : modelled; one virtual 'model_grid' station per city
  * OpenAQ        : measured; real stations, units vary (µg/m³, ppb, ppm)
Every value leaves this module in µg/m³.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime

from src.common.config import City
from src.extract.openaq_measurements import SensorTarget
from src.transform.common import (Rejection, TransformResult, UnitError, is_number,
                                  parse_utc, to_local, to_ugm3)

OPEN_METEO_SOURCE = "open_meteo_air_quality"
OPENAQ_SOURCE = "openaq"

# Open-Meteo variable name -> our pollutant code
OPEN_METEO_POLLUTANTS = {
    "pm2_5": "pm25", "pm10": "pm10", "nitrogen_dioxide": "no2",
    "sulphur_dioxide": "so2", "ozone": "o3", "carbon_monoxide": "co",
}
# european_aqi / us_aqi are NOT kept: AQI is derived in dbt from concentrations (D4),
# so every city's AQI uses the same formula regardless of source.


@dataclass
class AirQualityRecord:
    source: str
    source_station_id: str          # 'open_meteo:london' or OpenAQ location id
    station_name: str
    station_type: str               # model_grid | reference_monitor | low_cost_sensor
    city_key: str
    latitude: float
    longitude: float
    pollutant_code: str
    observed_at_utc: datetime       # END of the averaging hour
    date_key: int
    local_date: date
    local_hour: int
    value_ugm3: float
    original_value: float
    original_unit: str
    raw_response_id: int
    ingested_at: datetime

    def as_dict(self) -> dict:
        return asdict(self)


def _reject(result, source, entity, rule, reason, record):
    result.rejected.append(Rejection(source, entity, rule, reason, record))


# ------------------------------------------------------------------ Open-Meteo AQ
def transform_open_meteo_aq(payload: dict, city: City, raw_response_id: int,
                            ingested_at: datetime) -> TransformResult:
    result = TransformResult()
    entity = f"location:{city.city_key}"
    hourly, units = payload["hourly"], payload.get("hourly_units") or {}

    for i, ts in enumerate(hourly["time"]):
        observed = parse_utc(ts)
        if observed > ingested_at:
            result.skip("future_hour", len(OPEN_METEO_POLLUTANTS))
            continue
        local = to_local(observed, city.timezone)
        for api_name, code in OPEN_METEO_POLLUTANTS.items():
            v = hourly[api_name][i]
            if v is None:
                result.skip("null_value")
                continue
            raw = {"time": ts, "variable": api_name, "value": v, "unit": units.get(api_name)}
            if not is_number(v) or v < 0:
                _reject(result, OPEN_METEO_SOURCE, entity, "negative_or_non_numeric",
                        f"{api_name}={v!r}", raw)
                continue
            result.kept.append(AirQualityRecord(
                source=OPEN_METEO_SOURCE, source_station_id=f"open_meteo:{city.city_key}",
                station_name=f"{city.city_name} (CAMS model)", station_type="model_grid",
                city_key=city.city_key, latitude=city.latitude, longitude=city.longitude,
                pollutant_code=code, observed_at_utc=observed, date_key=local.date_key,
                local_date=local.local_date, local_hour=local.local_hour,
                value_ugm3=round(float(v), 3), original_value=float(v),
                original_unit=units.get(api_name, "µg/m³"),
                raw_response_id=raw_response_id, ingested_at=ingested_at))
    return result


# ------------------------------------------------------------------ OpenAQ
@dataclass(frozen=True)
class StationInfo:
    """What the transform needs to know about a sensor's station (from stations.yaml)."""
    target: SensorTarget
    city: City
    latitude: float
    longitude: float
    station_type: str


def transform_openaq(payload: dict, info: StationInfo, raw_response_id: int,
                     ingested_at: datetime) -> TransformResult:
    result = TransformResult()
    t = info.target
    entity = f"sensor:{t.sensor_id}"

    for r in payload["results"]:
        period = r.get("period") or {}
        raw = {"value": r.get("value"), "unit": (r.get("parameter") or {}).get("units"),
               "period": period}
        if period.get("interval") not in (None, "01:00:00"):
            _reject(result, OPENAQ_SOURCE, entity, "not_hourly",
                    f"interval={period.get('interval')}", raw)
            continue
        v = r.get("value")
        if v is None:
            result.skip("null_value")
            continue
        if not is_number(v) or v < 0:
            # Negative concentrations happen (sensor noise, baseline drift). They are
            # physically impossible, so they are rejected - never clipped to 0.
            _reject(result, OPENAQ_SOURCE, entity, "negative_or_non_numeric",
                    f"value={v!r}", raw)
            continue
        unit = (r.get("parameter") or {}).get("units") or t.units
        try:
            ugm3 = to_ugm3(float(v), unit, t.parameter)
        except UnitError as exc:
            _reject(result, OPENAQ_SOURCE, entity, "unknown_unit", str(exc), raw)
            continue

        observed = parse_utc(period["datetimeTo"]["utc"])
        local = to_local(observed, info.city.timezone)
        result.kept.append(AirQualityRecord(
            source=OPENAQ_SOURCE, source_station_id=str(t.openaq_location_id),
            station_name=t.station_name, station_type=info.station_type,
            city_key=t.city_key, latitude=info.latitude, longitude=info.longitude,
            pollutant_code=t.parameter, observed_at_utc=observed, date_key=local.date_key,
            local_date=local.local_date, local_hour=local.local_hour,
            value_ugm3=round(ugm3, 3), original_value=float(v), original_unit=unit,
            raw_response_id=raw_response_id, ingested_at=ingested_at))
    return result
