"""Open-Meteo weather payload -> clean hourly weather records.

Open-Meteo returns COLUMNS (one array per variable). We turn them into ROWS
(one record per hour), because the fact table is one row per city per hour.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime

from src.common.config import City
from src.transform.common import (Rejection, TransformResult, is_number, parse_utc,
                                  to_local)

SOURCE = "open_meteo_weather"

# API field -> (record field, min, max). Bounds mirror the DB CHECK constraints so a
# bad value is rejected HERE with a reason, instead of crashing the load later.
FIELDS = {
    "temperature_2m":       ("temperature_c", -90, 60),
    "relative_humidity_2m": ("relative_humidity_pct", 0, 100),
    "pressure_msl":         ("pressure_hpa", 850, 1100),
    "precipitation":        ("precipitation_mm", 0, None),
    "wind_speed_10m":       ("wind_speed_kmh", 0, None),
    "wind_direction_10m":   ("wind_direction_deg", 0, 360),
}


@dataclass
class WeatherRecord:
    city_key: str
    observed_at_utc: datetime
    date_key: int
    local_date: date
    local_hour: int
    temperature_c: float | None
    relative_humidity_pct: float | None
    pressure_hpa: float | None
    precipitation_mm: float | None
    wind_speed_kmh: float | None
    wind_direction_deg: int | None
    weather_code: int | None
    source: str
    raw_response_id: int
    ingested_at: datetime

    def as_dict(self) -> dict:
        return asdict(self)


def transform_weather(payload: dict, city: City, raw_response_id: int,
                      ingested_at: datetime) -> TransformResult:
    result = TransformResult()
    entity = f"location:{city.city_key}"
    hourly = payload["hourly"]

    for i, ts in enumerate(hourly["time"]):
        observed = parse_utc(ts)
        # The forecast endpoint also returns hours that hadn't happened yet when we
        # fetched. Those are predictions, not observations -> never load them.
        if observed > ingested_at:
            result.skip("future_hour")
            continue

        values, problems = {}, []
        for api_name, (field, lo, hi) in FIELDS.items():
            v = hourly[api_name][i]
            if v is None:
                values[field] = None
            elif not is_number(v):
                problems.append(f"{field}={v!r} is not a number")
            elif (lo is not None and v < lo) or (hi is not None and v > hi):
                problems.append(f"{field}={v} outside [{lo}, {hi}]")
            else:
                values[field] = float(v)

        if problems:
            result.rejected.append(Rejection(
                SOURCE, entity, "weather_range", "; ".join(problems),
                {"time": ts, **{a: hourly[a][i] for a in FIELDS}}))
            continue
        if all(v is None for v in values.values()):
            result.skip("all_values_null")        # e.g. archive hours not yet published
            continue

        code = hourly["weather_code"][i]
        local = to_local(observed, city.timezone)
        result.kept.append(WeatherRecord(
            city_key=city.city_key, observed_at_utc=observed,
            date_key=local.date_key, local_date=local.local_date, local_hour=local.local_hour,
            wind_direction_deg=(int(round(values["wind_direction_deg"]))
                                if values["wind_direction_deg"] is not None else None),
            weather_code=int(code) if is_number(code) else None,
            source=SOURCE, raw_response_id=raw_response_id, ingested_at=ingested_at,
            **{k: v for k, v in values.items() if k != "wind_direction_deg"},
        ))
    return result
