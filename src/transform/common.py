"""Shared transform building blocks.

Every input row ends in exactly ONE bucket:
  * kept      - clean record, ready to load
  * rejected  - broke a rule; saved to ops.rejected_record with the rule name
  * skipped   - deliberately not loaded, but not bad data (e.g. forecast hours)
Nothing disappears without being counted.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Callable, Iterable, TypeVar
from zoneinfo import ZoneInfo

UTC = timezone.utc


# ------------------------------------------------------------------ time
def parse_utc(value: str) -> datetime:
    """Parse an API timestamp into an aware UTC datetime.

    Open-Meteo (asked for GMT) returns '2026-11-03T05:00' with NO offset.
    OpenAQ returns '2026-11-03T05:00:00Z'. Both mean UTC; naive input is assumed UTC
    because the extractor already verified utc_offset_seconds == 0.
    """
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


@dataclass(frozen=True)
class LocalTime:
    local_date: date
    local_hour: int

    @property
    def date_key(self) -> int:
        return int(self.local_date.strftime("%Y%m%d"))


def to_local(observed_at_utc: datetime, tz_name: str) -> LocalTime:
    """'AQI by hour of day' must use LOCAL hour: Delhi 08:00 is 02:30 UTC.

    Delhi is UTC+05:30, so 02:30 UTC -> 08:00 local. Hour = floor of local time.
    """
    local = observed_at_utc.astimezone(ZoneInfo(tz_name))
    return LocalTime(local.date(), local.hour)


# ------------------------------------------------------------------ units
MOLAR_VOLUME_L = 24.45      # litres per mole at 25 °C, 1 atm (US EPA convention)
# Same values as core.dim_pollutant (a test checks they match).
MOLECULAR_WEIGHT = {"no2": 46.01, "so2": 64.07, "o3": 48.00, "co": 28.01}


class UnitError(ValueError):
    pass


def normalise_unit(unit: str | None) -> str:
    return (unit or "").replace("μ", "µ").replace(" ", "").lower()


def to_ugm3(value: float, unit: str | None, pollutant: str) -> float:
    """Convert a concentration to µg/m³ (the canonical unit).

    Gases:  µg/m³ = ppb × molecular_weight / 24.45      (ppm = 1000 ppb)
    Particles (PM2.5, PM10) have no molecular weight - they must already be µg/m³.
    """
    u = normalise_unit(unit)
    if u in ("µg/m³", "µg/m3", "ug/m3", "ug/m³"):
        return value
    if u in ("mg/m³", "mg/m3"):
        return value * 1000
    if u in ("ppb", "ppm"):
        mw = MOLECULAR_WEIGHT.get(pollutant)
        if mw is None:
            raise UnitError(f"cannot convert {unit} for {pollutant} (no molecular weight)")
        ppb = value * 1000 if u == "ppm" else value
        return ppb * mw / MOLAR_VOLUME_L
    raise UnitError(f"unknown unit '{unit}' for {pollutant}")


def is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


# ------------------------------------------------------------------ results
@dataclass
class Rejection:
    source: str
    entity_key: str
    rule_name: str
    reason: str
    record: dict


@dataclass
class TransformResult:
    kept: list = field(default_factory=list)
    rejected: list[Rejection] = field(default_factory=list)
    skipped: dict[str, int] = field(default_factory=dict)   # reason -> count

    def skip(self, reason: str, n: int = 1) -> None:
        self.skipped[reason] = self.skipped.get(reason, 0) + n

    def extend(self, other: "TransformResult") -> None:
        self.kept.extend(other.kept)
        self.rejected.extend(other.rejected)
        for k, v in other.skipped.items():
            self.skip(k, v)

    @property
    def counts(self) -> dict:
        return {"kept": len(self.kept), "rejected": len(self.rejected),
                "skipped": sum(self.skipped.values())}


# ------------------------------------------------------------------ dedup
T = TypeVar("T")


def dedupe_latest(records: Iterable[T], key: Callable[[T], tuple],
                  version: Callable[[T], Any]) -> tuple[list[T], int]:
    """Keep one record per key - the one with the highest version (latest ingestion).

    Why 'latest wins': the same hour can arrive in several raw responses (re-runs,
    overlapping windows). Forecast-model values get revised, so the newest fetch
    is the best value we have. Returns (records, number_of_duplicates_dropped).
    """
    best: dict[tuple, T] = {}
    total = 0
    for r in records:
        total += 1
        k = key(r)
        if k not in best or version(r) > version(best[k]):
            best[k] = r
    return list(best.values()), total - len(best)
