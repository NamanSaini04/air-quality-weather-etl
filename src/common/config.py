"""Configuration loading.

Two kinds of config:
  * Settings  - secrets and connection details, read from environment variables.
  * Cities    - business config, read from config/cities.yaml (version-controlled).

Secrets never go in YAML or Git. Business config always does.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CITIES_PATH = PROJECT_ROOT / "config" / "cities.yaml"
SCHEMA_DIR = PROJECT_ROOT / "database" / "schemas"

VALID_CLIMATE_ZONES = {"uk_temperate", "india_monsoon"}


class ConfigError(ValueError):
    """Raised when configuration is invalid. Fail at startup, not mid-run."""


@dataclass(frozen=True)
class Settings:
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str
    openaq_api_key: str | None

    @property
    def conninfo(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )


DEFAULT_DOTENV_PATH = PROJECT_ROOT / ".env"


def read_dotenv(path: Path | str = DEFAULT_DOTENV_PATH) -> dict[str, str]:
    """Minimal .env reader: KEY=VALUE lines, '#' comments, optional quotes.

    Kept tiny on purpose (no python-dotenv dependency). Missing file -> {}.
    """
    values: dict[str, str] = {}
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return values
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.removeprefix("export ").strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def load_settings(env: dict[str, str] | None = None) -> Settings:
    """Precedence: real environment variables > .env file > defaults.

    Real env wins so Docker/Airflow can override values without editing files.
    """
    if env is None:
        env = {**read_dotenv(), **os.environ}
    try:
        port = int(env.get("POSTGRES_PORT", "5432"))
    except ValueError as exc:
        raise ConfigError("POSTGRES_PORT must be an integer") from exc
    return Settings(
        pg_host=env.get("POSTGRES_HOST", "localhost"),
        pg_port=port,
        pg_db=env.get("POSTGRES_DB", "aq_weather"),
        pg_user=env.get("POSTGRES_USER", "etl"),
        pg_password=env.get("POSTGRES_PASSWORD", "etl"),
        openaq_api_key=env.get("OPENAQ_API_KEY") or None,
    )


@dataclass(frozen=True)
class City:
    city_key: str
    city_name: str
    region: str | None
    country_code: str
    latitude: float
    longitude: float
    timezone: str
    climate_zone: str


REQUIRED_CITY_FIELDS = (
    "city_key", "city_name", "country_code",
    "latitude", "longitude", "timezone", "climate_zone",
)


def _validate_city(raw: dict, index: int) -> City:
    missing = [f for f in REQUIRED_CITY_FIELDS if raw.get(f) in (None, "")]
    if missing:
        raise ConfigError(f"city #{index}: missing fields {missing}")

    key = str(raw["city_key"]).strip().lower()
    if not key.replace("_", "").isalnum():
        raise ConfigError(f"city #{index}: city_key '{key}' must be letters/digits/underscore")

    lat, lon = float(raw["latitude"]), float(raw["longitude"])
    if not -90 <= lat <= 90:
        raise ConfigError(f"{key}: latitude {lat} out of range")
    if not -180 <= lon <= 180:
        raise ConfigError(f"{key}: longitude {lon} out of range")

    country = str(raw["country_code"]).strip().upper()
    if len(country) != 2:
        raise ConfigError(f"{key}: country_code must be 2 letters (ISO 3166-1)")

    try:
        ZoneInfo(raw["timezone"])
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ConfigError(f"{key}: unknown timezone '{raw['timezone']}'") from exc

    if raw["climate_zone"] not in VALID_CLIMATE_ZONES:
        raise ConfigError(
            f"{key}: climate_zone must be one of {sorted(VALID_CLIMATE_ZONES)}"
        )

    return City(
        city_key=key,
        city_name=str(raw["city_name"]).strip(),
        region=(str(raw["region"]).strip() if raw.get("region") else None),
        country_code=country,
        latitude=lat,
        longitude=lon,
        timezone=raw["timezone"],
        climate_zone=raw["climate_zone"],
    )


def load_cities(path: Path | str = DEFAULT_CITIES_PATH) -> list[City]:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    raw_cities = data.get("cities")
    if not raw_cities:
        raise ConfigError(f"{path}: no 'cities' list found")

    cities = [_validate_city(c, i) for i, c in enumerate(raw_cities)]
    keys = [c.city_key for c in cities]
    dupes = sorted({k for k in keys if keys.count(k) > 1})
    if dupes:
        raise ConfigError(f"duplicate city_key values: {dupes}")
    return cities
