"""Unit tests: config validation. No database needed."""
import textwrap

import pytest

from src.common.config import ConfigError, load_cities, load_settings

VALID_CITY = """
  - city_key: london
    city_name: London
    region: England
    country_code: GB
    latitude: 51.5
    longitude: -0.12
    timezone: Europe/London
    climate_zone: uk_temperate
"""


def write_yaml(tmp_path, body: str):
    p = tmp_path / "cities.yaml"
    p.write_text("cities:\n" + textwrap.dedent(body).replace("\n", "\n", -1))
    return p


def test_real_config_loads_five_cities():
    cities = load_cities()
    assert len(cities) == 5
    assert {c.city_key for c in cities} == {"london", "manchester", "birmingham", "delhi", "mumbai"}


def test_valid_city_parses(tmp_path):
    [city] = load_cities(write_yaml(tmp_path, VALID_CITY))
    assert city.city_key == "london"
    assert city.country_code == "GB"


def test_duplicate_city_key_rejected(tmp_path):
    with pytest.raises(ConfigError, match="duplicate"):
        load_cities(write_yaml(tmp_path, VALID_CITY + VALID_CITY))


@pytest.mark.parametrize("field,bad_value,message", [
    ("latitude: 51.5", "latitude: 95", "latitude"),
    ("longitude: -0.12", "longitude: 200", "longitude"),
    ("timezone: Europe/London", "timezone: Mars/Olympus", "timezone"),
    ("climate_zone: uk_temperate", "climate_zone: tropical", "climate_zone"),
    ("country_code: GB", "country_code: GBR", "country_code"),
])
def test_invalid_values_rejected(tmp_path, field, bad_value, message):
    with pytest.raises(ConfigError, match=message):
        load_cities(write_yaml(tmp_path, VALID_CITY.replace(field, bad_value)))


def test_missing_required_field_rejected(tmp_path):
    with pytest.raises(ConfigError, match="missing"):
        load_cities(write_yaml(tmp_path, VALID_CITY.replace("    timezone: Europe/London\n", "")))


def test_empty_file_rejected(tmp_path):
    p = tmp_path / "cities.yaml"
    p.write_text("")
    with pytest.raises(ConfigError):
        load_cities(p)


def test_read_dotenv_parses_comments_quotes_and_export(tmp_path):
    from src.common.config import read_dotenv
    p = tmp_path / ".env"
    p.write_text('# comment\n\nPOSTGRES_PORT=5433\nexport POSTGRES_DB="aq"\nEMPTY=\nbad line\n')
    assert read_dotenv(p) == {"POSTGRES_PORT": "5433", "POSTGRES_DB": "aq", "EMPTY": ""}
    assert read_dotenv(tmp_path / "missing.env") == {}


def test_settings_defaults_and_bad_port():
    s = load_settings({})
    assert s.pg_port == 5432 and s.openaq_api_key is None
    with pytest.raises(ConfigError):
        load_settings({"POSTGRES_PORT": "abc"})
