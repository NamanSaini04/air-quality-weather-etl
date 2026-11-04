"""Integration tests: prove the database itself enforces our rules.

Each test is one guarantee you can state in an interview:
"the database rejects humidity of 150", "re-running a load cannot duplicate rows", etc.
"""
import psycopg
import pytest
from psycopg import errors

from src.common.db import apply_schema
from src.load.seed_locations import seed_locations

EXPECTED_TABLES = {
    ("raw", "api_response"),
    ("core", "dim_location"), ("core", "dim_station"), ("core", "dim_pollutant"),
    ("core", "dim_date"), ("core", "fact_weather_hourly"),
    ("core", "fact_air_quality_measurement"),
    ("ops", "pipeline_run"), ("ops", "task_run"), ("ops", "watermark"),
    ("ops", "rejected_record"),
}

WEATHER_INSERT = """
INSERT INTO core.fact_weather_hourly
    (location_id, date_key, observed_at_utc, local_hour, source,
     temperature_c, relative_humidity_pct, batch_run_id)
VALUES (%(loc)s, 20261103, '2026-11-03 10:00+00', 10, 'open_meteo_weather',
        %(temp)s, %(hum)s, %(run)s)
"""


def location_id(conn, key="london"):
    return conn.execute(
        "SELECT location_id FROM core.dim_location WHERE city_key = %s", (key,)
    ).fetchone()[0]


# ---------- structure ----------

def test_all_tables_exist(conn):
    rows = conn.execute("""
        SELECT table_schema, table_name FROM information_schema.tables
        WHERE table_schema IN ('raw', 'core', 'ops')
    """).fetchall()
    assert EXPECTED_TABLES <= set(rows)


def test_schema_apply_is_rerunnable(conn):
    # Applying DDL a second time must not fail or duplicate seed data.
    apply_schema(conn)
    assert conn.execute("SELECT count(*) FROM core.dim_pollutant").fetchone()[0] == 6


def test_dim_date_is_correct(conn):
    count = conn.execute("SELECT count(*) FROM core.dim_date").fetchone()[0]
    assert count == 3287  # 2022-01-01 .. 2030-12-31 inclusive
    row = conn.execute("""
        SELECT day_of_week, day_name, is_weekend, month, quarter, iso_week
        FROM core.dim_date WHERE date_key = 20261103
    """).fetchone()
    assert row == (2, "Tuesday", False, 11, 4, 45)


# ---------- idempotent seeding ----------

def test_seed_locations_is_idempotent(conn, cities):
    assert seed_locations(conn, cities) == 5      # first run inserts 5
    assert seed_locations(conn, cities) == 0      # second run changes nothing
    assert conn.execute("SELECT count(*) FROM core.dim_location").fetchone()[0] == 5


def test_seed_locations_updates_changed_city(conn, cities):
    seed_locations(conn, cities)
    from dataclasses import replace
    changed = [replace(c, region="Greater London") if c.city_key == "london" else c
               for c in cities]
    assert seed_locations(conn, changed) == 1
    region = conn.execute(
        "SELECT region FROM core.dim_location WHERE city_key='london'").fetchone()[0]
    assert region == "Greater London"


# ---------- constraints ----------

def test_duplicate_weather_observation_rejected(conn, cities, run_id):
    seed_locations(conn, cities)
    params = {"loc": location_id(conn), "temp": 9.5, "hum": 80, "run": run_id}
    conn.execute(WEATHER_INSERT, params)
    with pytest.raises(errors.UniqueViolation):
        with conn.transaction():
            conn.execute(WEATHER_INSERT, params)


def test_upsert_updates_instead_of_duplicating(conn, cities, run_id):
    seed_locations(conn, cities)
    params = {"loc": location_id(conn), "temp": 9.5, "hum": 80, "run": run_id}
    upsert = WEATHER_INSERT + """
        ON CONFLICT ON CONSTRAINT uq_weather_obs DO UPDATE SET
            temperature_c = EXCLUDED.temperature_c, updated_at = now()
    """
    conn.execute(upsert, params)
    conn.execute(upsert, {**params, "temp": 10.1})   # same hour, corrected value
    rows = conn.execute(
        "SELECT temperature_c FROM core.fact_weather_hourly").fetchall()
    assert len(rows) == 1 and float(rows[0][0]) == 10.1


@pytest.mark.parametrize("temp,hum", [(9.5, 150), (9.5, -1), (75, 50), (-100, 50)])
def test_out_of_range_weather_rejected(conn, cities, run_id, temp, hum):
    seed_locations(conn, cities)
    with pytest.raises(errors.CheckViolation):
        with conn.transaction():
            conn.execute(WEATHER_INSERT,
                         {"loc": location_id(conn), "temp": temp, "hum": hum, "run": run_id})


def test_unknown_location_rejected(conn, run_id):
    with pytest.raises(errors.ForeignKeyViolation):
        with conn.transaction():
            conn.execute(WEATHER_INSERT, {"loc": 999, "temp": 9, "hum": 50, "run": run_id})


def test_negative_pollutant_rejected_and_unknown_pollutant_rejected(conn, cities, run_id):
    seed_locations(conn, cities)
    station_id = conn.execute("""
        INSERT INTO core.dim_station (location_id, source, source_station_id, station_type)
        VALUES (%s, 'openaq', '2178', 'reference_monitor') RETURNING station_id
    """, (location_id(conn),)).fetchone()[0]
    sql = """
        INSERT INTO core.fact_air_quality_measurement
            (station_id, pollutant_code, date_key, observed_at_utc, local_hour,
             value_ugm3, source, batch_run_id)
        VALUES (%s, %s, 20261103, '2026-11-03 10:00+00', 10, %s, 'openaq', %s)
    """
    conn.execute(sql, (station_id, "pm25", 12.4, run_id))           # valid
    with pytest.raises(errors.CheckViolation):
        with conn.transaction():
            conn.execute(sql, (station_id, "pm25", -3, run_id))
    with pytest.raises(errors.ForeignKeyViolation):
        with conn.transaction():
            conn.execute(sql, (station_id, "benzene", 1.0, run_id))
    with pytest.raises(errors.UniqueViolation):
        with conn.transaction():
            conn.execute(sql, (station_id, "pm25", 12.4, run_id))


def test_watermark_one_row_per_source_and_entity(conn, run_id):
    sql = """
        INSERT INTO ops.watermark (source, entity_key, last_observed_at_utc, updated_by_run_id)
        VALUES ('openaq', 'station:2178', %s, %s)
        ON CONFLICT (source, entity_key) DO UPDATE
        SET last_observed_at_utc = GREATEST(ops.watermark.last_observed_at_utc,
                                            EXCLUDED.last_observed_at_utc),
            updated_at = now()
    """
    conn.execute(sql, ("2026-11-03 10:00+00", run_id))
    conn.execute(sql, ("2026-11-03 08:00+00", run_id))   # older value must NOT move it back
    rows = conn.execute(
        "SELECT last_observed_at_utc::text FROM ops.watermark").fetchall()
    assert rows == [("2026-11-03 10:00:00+00",)]


def test_raw_identical_payload_stored_once(conn, run_id):
    sql = """
        INSERT INTO raw.api_response (run_id, source, endpoint, http_status, payload, payload_sha256)
        VALUES (%s, 'openaq', '/v3/sensors/1/hours', 200, '{"results": []}', 'abc123')
        ON CONFLICT DO NOTHING
    """
    conn.execute(sql, (run_id,))
    conn.execute(sql, (run_id,))
    assert conn.execute("SELECT count(*) FROM raw.api_response").fetchone()[0] == 1
