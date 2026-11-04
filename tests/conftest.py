"""Shared fixtures.

Integration tests create a throwaway database (aq_weather_test), apply the full
schema, and drop it afterwards. They are skipped if PostgreSQL is not reachable,
so unit tests still run on a laptop without Docker.
"""
from __future__ import annotations

import uuid
from dataclasses import replace

import psycopg
import pytest

from src.common.config import load_cities, load_settings
from src.common.db import apply_schema, get_connection

TEST_DB = "aq_weather_test"


@pytest.fixture(scope="session")
def admin_settings():
    return load_settings()


@pytest.fixture(scope="session")
def test_db(admin_settings):
    try:
        admin = get_connection(admin_settings, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable ({exc.__class__.__name__}); "
                    "start it with `docker compose up -d postgres`")
    with admin:
        admin.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
        admin.execute(f"CREATE DATABASE {TEST_DB}")
    settings = replace(admin_settings, pg_db=TEST_DB)
    with get_connection(settings) as conn:
        apply_schema(conn)
    yield settings
    with get_connection(admin_settings, autocommit=True) as admin:
        admin.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")


@pytest.fixture
def conn(test_db):
    """Each test runs inside a transaction that is rolled back -> tests are isolated."""
    with get_connection(test_db) as c:
        c.execute("BEGIN")
        yield c
        c.rollback()


@pytest.fixture
def cities():
    return load_cities()


@pytest.fixture
def run_id(conn):
    rid = uuid.uuid4()
    conn.execute("INSERT INTO ops.pipeline_run (run_id) VALUES (%s)", (rid,))
    return rid
