"""Database helpers (PostgreSQL via psycopg 3)."""
from __future__ import annotations

import logging
from pathlib import Path

import psycopg

from src.common.config import SCHEMA_DIR, Settings

log = logging.getLogger(__name__)


def get_connection(settings: Settings, **kwargs) -> psycopg.Connection:
    return psycopg.connect(settings.conninfo, **kwargs)


def schema_files(schema_dir: Path = SCHEMA_DIR) -> list[Path]:
    """DDL files run in filename order (001, 010, 020 ...). Order matters: FKs."""
    files = sorted(schema_dir.glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"no .sql files in {schema_dir}")
    return files


def apply_schema(conn: psycopg.Connection, schema_dir: Path = SCHEMA_DIR) -> list[str]:
    """Apply all DDL files in ONE transaction: either everything applies or nothing.

    Every statement uses IF NOT EXISTS / ON CONFLICT, so running this twice is safe.
    """
    applied = []
    with conn.transaction():
        for path in schema_files(schema_dir):
            conn.execute(path.read_text(encoding="utf-8"))
            applied.append(path.name)
            log.info("applied %s", path.name)
    return applied
