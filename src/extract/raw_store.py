"""Persist API responses to raw.api_response exactly as received."""
from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any, Iterable

import psycopg

from src.common.http_client import ApiResponse


def payload_hash(data: Any | None, raw_text: str = "",
                 volatile_keys: Iterable[str] = ()) -> str:
    """SHA-256 of the payload, ignoring fields that change on every call.

    Why: Open-Meteo includes `generationtime_ms` (server timing) in every response.
    Hash that, and two identical datasets look different -> raw dedup never works.
    """
    if data is None:
        return hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
    if isinstance(data, dict) and volatile_keys:
        data = {k: v for k, v in data.items() if k not in set(volatile_keys)}
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def save_raw_response(conn: psycopg.Connection, *, run_id: uuid.UUID, source: str,
                      endpoint: str, entity_key: str, params: dict,
                      response: ApiResponse, volatile_keys: Iterable[str] = ()) -> bool:
    """Insert one raw response. Returns True if new, False if an identical one exists."""
    sha = payload_hash(response.data, response.text, volatile_keys)
    with conn.transaction():
        cur = conn.execute(
            """INSERT INTO raw.api_response
                 (run_id, source, endpoint, entity_key, request_params,
                  http_status, payload, payload_text, payload_sha256)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (source, endpoint, payload_sha256) DO NOTHING""",
            (run_id, source, endpoint, entity_key, json.dumps(params, default=str),
             response.status_code,
             json.dumps(response.data) if response.data is not None else None,
             response.text if response.data is None else None,
             sha))
        return cur.rowcount == 1
