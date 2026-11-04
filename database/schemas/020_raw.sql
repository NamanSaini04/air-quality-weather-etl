-- 020_raw.sql
-- Raw landing layer: store every API response exactly as received.
-- Why: if a transform bug is found later, we re-process from here
-- instead of calling the API again (which may no longer return old data).

CREATE TABLE IF NOT EXISTS raw.api_response (
    response_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id          UUID        NOT NULL REFERENCES ops.pipeline_run (run_id),
    source          TEXT        NOT NULL,            -- 'open_meteo_weather', 'openaq', ...
    endpoint        TEXT        NOT NULL,
    entity_key      TEXT,                            -- 'location:london', 'station:2178'
    request_params  JSONB       NOT NULL DEFAULT '{}'::jsonb,
    http_status     SMALLINT    NOT NULL,
    payload         JSONB,                           -- NULL if body was not valid JSON
    payload_text    TEXT,                            -- kept only when JSON parsing failed
    payload_sha256  TEXT        NOT NULL,            -- detects identical re-fetches
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (payload IS NOT NULL OR payload_text IS NOT NULL)
);

-- Same body for the SAME entity is stored once (raw idempotency).
-- entity_key MUST be part of the key: OpenAQ hourly results carry no sensor id, so two
-- different sensors can return byte-identical bodies (e.g. both empty). Without
-- entity_key the second sensor's response was silently dropped. (Bug found in Phase 3b.)
-- NULLS NOT DISTINCT (PostgreSQL 15+): two NULL entity_keys still count as duplicates.
DROP INDEX IF EXISTS raw.ux_api_response_dedup;          -- v1 index, missing entity_key
CREATE UNIQUE INDEX IF NOT EXISTS ux_api_response_dedup_v2
    ON raw.api_response (source, endpoint, entity_key, payload_sha256) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS ix_api_response_run ON raw.api_response (run_id);
