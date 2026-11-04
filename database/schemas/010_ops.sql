-- 010_ops.sql
-- Pipeline metadata. Created before raw/core because they reference ops.pipeline_run.

-- One row per pipeline execution (one Airflow DAG run, or one manual run).
CREATE TABLE IF NOT EXISTS ops.pipeline_run (
    run_id               UUID PRIMARY KEY,
    dag_id               TEXT        NOT NULL DEFAULT 'manual',
    orchestrator_run_id  TEXT,                       -- Airflow run_id, if any
    run_type             TEXT        NOT NULL DEFAULT 'manual'
                         CHECK (run_type IN ('scheduled', 'manual', 'backfill')),
    status               TEXT        NOT NULL DEFAULT 'running'
                         CHECK (status IN ('running', 'success', 'partial', 'failed')),
    data_interval_start  TIMESTAMPTZ,
    data_interval_end    TIMESTAMPTZ,
    started_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at             TIMESTAMPTZ,
    error_message        TEXT,
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);

-- One row per task inside a run. This is where row counts live.
CREATE TABLE IF NOT EXISTS ops.task_run (
    task_run_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id            UUID        NOT NULL REFERENCES ops.pipeline_run (run_id),
    task_name         TEXT        NOT NULL,
    source            TEXT,                          -- 'open_meteo_weather', 'openaq', ...
    status            TEXT        NOT NULL DEFAULT 'running'
                      CHECK (status IN ('running', 'success', 'partial', 'failed', 'skipped')),
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at          TIMESTAMPTZ,
    rows_extracted    INTEGER     NOT NULL DEFAULT 0 CHECK (rows_extracted   >= 0),
    rows_transformed  INTEGER     NOT NULL DEFAULT 0 CHECK (rows_transformed >= 0),
    rows_loaded       INTEGER     NOT NULL DEFAULT 0 CHECK (rows_loaded      >= 0),
    rows_rejected     INTEGER     NOT NULL DEFAULT 0 CHECK (rows_rejected    >= 0),
    error_message     TEXT,
    details           JSONB       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS ix_task_run_run_id ON ops.task_run (run_id);

-- Incremental-load watermarks.
-- One watermark PER SOURCE PER ENTITY (city or station), not one global value.
-- Reason: if Delhi fails and London succeeds, a single global watermark would
-- move forward and Delhi's missing hours would never be fetched again.
CREATE TABLE IF NOT EXISTS ops.watermark (
    source                TEXT        NOT NULL,
    entity_key            TEXT        NOT NULL,      -- e.g. 'location:london', 'station:2178'
    last_observed_at_utc  TIMESTAMPTZ NOT NULL,      -- latest observation safely loaded
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by_run_id     UUID REFERENCES ops.pipeline_run (run_id),
    PRIMARY KEY (source, entity_key)
);

-- Quarantine for rows that failed validation. Never silently drop data.
CREATE TABLE IF NOT EXISTS ops.rejected_record (
    reject_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id       UUID        NOT NULL REFERENCES ops.pipeline_run (run_id),
    source       TEXT        NOT NULL,
    entity_key   TEXT,
    rule_name    TEXT        NOT NULL,               -- e.g. 'humidity_range'
    reason       TEXT        NOT NULL,
    record       JSONB       NOT NULL,
    rejected_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rejected_run_rule ON ops.rejected_record (run_id, rule_name);
