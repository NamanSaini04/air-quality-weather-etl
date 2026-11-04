-- 001_schemas.sql
-- Layered schemas. Each layer has one job.
--   raw  : untouched API responses (replayable, auditable)
--   core : cleaned, validated dimensions and facts (written by Python)
--   ops  : pipeline metadata - runs, task stats, watermarks, rejected rows
--   analytics : built by dbt later (Phase 12). Not created here.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS ops;
