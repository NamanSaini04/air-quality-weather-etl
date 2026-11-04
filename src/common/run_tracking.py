"""Write pipeline runs and task runs to the ops schema.

Every write happens in its own short transaction, so run/task status is saved even
if the data work fails. (A failed run that leaves no record is the worst kind.)
"""
from __future__ import annotations

import json
import logging
import uuid

import psycopg

log = logging.getLogger(__name__)


def start_pipeline_run(conn: psycopg.Connection, run_type: str = "manual",
                       dag_id: str = "manual", orchestrator_run_id: str | None = None,
                       data_interval_start=None, data_interval_end=None) -> uuid.UUID:
    run_id = uuid.uuid4()
    with conn.transaction():
        conn.execute(
            """INSERT INTO ops.pipeline_run
               (run_id, dag_id, orchestrator_run_id, run_type,
                data_interval_start, data_interval_end)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (run_id, dag_id, orchestrator_run_id, run_type,
             data_interval_start, data_interval_end))
    log.info("pipeline run %s started (%s)", run_id, run_type)
    return run_id


def finish_pipeline_run(conn: psycopg.Connection, run_id: uuid.UUID,
                        status: str, error_message: str | None = None) -> None:
    with conn.transaction():
        conn.execute(
            """UPDATE ops.pipeline_run
               SET status = %s, ended_at = now(), error_message = %s
               WHERE run_id = %s""",
            (status, error_message, run_id))
    log.info("pipeline run %s finished: %s", run_id, status)


class TaskRecorder:
    """Context manager around one task.

        with TaskRecorder(conn, run_id, "extract_weather", "open_meteo_weather") as t:
            t.rows_extracted += 48
            t.details["london"] = "ok"

    On normal exit   -> status 'success' (or whatever you set in t.status)
    On exception     -> 'partial' if some rows were extracted, else 'failed';
                        the error is saved and the exception is re-raised (never swallowed).
    """

    def __init__(self, conn: psycopg.Connection, run_id: uuid.UUID,
                 task_name: str, source: str | None = None):
        self.conn, self.run_id = conn, run_id
        self.task_name, self.source = task_name, source
        self.rows_extracted = self.rows_transformed = 0
        self.rows_loaded = self.rows_rejected = 0
        self.details: dict = {}
        self.status: str | None = None
        self.task_run_id: int | None = None

    def __enter__(self) -> "TaskRecorder":
        with self.conn.transaction():
            self.task_run_id = self.conn.execute(
                """INSERT INTO ops.task_run (run_id, task_name, source)
                   VALUES (%s, %s, %s) RETURNING task_run_id""",
                (self.run_id, self.task_name, self.source)).fetchone()[0]
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is None:
            status, error = self.status or "success", None
        else:
            status = "partial" if self.rows_extracted or self.rows_loaded else "failed"
            error = f"{exc_type.__name__}: {exc}"
        with self.conn.transaction():
            self.conn.execute(
                """UPDATE ops.task_run SET
                     status = %s, ended_at = now(), error_message = %s,
                     rows_extracted = %s, rows_transformed = %s,
                     rows_loaded = %s, rows_rejected = %s, details = %s
                   WHERE task_run_id = %s""",
                (status, error, self.rows_extracted, self.rows_transformed,
                 self.rows_loaded, self.rows_rejected,
                 json.dumps(self.details, default=str), self.task_run_id))
        level = logging.INFO if status == "success" else logging.ERROR
        log.log(level, "task %s %s: extracted=%d loaded=%d rejected=%d%s",
                self.task_name, status, self.rows_extracted, self.rows_loaded,
                self.rows_rejected, f" error={error}" if error else "")
        return False    # never suppress the exception
