"""CLI entry point.

Phase 1 commands:
    python main.py init-db          # apply database/schemas/*.sql
    python main.py seed-locations   # upsert config/cities.yaml into core.dim_location
    python main.py check            # print row counts of key tables

Phase 2:
    python main.py extract-weather [--start 2026-11-01] [--end 2026-11-03] [--cities london]
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta, timezone

from src.common.config import load_cities, load_settings
from src.common.db import apply_schema, get_connection
from src.common.http_client import RateLimiter, ResilientHttpClient
from src.common.run_tracking import finish_pipeline_run, start_pipeline_run
from src.extract.weather_api import ExtractionError, extract_weather
from src.load.seed_locations import seed_locations

CHECK_TABLES = [
    "core.dim_location", "core.dim_station", "core.dim_pollutant", "core.dim_date",
    "core.fact_weather_hourly", "core.fact_air_quality_measurement",
    "raw.api_response", "ops.pipeline_run", "ops.task_run",
    "ops.watermark", "ops.rejected_record",
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Air Quality & Weather ETL")
    parser.add_argument("command",
                        choices=["init-db", "seed-locations", "check", "extract-weather"])
    parser.add_argument("--start", type=date.fromisoformat,
                        help="YYYY-MM-DD (default: yesterday UTC)")
    parser.add_argument("--end", type=date.fromisoformat,
                        help="YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--cities", help="comma-separated city_keys (default: all)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = load_settings()

    if args.command == "extract-weather":
        return run_extract_weather(settings, args)

    with get_connection(settings) as conn:
        if args.command == "init-db":
            applied = apply_schema(conn)
            print(f"Applied {len(applied)} schema files: {', '.join(applied)}")
        elif args.command == "seed-locations":
            n = seed_locations(conn, load_cities())
            print(f"dim_location rows inserted/updated: {n}")
        elif args.command == "check":
            for table in CHECK_TABLES:
                count = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                print(f"{table:<40} {count:>8}")
    return 0


def run_extract_weather(settings, args) -> int:
    today = datetime.now(timezone.utc).date()
    start = args.start or today - timedelta(days=1)
    end = args.end or today
    cities = load_cities()
    if args.cities:
        wanted = {c.strip() for c in args.cities.split(",")}
        cities = [c for c in cities if c.city_key in wanted]
        if not cities:
            print(f"no matching cities for {wanted}")
            return 2

    # autocommit: each `with conn.transaction()` block is its own committed unit,
    # so London's data survives even if Delhi fails afterwards.
    with get_connection(settings, autocommit=True) as conn, \
         ResilientHttpClient(rate_limiter=RateLimiter(0.25)) as client:
        run_id = start_pipeline_run(conn, data_interval_start=start, data_interval_end=end)
        try:
            details = extract_weather(conn, client, cities, start, end, run_id)
        except ExtractionError as exc:
            finish_pipeline_run(conn, run_id, "failed", str(exc))
            print(f"FAILED run {run_id}: {exc}")
            return 1
        finish_pipeline_run(conn, run_id, "success")
    print(f"run {run_id} OK")
    for key, stats in details["cities"].items():
        print(f"  {key:<12} rows={stats['rows']:<5} requests={stats['requests']} "
              f"new_raw={stats['new_raw']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
