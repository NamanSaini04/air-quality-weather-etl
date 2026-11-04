"""CLI entry point.

Phase 1 commands:
    python main.py init-db          # apply database/schemas/*.sql
    python main.py seed-locations   # upsert config/cities.yaml into core.dim_location
    python main.py check            # print row counts of key tables

Phase 2:
    python main.py extract-weather [--start 2026-11-01] [--end 2026-11-03] [--cities london]

Phase 3a:
    python main.py discover-stations [--radius-km 25] [--max-per-city 3]

Phase 3b/3c:
    python main.py extract-openaq        [--start ...] [--end ...] [--cities ...]
    python main.py extract-open-meteo-aq [--start ...] [--end ...] [--cities ...]

Phase 4:
    python main.py transform-preview     # raw -> clean rows, CSVs in data/processed/
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
                        choices=["init-db", "seed-locations", "check", "extract-weather",
                                 "discover-stations", "extract-openaq",
                                 "extract-open-meteo-aq", "transform-preview"])
    parser.add_argument("--radius-km", type=float, default=25,
                        help="discover-stations search radius, max 25 (default 25)")
    parser.add_argument("--max-per-city", type=int, default=3,
                        help="discover-stations: stations kept per city (default 3)")
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
    if args.command == "discover-stations":
        return run_discover_stations(settings, args)
    if args.command == "extract-openaq":
        return run_extract_openaq(settings, args)
    if args.command == "extract-open-meteo-aq":
        return run_extract_open_meteo_aq(settings, args)
    if args.command == "transform-preview":
        return run_transform_preview(settings, args)

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


def run_discover_stations(settings, args) -> int:
    """Phase 3a: find OpenAQ stations near each city -> config/stations.yaml."""
    import yaml

    from src.common.config import PROJECT_ROOT, ConfigError
    from src.common.http_client import ApiError
    from src.common.run_tracking import TaskRecorder
    from src.extract import openaq_api as oaq
    from src.extract.raw_store import save_raw_response

    radius_m = int(args.radius_km * 1000)
    cities = load_cities()
    try:
        client = oaq.OpenAQClient(settings.openaq_api_key)
    except ConfigError as exc:
        print(f"ERROR: {exc}")
        return 2

    coverages, failures = [], {}
    with get_connection(settings, autocommit=True) as conn, client:
        run_id = start_pipeline_run(conn, dag_id="discover_stations")
        try:
            with TaskRecorder(conn, run_id, "discover_stations", oaq.SOURCE) as task:
                for city in cities:
                    def keep_raw(params, resp, _city=city):
                        save_raw_response(conn, run_id=run_id, source=oaq.SOURCE,
                                          endpoint="locations",
                                          entity_key=f"location:{_city.city_key}",
                                          params=params, response=resp)
                    try:
                        cov = oaq.discover_city(client, city, radius_m, args.max_per_city,
                                                on_response=keep_raw)
                        coverages.append(cov)
                        task.rows_extracted += cov.total
                        task.details[city.city_key] = {"found": cov.total, "active": cov.active,
                                                       "source": cov.recommendation}
                    except ApiError as exc:
                        failures[city.city_key] = f"{type(exc).__name__}: {exc}"
                        task.details[city.city_key] = {"error": failures[city.city_key]}
                if failures:
                    raise ExtractionError(f"discovery failed for: {failures}")
        except ExtractionError as exc:
            finish_pipeline_run(conn, run_id, "failed", str(exc))
            print(f"FAILED: {exc}")
            return 1
        finish_pipeline_run(conn, run_id, "success")

    out = PROJECT_ROOT / "config" / "stations.yaml"
    config = oaq.coverage_to_config(coverages, radius_m, datetime.now(timezone.utc))
    out.write_text("# GENERATED by `python main.py discover-stations` - review before committing.\n"
                   + yaml.safe_dump(config, sort_keys=False, allow_unicode=True),
                   encoding="utf-8")

    print(f"\n{'city':<12}{'found':>6}{'active':>8}  {'source':<20} reason")
    for c in coverages:
        print(f"{c.city_key:<12}{c.total:>6}{c.active:>8}  {c.recommendation:<20} {c.reason}")
        for s in c.selected:
            kind = "monitor" if s.is_monitor else "low-cost"
            print(f"    - {s.name[:40]:<40} {s.distance_km:>5.1f} km  {kind:<8} "
                  f"{','.join(sorted(s.parameters & set(oaq.TARGET_PARAMETERS)))}")
    print(f"\nwrote {out.relative_to(PROJECT_ROOT)}")
    return 0


def run_transform_preview(settings, args) -> int:
    """Phase 4: transform everything in raw, report where every row went, write CSVs."""
    import csv

    from src.common.config import PROJECT_ROOT, ConfigError
    from src.transform.run_transform import SOURCES, load_station_infos, transform_source

    cities = load_cities()
    try:
        infos = load_station_infos(cities)
    except ConfigError as exc:
        print(f"ERROR: {exc}")
        return 2
    out_dir = PROJECT_ROOT / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)

    with get_connection(settings) as conn:
        for source in SOURCES:
            rep = transform_source(conn, source, cities, infos)
            res = rep.result
            print(f"\n=== {source} ===")
            print(f"  raw responses read : {rep.responses}")
            print(f"  kept               : {len(res.kept)}")
            print(f"  duplicates dropped : {rep.duplicates_dropped}  (same hour seen in several responses)")
            print(f"  rejected           : {len(res.rejected)}  {rep.rejected_by_rule() or ''}")
            print(f"  skipped            : {sum(res.skipped.values())}  {res.skipped or ''}")
            for rej in res.rejected[:3]:
                print(f"    reject example -> {rej.entity_key} [{rej.rule_name}] {rej.reason[:90]}")
            if res.kept:
                path = out_dir / f"{source}.csv"
                rows = sorted((r.as_dict() for r in res.kept),
                              key=lambda d: (d["city_key"], d["observed_at_utc"]))
                with open(path, "w", newline="", encoding="utf-8") as fh:
                    w = csv.DictWriter(fh, fieldnames=list(rows[0]))
                    w.writeheader()
                    w.writerows(rows)
                print(f"  wrote {path.relative_to(PROJECT_ROOT)}")
                converted = [r for r in res.kept
                             if getattr(r, "original_unit", "") in ("ppb", "ppm")]
                if converted:
                    c = converted[0]
                    print(f"  unit conversion example: {c.city_key} {c.pollutant_code} "
                          f"{c.original_value} {c.original_unit} -> {c.value_ugm3} µg/m³")
    return 0


def _window_and_cities(args):
    today = datetime.now(timezone.utc).date()
    start = args.start or today - timedelta(days=1)
    end = args.end or today
    cities = load_cities()
    wanted = {c.strip() for c in args.cities.split(",")} if args.cities else None
    if wanted:
        cities = [c for c in cities if c.city_key in wanted]
    return start, end, cities, wanted


def _run_single_task(settings, start, end, dag_id, work, http_client) -> int:
    """Open DB + HTTP client, record a pipeline run around one extract task, report."""
    # autocommit: each `with conn.transaction()` block is its own committed unit,
    # so London's data survives even if Delhi fails afterwards.
    with get_connection(settings, autocommit=True) as conn, http_client as client:
        run_id = start_pipeline_run(conn, dag_id=dag_id,
                                    data_interval_start=start, data_interval_end=end)
        try:
            details = work(conn, client, run_id)
        except ExtractionError as exc:
            finish_pipeline_run(conn, run_id, "failed", str(exc))
            print(f"FAILED run {run_id}: {exc}")
            return 1
        finish_pipeline_run(conn, run_id, "success")
    print(f"run {run_id} OK  ({start} .. {end})")
    for key, st in (details.get("cities") or details.get("stations") or {}).items():
        label = f"{key} {st.get('name', '')}".strip()[:45]
        print(f"  {label:<45} rows={st['rows']:<5} requests={st['requests']:<3} "
              f"new_raw={st['new_raw']}")
    return 0


def run_extract_weather(settings, args) -> int:
    start, end, cities, wanted = _window_and_cities(args)
    if not cities:
        print(f"no matching cities for {wanted}")
        return 2
    return _run_single_task(
        settings, start, end, "extract_weather",
        lambda conn, client, run_id: extract_weather(conn, client, cities, start, end, run_id),
        ResilientHttpClient(rate_limiter=RateLimiter(0.25)))


def run_extract_open_meteo_aq(settings, args) -> int:
    from src.extract.open_meteo_aq import extract_open_meteo_aq
    start, end, cities, wanted = _window_and_cities(args)
    if not cities:
        print(f"no matching cities for {wanted}")
        return 2
    return _run_single_task(
        settings, start, end, "extract_open_meteo_aq",
        lambda conn, client, run_id: extract_open_meteo_aq(conn, client, cities, start, end,
                                                           run_id),
        ResilientHttpClient(rate_limiter=RateLimiter(0.25)))


def run_extract_openaq(settings, args) -> int:
    from src.common.config import ConfigError
    from src.extract.openaq_api import OpenAQClient
    from src.extract.openaq_measurements import extract_openaq, load_sensor_targets
    start, end, _, wanted = _window_and_cities(args)
    try:
        targets = load_sensor_targets(city_keys=wanted)
        client = OpenAQClient(settings.openaq_api_key)
    except ConfigError as exc:
        print(f"ERROR: {exc}")
        return 2
    stations = len({t.station_key for t in targets})
    print(f"{len(targets)} sensors at {stations} stations; ~{len(targets) * 1.1:.0f}s "
          f"at OpenAQ's free rate limit")
    return _run_single_task(
        settings, start, end, "extract_openaq",
        lambda conn, c, run_id: extract_openaq(conn, c, targets, start, end, run_id),
        client)


if __name__ == "__main__":
    sys.exit(main())
