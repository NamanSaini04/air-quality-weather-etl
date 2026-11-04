# Air Quality & Weather ETL Pipeline

Batch ETL pipeline: hourly weather (Open-Meteo) and air-quality (OpenAQ) data for 5 UK
and Indian cities → validated, incremental, idempotent loads into PostgreSQL → dbt marts.

> Status: **Phase 2 complete** — schema, weather extractor with retries, raw landing, run logging.
> Design decisions: [docs/architecture.md](docs/architecture.md)

## Quick start (Phase 1)

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env

docker compose up -d postgres          # schema auto-applies on first start
python main.py init-db                 # safe to re-run
python main.py seed-locations          # loads config/cities.yaml
python main.py check                   # row counts

pytest                                 # unit + integration tests
```

Environment variables are read from your shell. On macOS/Linux: `set -a; source .env; set +a`.
On Windows PowerShell, the defaults (`localhost`, `etl`/`etl`) match docker-compose, so no
action is needed.

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 1 | Architecture, schema, config | ✅ |
| 2 | Weather extractor (Open-Meteo) | ✅ |
| 3 | Air-quality extractor (OpenAQ) — 3a discovery ✅, 3b extractor ⏳ | |
| 4 | Transformations | |
| 5–8 | Loading, incremental, idempotency | |
| 9 | Airflow DAG | |
| 10–11 | Data quality, structured logging | |
| 12–13 | dbt models and marts | |
| 14–17 | Dashboard, Docker, tests, docs | |
