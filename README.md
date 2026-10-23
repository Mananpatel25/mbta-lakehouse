# MBTA Real-Time Lakehouse

End-to-end streaming data platform on live Boston transit data. One `docker compose up`.

```
MBTA V3 API ──▶ producer.py ──▶ Redpanda (Kafka) ──▶ consumer.py ──▶ MinIO (S3) Parquet, hour-partitioned
GTFS static ──▶ gtfs_static.py ─────────────────────────────────────▶ MinIO raw/
                                                                          │
                                   Airflow DAG (every 15 min) ◀───────────┘
                                   ├─ quality gates (schema, freshness, dupes, nulls)
                                   ├─ dbt run  (DuckDB + httpfs over S3)
                                   ├─ dbt snapshot (SCD2 on routes)
                                   └─ dbt test
```

## What it demonstrates

| Concern | How it's handled |
|---|---|
| Streaming ingestion | Python producer polls `/vehicles` + `/predictions`, publishes to Kafka-compatible Redpanda |
| Exactly-once-ish writes | Consumer dedupes on `(vehicle_id, updated_at)` within batch; Parquet files keyed by batch id so re-runs overwrite, not duplicate |
| Late-arriving data | Partitioned by **event time**, not ingest time; dbt incremental model uses a 2-hour lookback window |
| Slowly changing dimensions | dbt snapshot (SCD Type 2) on GTFS `routes` — captures renames and recolors with valid_from/valid_to |
| Data quality gates | `quality/gates.py` runs before dbt: schema conformance, freshness < 20 min, zero duplicate keys, null thresholds. Fails the DAG, not silently |
| Idempotent backfills | `make backfill START=2026-10-01 END=2026-10-07` reprocesses a date range; every step is safe to re-run |
| Orchestration | Airflow (LocalExecutor), task dependencies with retries and SLA |
| CI | GitHub Actions: lint, pytest, `dbt parse`, `dbt test` against a seeded DuckDB |

## Run it

```bash
cp .env.example .env            # add your free MBTA API key: https://api-v3.mbta.com/
docker compose up -d            # Redpanda, MinIO, Postgres, Airflow
make producer                   # start streaming live vehicle positions
# Airflow UI: http://localhost:8080  (admin/admin)
# MinIO UI:   http://localhost:9001  (minio/minio12345)
# Redpanda:   http://localhost:8081
```

Query the marts directly:

```bash
make duck
D SELECT route_id, count(*) AS positions, avg(speed_mph) FROM main.fct_vehicle_positions GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
```

## Repo layout

```
ingestion/      producer.py (stream), gtfs_static.py (batch)
consumer/       consumer.py  — Kafka → Parquet on S3
dbt/            staging → marts, snapshots (SCD2), schema tests
quality/        gates.py — pre-dbt data-quality checks
orchestration/  Airflow DAG
tests/          pytest (dedupe, late-arrival, gate logic)
docs/           architecture.md, decisions.md
```

## Design decisions

See [docs/decisions.md](docs/decisions.md) — why Redpanda over Kafka, why DuckDB+Parquet over a warehouse, why event-time partitioning, what I'd change at 100x scale.

## Roadmap

- [ ] Apache Iceberg tables (pyiceberg + SQL catalog) in place of raw Parquet — ACID, time travel, schema evolution
- [ ] Prometheus/Grafana for pipeline lag and throughput
- [ ] Terraform module for the AWS (MSK + S3 + MWAA) version
