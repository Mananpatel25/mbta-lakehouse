# Design decisions

**Why Redpanda instead of Kafka?** Kafka-API compatible, single binary, no ZooKeeper, starts in seconds in Compose. Every line of producer/consumer code is unchanged against real Kafka or MSK.

**Why DuckDB + Parquet instead of Snowflake/Redshift?** Zero cost, runs in CI, and the dbt project is portable: swap `profiles.yml` to `snowflake` and the models run unchanged. The lakehouse pattern (object storage + open file format + query engine) is the same one Databricks and Iceberg-on-S3 shops use.

**Why partition by event time?** The MBTA API's `updated_at` is when the vehicle reported; `ingest_time` is when I polled. Partitioning by ingest time would scatter one hour's real activity across two partitions whenever a poll is slow. Event-time partitioning plus a 2-hour incremental lookback means late events still land in the right hour and still get into the mart.

**Why delete+insert incremental strategy?** DuckDB doesn't support `merge`. `delete+insert` on the `(vehicle_id, event_ts)` key gives the same idempotency: re-running a window replaces, never duplicates.

**Why quality gates before dbt rather than only dbt tests?** dbt tests run after models build; by then bad rows are already in the mart. The gates run against raw and fail the DAG first. dbt tests remain as a second line.

**What changes at 100x scale?**
- Consumer: move from batch-poll to Spark Structured Streaming or Flink with checkpointing; write Iceberg instead of bare Parquet for ACID and compaction.
- Storage: S3 with lifecycle rules; partition pruning on `dt/hour` already in place.
- Orchestration: Airflow on Kubernetes (or MWAA); dbt via Cosmos so each model is its own task.
- Observability: emit freshness/lag metrics to Prometheus; alert on SLA miss.
