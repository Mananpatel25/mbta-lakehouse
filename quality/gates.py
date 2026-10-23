"""Pre-dbt data-quality gates. Run as an Airflow task; any failure raises and
stops the DAG before bad data reaches the marts.

Checks (all against the raw Parquet layer via DuckDB + httpfs):
  1. schema      — required columns present with expected types
  2. freshness   — newest event_ts within FRESHNESS_MINUTES
  3. duplicates  — no duplicate (vehicle_id, updated_at) within the lookback window
  4. null rates  — critical columns below null-rate thresholds
  5. geo bounds  — lat/lon inside Massachusetts bounding box
"""
from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass

import duckdb

log = logging.getLogger("quality")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RAW_GLOB = os.getenv("RAW_GLOB", "s3://lake/raw/vehicles/*/*/*.parquet")
FRESHNESS_MINUTES = int(os.getenv("FRESHNESS_MINUTES", "20"))
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "3"))

REQUIRED = {
    "vehicle_id": "VARCHAR",
    "route_id": "VARCHAR",
    "latitude": "DOUBLE",
    "longitude": "DOUBLE",
    "updated_at": "VARCHAR",
    "ingest_time": "VARCHAR",
}
NULL_THRESHOLDS = {"vehicle_id": 0.0, "updated_at": 0.0, "route_id": 0.02, "latitude": 0.01}


@dataclass
class GateResult:
    name: str
    passed: bool
    detail: str


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{os.getenv('S3_ENDPOINT_HOST', 'minio:9000')}';")
    con.execute(f"SET s3_access_key_id='{os.getenv('AWS_ACCESS_KEY_ID', 'minio')}';")
    con.execute(f"SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY', 'minio12345')}';")
    con.execute("SET s3_use_ssl=false; SET s3_url_style='path';")
    con.execute(f"CREATE VIEW raw AS SELECT * FROM read_parquet('{RAW_GLOB}', hive_partitioning=true);")
    return con


def gate_schema(con) -> GateResult:
    cols = {r[0]: r[1] for r in con.execute("DESCRIBE raw").fetchall()}
    missing = [c for c in REQUIRED if c not in cols]
    wrong = [f"{c}:{cols[c]}" for c, t in REQUIRED.items() if c in cols and cols[c] != t]
    ok = not missing and not wrong
    return GateResult("schema", ok, f"missing={missing} wrong_type={wrong}")


def gate_freshness(con) -> GateResult:
    (age_min,) = con.execute(
        "SELECT date_diff('minute', max(cast(updated_at AS timestamptz)), now()) FROM raw"
    ).fetchone()
    ok = age_min is not None and age_min <= FRESHNESS_MINUTES
    return GateResult("freshness", ok, f"newest event is {age_min} min old (limit {FRESHNESS_MINUTES})")


def gate_duplicates(con) -> GateResult:
    (dups,) = con.execute(
        f"""SELECT count(*) FROM (
              SELECT vehicle_id, updated_at, count(*) c FROM raw
              WHERE cast(updated_at AS timestamptz) >= now() - interval {LOOKBACK_HOURS} hour
              GROUP BY 1,2 HAVING c > 1)"""
    ).fetchone()
    return GateResult("duplicates", dups == 0, f"{dups} duplicate keys in last {LOOKBACK_HOURS}h")


def gate_nulls(con) -> GateResult:
    failures = []
    for col, limit in NULL_THRESHOLDS.items():
        (rate,) = con.execute(
            f"SELECT avg(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) FROM raw "
            f"WHERE cast(updated_at AS timestamptz) >= now() - interval {LOOKBACK_HOURS} hour"
        ).fetchone()
        if rate is not None and rate > limit:
            failures.append(f"{col}={rate:.3%}>{limit:.0%}")
    return GateResult("null_rates", not failures, f"failures={failures}")


def gate_geo(con) -> GateResult:
    (bad,) = con.execute(
        "SELECT count(*) FROM raw WHERE latitude NOT BETWEEN 41.5 AND 43.0 OR longitude NOT BETWEEN -72.0 AND -70.0"
    ).fetchone()
    return GateResult("geo_bounds", bad == 0, f"{bad} rows outside MA bounding box")


def run_all() -> list[GateResult]:
    con = connect()
    return [g(con) for g in (gate_schema, gate_freshness, gate_duplicates, gate_nulls, gate_geo)]


def main() -> None:
    results = run_all()
    for r in results:
        log.info("%-12s %s  %s", r.name, "PASS" if r.passed else "FAIL", r.detail)
    failed = [r for r in results if not r.passed]
    if failed:
        log.error("%d gate(s) failed — blocking downstream models", len(failed))
        sys.exit(1)


if __name__ == "__main__":
    main()
