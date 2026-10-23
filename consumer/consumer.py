"""Kafka → Parquet on S3 (MinIO) consumer.

Reads a bounded batch from the vehicles topic, dedupes, partitions by EVENT
time (not ingest time) so late-arriving events land in the right hour, and
writes one Parquet file per (partition, batch_id). Re-running the same batch_id
overwrites the same keys → idempotent.

Usage:
    python consumer/consumer.py --max-messages 5000 --batch-id 2026-10-22T15:00
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
from datetime import UTC, datetime

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaException

log = logging.getLogger("consumer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

TOPIC = os.getenv("KAFKA_TOPIC", "mbta.vehicles.v1")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
BUCKET = os.getenv("LAKE_BUCKET", "lake")

SCHEMA = pa.schema(
    [
        ("vehicle_id", pa.string()),
        ("label", pa.string()),
        ("route_id", pa.string()),
        ("trip_id", pa.string()),
        ("stop_id", pa.string()),
        ("direction_id", pa.int32()),
        ("current_status", pa.string()),
        ("current_stop_sequence", pa.int32()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("bearing", pa.float64()),
        ("speed", pa.float64()),
        ("occupancy_status", pa.string()),
        ("updated_at", pa.string()),
        ("ingest_time", pa.string()),
    ]
)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minio"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minio12345"),
        region_name="us-east-1",
    )


def dedupe(rows: list[dict]) -> list[dict]:
    """Keep one row per (vehicle_id, updated_at). Producer re-polls every 15s,
    so unchanged vehicles are republished with the same event time."""
    seen: set[tuple[str, str]] = set()
    out = []
    for r in rows:
        key = (r["vehicle_id"], r["updated_at"] or "")
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def partition_key(updated_at: str | None, ingest_time: str) -> tuple[str, str]:
    """Partition by event time; fall back to ingest time if event time missing."""
    ts = updated_at or ingest_time
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(UTC)  # noqa: FURB162
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H")


def group_by_partition(rows: list[dict]) -> dict[tuple[str, str], list[dict]]:
    groups: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        groups.setdefault(partition_key(r.get("updated_at"), r["ingest_time"]), []).append(r)
    return groups


def write_partition(s3, key: tuple[str, str], rows: list[dict], batch_id: str) -> str:
    dt, hour = key
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    s3_key = f"raw/vehicles/dt={dt}/hour={hour}/batch={batch_id}.parquet"
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=buf.getvalue())
    return s3_key


def consume(max_messages: int, timeout_s: float = 10.0) -> list[dict]:
    c = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "lakehouse-writer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe([TOPIC])
    rows: list[dict] = []
    try:
        while len(rows) < max_messages:
            msg = c.poll(timeout_s)
            if msg is None:
                break  # nothing more right now
            if msg.error():
                raise KafkaException(msg.error())
            rows.append(json.loads(msg.value()))
        c.commit(asynchronous=False)  # commit only after we hold the batch
    finally:
        c.close()
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-messages", type=int, default=5000)
    ap.add_argument("--batch-id", default=datetime.now(UTC).strftime("%Y%m%dT%H%M%S"))
    args = ap.parse_args()

    rows = consume(args.max_messages)
    log.info("consumed %d messages", len(rows))
    if not rows:
        return
    rows = dedupe(rows)
    log.info("after dedupe: %d rows", len(rows))

    s3 = s3_client()
    for key, group in group_by_partition(rows).items():
        s3_key = write_partition(s3, key, group, args.batch_id)
        log.info("wrote %d rows → s3://%s/%s", len(group), BUCKET, s3_key)


if __name__ == "__main__":
    main()
