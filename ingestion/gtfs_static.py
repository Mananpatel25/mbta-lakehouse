"""Download MBTA GTFS static feed and land routes/stops as Parquet in S3 raw/.

GTFS changes a few times a year; the dbt snapshot on routes turns those
changes into SCD Type 2 history. Safe to re-run: each run overwrites
raw/gtfs/<table>/snapshot_date=<today>/data.parquet.
"""
from __future__ import annotations

import io
import logging
import os
import zipfile
from datetime import date

import boto3
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import requests

log = logging.getLogger("gtfs")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

GTFS_URL = "https://cdn.mbta.com/MBTA_GTFS.zip"
TABLES = ["routes", "stops", "trips"]
BUCKET = os.getenv("LAKE_BUCKET", "lake")


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minio"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minio12345"),
        region_name="us-east-1",
    )


def main() -> None:
    r = requests.get(GTFS_URL, timeout=60)
    r.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    s3 = s3_client()
    today = date.today().isoformat()

    for t in TABLES:
        with zf.open(f"{t}.txt") as fh:
            table = pacsv.read_csv(fh, convert_options=pacsv.ConvertOptions(strings_can_be_null=True))
        table = table.append_column("snapshot_date", pa.array([today] * table.num_rows))
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        key = f"raw/gtfs/{t}/snapshot_date={today}/data.parquet"
        s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
        log.info("%s: %d rows → s3://%s/%s", t, table.num_rows, BUCKET, key)


if __name__ == "__main__":
    main()
