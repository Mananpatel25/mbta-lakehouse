"""Poll MBTA V3 API and publish vehicle positions to Kafka (Redpanda).

Runs forever. Each poll fetches all live vehicles, flattens the JSON:API
payload, stamps an ingest time, and publishes one message per vehicle keyed
by vehicle_id (so a partition always sees a vehicle's events in order).

Usage:
    MBTA_API_KEY=... python ingestion/producer.py
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime

import requests
from confluent_kafka import Producer

log = logging.getLogger("producer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

MBTA_URL = "https://api-v3.mbta.com/vehicles"
TOPIC = os.getenv("KAFKA_TOPIC", "mbta.vehicles.v1")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))


def flatten(vehicle: dict) -> dict:
    """JSON:API -> flat row. Keep raw attributes; add lineage columns."""
    a = vehicle["attributes"]
    rel = vehicle.get("relationships", {})

    def rel_id(name: str):
        d = rel.get(name, {}).get("data")
        return d["id"] if d else None

    return {
        "vehicle_id": vehicle["id"],
        "label": a.get("label"),
        "route_id": rel_id("route"),
        "trip_id": rel_id("trip"),
        "stop_id": rel_id("stop"),
        "direction_id": a.get("direction_id"),
        "current_status": a.get("current_status"),
        "current_stop_sequence": a.get("current_stop_sequence"),
        "latitude": a.get("latitude"),
        "longitude": a.get("longitude"),
        "bearing": a.get("bearing"),
        "speed": a.get("speed"),  # m/s, often null
        "occupancy_status": a.get("occupancy_status"),
        "updated_at": a.get("updated_at"),  # event time (ISO8601, from MBTA)
        "ingest_time": datetime.now(UTC).isoformat(),
    }


def fetch_vehicles(session: requests.Session, api_key: str | None) -> list[dict]:
    headers = {"x-api-key": api_key} if api_key else {}
    r = session.get(MBTA_URL, headers=headers, timeout=20)
    r.raise_for_status()
    return [flatten(v) for v in r.json()["data"]]


def delivery_report(err, msg):
    if err is not None:
        log.error("delivery failed key=%s err=%s", msg.key(), err)


def main() -> None:
    api_key = os.getenv("MBTA_API_KEY")
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50, "acks": "all"})
    session = requests.Session()
    log.info("publishing to %s on %s every %ss", TOPIC, BOOTSTRAP, POLL_SECONDS)

    while True:
        started = time.monotonic()
        try:
            rows = fetch_vehicles(session, api_key)
            for row in rows:
                producer.produce(
                    TOPIC,
                    key=row["vehicle_id"],
                    value=json.dumps(row).encode(),
                    callback=delivery_report,
                )
            producer.flush(10)
            log.info("published %d vehicles", len(rows))
        except requests.RequestException as e:
            log.warning("MBTA request failed: %s", e)
        except Exception:
            log.exception("unexpected error")
        time.sleep(max(0, POLL_SECONDS - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
