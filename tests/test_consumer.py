import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from consumer.consumer import dedupe, group_by_partition, partition_key


def row(vid, updated, ingest="2026-10-22T15:05:00+00:00"):
    return {"vehicle_id": vid, "updated_at": updated, "ingest_time": ingest}


def test_dedupe_keeps_one_per_vehicle_event():
    rows = [row("y1", "2026-10-22T15:00:00Z"), row("y1", "2026-10-22T15:00:00Z"), row("y1", "2026-10-22T15:00:15Z")]
    assert len(dedupe(rows)) == 2


def test_partition_uses_event_time_not_ingest_time():
    # event at 14:59, ingested at 15:05 → must land in hour=14 (late arrival)
    assert partition_key("2026-10-22T14:59:50Z", "2026-10-22T15:05:00+00:00") == ("2026-10-22", "14")


def test_partition_falls_back_to_ingest_time():
    assert partition_key(None, "2026-10-22T15:05:00+00:00") == ("2026-10-22", "15")


def test_group_by_partition_splits_across_hours():
    rows = [row("a", "2026-10-22T14:59:00Z"), row("b", "2026-10-22T15:01:00Z"), row("c", "2026-10-22T15:30:00Z")]
    g = group_by_partition(rows)
    assert set(g) == {("2026-10-22", "14"), ("2026-10-22", "15")}
    assert len(g[("2026-10-22", "15")]) == 2
