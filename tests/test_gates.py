import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import duckdb

from quality import gates


def _con_with(rows_sql: str):
    con = duckdb.connect()
    con.execute(f"CREATE VIEW raw AS {rows_sql}")
    return con


def test_geo_gate_fails_on_out_of_bounds():
    con = _con_with("SELECT 42.3 AS latitude, -71.1 AS longitude UNION ALL SELECT 0.0, 0.0")
    assert gates.gate_geo(con).passed is False


def test_geo_gate_passes_in_bounds():
    con = _con_with("SELECT 42.3 AS latitude, -71.1 AS longitude")
    assert gates.gate_geo(con).passed is True


def test_duplicate_gate_detects_dupes():
    con = _con_with(
        "SELECT 'v1' AS vehicle_id, strftime(now() AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS updated_at "
        "UNION ALL SELECT 'v1', strftime(now() AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ')"
    )
    assert gates.gate_duplicates(con).passed is False
