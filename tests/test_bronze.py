"""Bronze keeps every raw line, bad or not. It only adds ingest_time and source."""

import json
from datetime import UTC, datetime

from src.pipeline.bronze import read_events, to_bronze

LINES = [
    json.dumps({"event_id": "E-1", "quantity": 5, "arrival_time_utc": "2026-11-02T14:00:01.250Z"}),
    json.dumps({"event_id": "E-2", "quantity": "abc", "arrival_time_utc": "2026-11-02T14:00:02.000Z"}),  # bad value
    json.dumps({"event_id": "E-3", "arrival_time_utc": "2026-11-02T14:00:03.000Z"}),  # missing fields
]


def test_bronze_keeps_every_line_unchanged(spark, tmp_path):
    path = tmp_path / "events.ndjson"
    path.write_text("\n".join(LINES) + "\n", encoding="utf-8")
    bronze = to_bronze(read_events(spark, str(path)))
    rows = bronze.orderBy("ingest_time").collect()  # the lines arrive in file order
    assert [r["value"] for r in rows] == LINES  # raw text, nothing parsed or dropped
    assert bronze.columns == ["value", "ingest_time", "source", "bronze_id"]


def test_bronze_adds_ingest_time_and_source(spark, tmp_path):
    path = tmp_path / "events.ndjson"
    path.write_text("\n".join(LINES) + "\n", encoding="utf-8")
    rows = to_bronze(read_events(spark, str(path))).orderBy("ingest_time").collect()
    assert all(r["source"] == "synthetic" for r in rows)
    first = rows[0]["ingest_time"].astimezone(UTC)
    assert first == datetime(2026, 11, 2, 14, 0, 1, 250000, tzinfo=UTC)
