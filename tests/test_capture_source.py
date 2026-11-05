"""Read the Event Hubs Capture archive (Avro). A rebuild from it must give the same tables as the file source.

The fixture follows the Capture record layout from the Azure docs (general knowledge, UNVERIFIED until Stage 2 reads a real file).
"""

import json

from pyspark.sql.types import BinaryType, LongType, MapType, StringType, StructField, StructType

from src.pipeline.bronze import read_events, to_bronze
from src.pipeline.capture_source import read_events_capture
from src.pipeline.replay import fingerprint
from src.pipeline.silver import build_silver
from tests.helpers import REF, ev

CAPTURE_SCHEMA = StructType(
    [
        StructField("SequenceNumber", LongType()),
        StructField("Offset", StringType()),
        StructField("EnqueuedTimeUtc", StringType()),
        StructField("SystemProperties", MapType(StringType(), StringType())),
        StructField("Properties", MapType(StringType(), StringType())),
        StructField("Body", BinaryType()),
    ]
)

RECORDS = [
    ev("E-1"),
    ev("E-1", arrival="2026-11-02T14:00:30.000Z"),  # exact copy: a duplicate
    ev("E-2", quantity=100),
    ev("E-2", arrival="2026-11-02T14:00:40.000Z", quantity=999),  # a conflict
    ev("E-3", currency="XXX"),  # a bad record
    ev("E-4"),
]
LINES = [json.dumps(r) for r in RECORDS]


def write_capture(spark, root, partition, lines):
    """One Avro file per partition, in the Capture folder layout."""
    rows = [(n, str(n * 100), "11/2/2026 2:00:01 PM", {}, {}, bytearray(line.encode())) for n, line in enumerate(lines)]
    folder = root / "ns" / "hub" / str(partition) / "2026" / "11" / "02" / "14" / "00" / "00"
    spark.createDataFrame(rows, CAPTURE_SCHEMA).coalesce(1).write.format("avro").save(str(folder))


def make_archive(spark, tmp_path):
    root = tmp_path / "archive"
    write_capture(spark, root, 0, LINES[:3])
    write_capture(spark, root, 1, LINES[3:])
    return root


def test_the_reader_returns_the_sent_lines_as_text(spark, tmp_path):
    raw = read_events_capture(spark, str(make_archive(spark, tmp_path)))
    assert raw.columns == ["value"]
    assert sorted(r["value"] for r in raw.collect()) == sorted(LINES)


def test_bronze_from_the_archive_equals_bronze_from_the_file(spark, tmp_path):
    archive = make_archive(spark, tmp_path)
    path = tmp_path / "events.ndjson"
    path.write_text("".join(line + "\n" for line in LINES), encoding="utf-8")
    from_archive = to_bronze(read_events_capture(spark, str(archive)))
    from_file = to_bronze(read_events(spark, str(path)))
    assert fingerprint(from_archive) == fingerprint(from_file)  # same value, ingest_time, source and bronze_id


def test_silver_quarantine_and_duplicates_from_the_archive_equal_the_file_result(spark, tmp_path):
    archive = make_archive(spark, tmp_path)
    path = tmp_path / "events.ndjson"
    path.write_text("".join(line + "\n" for line in LINES), encoding="utf-8")
    left = build_silver(to_bronze(read_events_capture(spark, str(archive))), REF)
    right = build_silver(to_bronze(read_events(spark, str(path))), REF)
    for name, a, b in zip(("silver", "quarantine", "duplicates"), left, right, strict=True):
        assert fingerprint(a) == fingerprint(b), name
    assert tuple(t.count() for t in left) == (3, 2, 1)
