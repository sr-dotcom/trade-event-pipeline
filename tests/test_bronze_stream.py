"""Bronze reads new events only and saves its place (checkpoint). Silver and Gold recompute from all of Bronze.

The test source is a folder of NDJSON files. It stands in for Event Hubs. See DECISIONS.md.
"""

import json

from src.pipeline.bronze import read_events, read_events_stream, stream_bronze, to_bronze
from src.pipeline.replay import fingerprint
from src.pipeline.silver import build_silver
from tests.helpers import REF, ev


class Lake:
    """A landing folder, a Bronze table and a checkpoint folder under one temp folder."""

    def __init__(self, spark, root):
        self.spark = spark
        self.landing, self.bronze, self.checkpoint = root / "landing", root / "bronze", root / "checkpoint"
        self.landing.mkdir()

    def drop(self, name, records):
        text = "".join(json.dumps(r) + "\n" for r in records)
        (self.landing / f"{name}.ndjson").write_text(text, encoding="utf-8")

    def run(self, max_files=None):
        raw = read_events_stream(self.spark, self.landing, max_files)
        return stream_bronze(raw, self.bronze, self.checkpoint)

    def table(self):
        return self.spark.read.format("delta").load(str(self.bronze))

    def one_shot(self):
        return to_bronze(read_events(self.spark, str(self.landing)))


FILE_1 = [ev("E-1"), ev("E-2", quantity=100), ev("E-3", currency="XXX")]  # E-3 is a bad record
FILE_2 = [
    ev("E-1", arrival="2026-11-02T14:00:30.000Z"),  # exact copy of E-1: a duplicate
    ev("E-2", arrival="2026-11-02T14:00:40.000Z", quantity=999),  # same id, other content: a conflict
    ev("E-4"),
]


def test_each_run_reads_only_the_new_files(spark, tmp_path):
    lake = Lake(spark, tmp_path)
    lake.drop("f1", FILE_1)
    lake.run()
    assert lake.table().count() == 3
    lake.run()  # nothing new
    assert lake.table().count() == 3
    lake.drop("f2", FILE_2)
    lake.run()
    assert lake.table().count() == 6


def test_a_lost_checkpoint_makes_the_next_run_read_everything_again(spark, tmp_path):
    lake = Lake(spark, tmp_path)
    lake.drop("f1", FILE_1)
    lake.run()
    lake.checkpoint = tmp_path / "new-checkpoint"  # the old one is lost
    lake.run()
    assert lake.table().count() == 6  # the same 3 events twice. This is why a new checkpoint needs an empty Bronze.


def test_incremental_bronze_equals_one_shot_bronze(spark, tmp_path):
    lake = Lake(spark, tmp_path)
    lake.drop("f1", FILE_1)
    lake.run()
    lake.drop("f2", FILE_2)
    lake.run()
    assert fingerprint(lake.table()) == fingerprint(lake.one_shot())


def test_silver_from_incremental_bronze_equals_silver_from_one_shot_bronze(spark, tmp_path):
    lake = Lake(spark, tmp_path)
    lake.drop("f1", FILE_1)
    lake.run()
    lake.drop("f2", FILE_2)
    lake.run()
    incremental = build_silver(lake.table(), REF)
    one_shot = build_silver(lake.one_shot(), REF)
    for name, left, right in zip(("silver", "quarantine", "duplicates"), incremental, one_shot, strict=True):
        assert fingerprint(left) == fingerprint(right), name
    silver, quarantine, duplicates = incremental
    assert (silver.count(), quarantine.count(), duplicates.count()) == (3, 2, 1)  # E-1 E-2 E-4 / E-3 bad, E-2 conflict / E-1 copy


def test_a_small_trigger_makes_several_micro_batches_and_the_same_table(spark, tmp_path):
    lake = Lake(spark, tmp_path)
    lake.drop("f1", FILE_1)
    lake.drop("f2", FILE_2)
    query = lake.run(max_files=1)
    batches = [p for p in query.recentProgress if p["numInputRows"] > 0]
    assert len(batches) == 2
    assert fingerprint(lake.table()) == fingerprint(lake.one_shot())
