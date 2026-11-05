"""bronze_id depends only on the raw text. A rebuild then gives the same ids. See DECISIONS.md."""

import json

from pyspark.sql import functions as F

from src.pipeline.bronze import read_events, to_bronze
from tests.helpers import ev, run


def write(path, records):
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")


def ids(spark, path, partitions=1):
    raw = read_events(spark, str(path)).repartition(partitions)
    return {r["value"]: r["bronze_id"] for r in to_bronze(raw).collect()}


def test_same_ids_for_any_partitioning(spark, tmp_path):
    write(tmp_path / "e.ndjson", [ev(f"E-{n}") for n in range(8)])
    assert ids(spark, tmp_path / "e.ndjson", 1) == ids(spark, tmp_path / "e.ndjson", 3)


def test_same_line_gets_the_same_id_in_two_batches(spark, tmp_path):
    write(tmp_path / "a.ndjson", [ev("E-1"), ev("E-2")])
    write(tmp_path / "b.ndjson", [ev("E-2")])
    line = json.dumps(ev("E-2"))
    assert ids(spark, tmp_path / "a.ndjson")[line] == ids(spark, tmp_path / "b.ndjson")[line]


def test_different_lines_get_different_ids_and_other_columns_do_not_matter(spark, tmp_path):
    write(tmp_path / "e.ndjson", [ev(f"E-{n}") for n in range(8)])
    plain = ids(spark, tmp_path / "e.ndjson")
    assert len(set(plain.values())) == 8
    raw = read_events(spark, str(tmp_path / "e.ndjson")).withColumn("kafka_time", F.current_timestamp())
    with_time = {r["value"]: r["bronze_id"] for r in to_bronze(raw).collect()}
    assert with_time == plain


def test_a_conflict_with_the_same_arrival_time_is_decided_the_same_way_in_any_file_order(spark, tmp_path):
    first, second = ev("E-1", quantity=100), ev("E-1", quantity=999)  # same arrival time
    (tmp_path / "ab").mkdir()
    (tmp_path / "ba").mkdir()
    _, silver_ab, _, _ = run(spark, tmp_path / "ab", [first, second])
    _, silver_ba, _, _ = run(spark, tmp_path / "ba", [second, first])
    assert silver_ab.collect()[0]["quantity"] == silver_ba.collect()[0]["quantity"]


def test_two_identical_lines_still_make_one_silver_row_and_one_duplicate(spark, tmp_path):
    _, silver, quarantine, duplicates = run(spark, tmp_path, [ev("E-1"), ev("E-1")])
    assert (silver.count(), quarantine.count(), duplicates.count()) == (1, 0, 1)
