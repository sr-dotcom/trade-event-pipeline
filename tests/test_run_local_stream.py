"""run_local reads Bronze through the stream and its checkpoint. Silver, current state and Gold recompute from all of Bronze."""

import json

import pytest

from src.generator.generate import generate, write_output
from src.pipeline.bronze import read_events, to_bronze
from src.pipeline.run_local import run
from tests.helpers import ev


def bronze_rows(spark, lake):
    return spark.read.format("delta").load(str(lake / "bronze")).count()


def test_a_second_run_on_the_same_file_adds_no_bronze_rows(spark, tmp_path):
    records, known = generate(n_events=2_000, seed=7, profile="A")
    write_output(records, known, tmp_path / "gen")
    first = run(spark, str(tmp_path / "gen" / "events.ndjson"), str(tmp_path / "lake"))
    second = run(spark, str(tmp_path / "gen" / "events.ndjson"), str(tmp_path / "lake"))
    assert first["events_in"] == 2_000
    assert second == first  # the checkpoint remembers the file. Nothing is read twice.


def test_a_new_file_adds_only_its_events_and_the_tables_recompute_from_all_of_bronze(spark, tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    for n, records in enumerate([[ev("E-1"), ev("E-2", quantity=100)], [ev("E-3"), ev("E-1", arrival="2026-11-02T14:00:30.000Z")]]):
        (landing / f"part-{n}.ndjson").write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")
        counts = run(spark, str(landing), str(tmp_path / "lake"))
    assert counts["events_in"] == 4  # 2 from file 1, 2 from file 2
    assert (counts["silver"], counts["quarantine"], counts["duplicates"]) == (3, 0, 1)  # E-1 again is a duplicate
    one_shot = to_bronze(read_events(spark, str(landing))).count()
    assert one_shot == bronze_rows(spark, tmp_path / "lake")


def test_run_refuses_a_bronze_table_that_has_no_checkpoint(spark, tmp_path):
    lake = tmp_path / "lake"
    to_bronze(spark.createDataFrame([(json.dumps(ev("E-1")),)], "value string")).write.format("delta").save(str(lake / "bronze"))
    events = tmp_path / "events.ndjson"
    events.write_text(json.dumps(ev("E-1")) + "\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="checkpoint"):
        run(spark, str(events), str(lake))
    assert bronze_rows(spark, lake) == 1  # nothing was appended
