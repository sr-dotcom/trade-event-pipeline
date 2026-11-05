"""Rebuild proof: delete every table, rebuild from the raw file, and the fingerprints must match.

A fingerprint is one short code for a whole table. It does not depend on the order of the rows.
"""

from pathlib import Path

from src.generator.generate import generate, write_output
from src.pipeline.replay import TABLES, fingerprint, replay
from src.pipeline.run_local import run


def table_of(spark, rows):
    values = ", ".join(f"('{a}', {b})" for a, b in rows)
    return spark.sql(f"SELECT * FROM VALUES {values} AS t(trade_id, quantity)")


def test_fingerprint_ignores_row_order(spark):
    rows = [("T-1", 10), ("T-2", 20), ("T-3", 30)]
    assert fingerprint(table_of(spark, rows)) == fingerprint(table_of(spark, rows[::-1]))


def test_fingerprint_changes_when_one_value_changes(spark):
    a = table_of(spark, [("T-1", 10), ("T-2", 20)])
    b = table_of(spark, [("T-1", 10), ("T-2", 21)])
    assert fingerprint(a) != fingerprint(b)


def test_fingerprint_changes_when_a_row_is_missing_or_repeated(spark):
    base = [("T-1", 10), ("T-2", 20)]
    assert fingerprint(table_of(spark, base)) != fingerprint(table_of(spark, base[:1]))
    assert fingerprint(table_of(spark, base)) != fingerprint(table_of(spark, [*base, base[0]]))


def test_a_null_is_not_the_same_as_the_text_null(spark):
    a = spark.sql("SELECT * FROM VALUES ('T-1', CAST(NULL AS STRING)) AS t(trade_id, note)")
    b = spark.sql("SELECT * FROM VALUES ('T-1', 'null') AS t(trade_id, note)")
    assert fingerprint(a) != fingerprint(b)


def test_replay_rebuilds_identical_tables(spark, tmp_path):
    records, known = generate(n_events=5_000, seed=7, profile="A")
    write_output(records, known, tmp_path / "gen")
    events, lake = str(tmp_path / "gen" / "events.ndjson"), tmp_path / "lake"
    run(spark, events, str(lake))

    result = replay(spark, events, str(lake))

    assert set(result["before"]) == set(TABLES)  # all 6 tables have a fingerprint
    assert result["after"] == result["before"]  # the rebuild is exact
    assert set(result["deleted"]) == set(TABLES)  # and we really deleted every table first
    assert all((Path(lake) / name).exists() for name in TABLES)  # and the tables are back


def test_replay_notices_a_changed_archive(spark, tmp_path):
    records, known = generate(n_events=2_000, seed=7, profile="A")
    write_output(records, known, tmp_path / "gen")
    events, lake = tmp_path / "gen" / "events.ndjson", str(tmp_path / "lake")
    run(spark, str(events), lake)
    lines = events.read_text(encoding="utf-8").splitlines()
    events.write_text("\n".join(lines[1:]) + "\n", encoding="utf-8")  # one raw event is gone

    result = replay(spark, str(events), lake)

    assert result["after"]["bronze"] != result["before"]["bronze"]
