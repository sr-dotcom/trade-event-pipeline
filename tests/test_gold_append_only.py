"""Gold is append-only across runs. A published version is never rewritten. A change becomes a new version.

run_local publishes at 14:20 (version 1) and again after the last arrival. See DECISIONS.md.
"""

import json

from src.pipeline.run_local import run
from tests.helpers import ev

FIRST = ev("E-1")  # trade T-000001, version 1, quantity 100, arrives 14:00:01
AMEND = ev(
    "E-2",
    arrival="2026-11-02T14:05:00.000Z",
    trade_version=2,
    action="AMEND",
    quantity=250,
    event_time_utc="2026-11-02T14:05:00Z",
)  # arrives before the 14:20 publish, so it changes what version 1 would show if we recomputed it


def run_with(spark, tmp_path, new_records):
    """Each call drops the new events as a new file in the landing folder, then runs. No new events, no new file."""
    landing = tmp_path / "landing"
    landing.mkdir(exist_ok=True)
    if new_records:
        text = "".join(json.dumps(r) + "\n" for r in new_records)
        (landing / f"part-{len(list(landing.iterdir()))}.ndjson").write_text(text, encoding="utf-8")
    run(spark, str(landing), str(tmp_path / "lake"))
    gold = spark.read.format("delta").load(str(tmp_path / "lake" / "gold_report"))
    return {tuple(r) for r in gold.collect()}


def test_a_second_run_never_rewrites_a_published_version(spark, tmp_path):
    first = run_with(spark, tmp_path, [FIRST])
    second = run_with(spark, tmp_path, [AMEND])  # the AMEND arrives as a new file
    assert first <= second  # every row published in run 1 is still there, unchanged
    assert {row[1] for row in second} == {1, 2}  # the change is a new version
    assert {row[6] for row in first} == {100}
    assert {row[6] for row in second if row[1] == 2} == {250}


def test_running_the_same_input_again_adds_no_version(spark, tmp_path):
    first = run_with(spark, tmp_path, [FIRST, AMEND])
    again = run_with(spark, tmp_path, [])  # nothing new arrived
    assert first == again
