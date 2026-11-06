"""A lake built in two runs can hold one more Gold version than a lake built in one run. The latest version is the same.

Why: run 1 sees half the events and publishes its own versions, because late events in that half change the numbers.
Run 2 then publishes again for all events. Old versions stay (append-only). A single run has no half-data stage.
On Azure the 50,000 events came in two halves: 3 Gold versions, against 2 on a laptop in one run.
The same split run on a laptop gave 3 versions too (docs/azure-run-results.md).

This is a characterization test. The code did not change, so it was green at the first run.
"""

from src.generator.generate import generate, write_output
from src.pipeline.run_local import run


def versions(spark, lake):
    gold = spark.read.format("delta").load(str(lake / "gold_report"))
    numbers = sorted(r[0] for r in gold.select("report_version").distinct().collect())
    latest = gold.filter(gold.report_version == numbers[-1]).drop("report_version", "published_at")
    return numbers, sorted(tuple(r) for r in latest.collect())


def test_two_runs_keep_every_version_and_end_on_the_same_report_as_one_run(spark, tmp_path):
    records, known = generate(n_events=2_000, seed=7, profile="A")
    write_output(records, known, tmp_path / "gen")
    lines = (tmp_path / "gen" / "events.ndjson").read_text(encoding="utf-8").splitlines(keepends=True)

    one = tmp_path / "one"
    (one / "in").mkdir(parents=True)
    (one / "in" / "all.ndjson").write_text("".join(lines), encoding="utf-8")
    run(spark, str(one / "in"), str(one / "lake"))

    two = tmp_path / "two"
    (two / "in").mkdir(parents=True)
    (two / "in" / "p1.ndjson").write_text("".join(lines[:1000]), encoding="utf-8")
    run(spark, str(two / "in"), str(two / "lake"))
    (two / "in" / "p2.ndjson").write_text("".join(lines[1000:]), encoding="utf-8")
    run(spark, str(two / "in"), str(two / "lake"))

    numbers_one, latest_one = versions(spark, one / "lake")
    numbers_two, latest_two = versions(spark, two / "lake")
    assert numbers_two == list(range(1, len(numbers_two) + 1))  # no gap in the numbers
    assert len(numbers_two) >= len(numbers_one)  # the split run can only add versions
    assert latest_two == latest_one  # the end state is the same
