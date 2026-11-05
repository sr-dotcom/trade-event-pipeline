"""The local run writes Bronze, Silver, quarantine and duplicates as Delta tables."""

from src.generator.generate import generate, write_output
from src.pipeline.run_local import run


def test_run_local_writes_delta_tables_that_match_the_known_answer(spark, tmp_path):
    records, known = generate(n_events=5_000, seed=7, profile="A")
    write_output(records, known, tmp_path / "gen")
    counts = run(spark, str(tmp_path / "gen" / "events.ndjson"), str(tmp_path / "lake"))

    exp = known["expected"]
    assert counts.pop("gold_versions") == 2  # a late event changed the 14:15 report after the first publish
    assert counts == {
        "events_in": 5_000,
        "silver": exp["good_unique_events"],
        "quarantine": exp["quarantine"],
        "duplicates": exp["duplicates"],
        "late_flagged": exp["late_flagged"],
        "trades": exp["trades"],
        "active_trades": exp["active_trades"],
        "events_lost": 0,
    }
    for table, n in (
        ("bronze", 5_000),
        ("silver", exp["good_unique_events"]),
        ("quarantine", exp["quarantine"]),
        ("duplicates", exp["duplicates"]),
        ("current_state", exp["trades"]),
    ):
        assert spark.read.format("delta").load(str(tmp_path / "lake" / table)).count() == n

    gold = spark.read.format("delta").load(str(tmp_path / "lake" / "gold_report"))
    assert {r["report_version"] for r in gold.collect()} == {1, 2}
