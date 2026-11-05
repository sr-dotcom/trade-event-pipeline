"""Silver rules. Hand-made events first (easy to read), then the generator's known answer."""

import json

import pytest
from pyspark.sql import functions as F

from src.generator.generate import generate, write_output
from src.pipeline.bronze import read_events, to_bronze
from src.pipeline.silver import build_silver
from tests.helpers import DROP, REF, ev, run


def reasons(quarantine):
    return {json.loads(r["value"])["event_id"]: r["reason"] for r in quarantine.collect()}


def test_valid_event_goes_to_silver_with_types(spark, tmp_path):
    _, silver, quarantine, duplicates = run(spark, tmp_path, [ev("E-1")])
    assert (silver.count(), quarantine.count(), duplicates.count()) == (1, 0, 0)
    row = silver.collect()[0]
    assert row["quantity"] == 100 and row["trade_version"] == 1
    assert float(row["price"]) == 10.5
    assert row["source"] == "synthetic" and row["is_late"] is False
    types = dict(silver.dtypes)
    assert types["quantity"] == "int" and types["trade_version"] == "int"
    assert types["event_time_utc"] == "timestamp" and types["ingest_time"] == "timestamp"


def test_exact_copy_is_a_duplicate(spark, tmp_path):
    later = ev("E-1", arrival="2026-11-02T14:00:30.000Z")
    _, silver, quarantine, duplicates = run(spark, tmp_path, [ev("E-1"), later])
    assert (silver.count(), quarantine.count(), duplicates.count()) == (1, 0, 1)


def test_same_id_different_content_is_a_conflict(spark, tmp_path):
    changed = ev("E-1", arrival="2026-11-02T14:00:30.000Z", quantity=999)
    _, silver, quarantine, duplicates = run(spark, tmp_path, [ev("E-1"), changed])
    assert silver.collect()[0]["quantity"] == 100  # the first arrival stays
    assert reasons(quarantine) == {"E-1": "event_id_conflict"}
    assert duplicates.count() == 0


def kept_id(bronze, quantity):
    """The Bronze id of the raw line with this quantity."""
    return next(r["bronze_id"] for r in bronze.collect() if json.loads(r["value"]).get("quantity") == quantity)


def test_conflict_row_says_which_row_stayed_and_which_fields_differ(spark, tmp_path):
    changed = ev("E-1", arrival="2026-11-02T14:00:30.000Z", quantity=999, price=11.0)
    bronze, _, quarantine, _ = run(spark, tmp_path, [ev("E-1"), changed])
    row = quarantine.collect()[0]
    assert row["kept_bronze_id"] == kept_id(bronze, 100)  # points to the row that stayed in Silver
    assert row["differing_fields"] == "quantity,price"  # in contract order


def test_a_bad_record_has_no_conflict_details(spark, tmp_path):
    _, _, quarantine, _ = run(spark, tmp_path, [ev("E-1", currency="XXX")])
    row = quarantine.collect()[0]
    assert row["kept_bronze_id"] is None and row["differing_fields"] is None


def test_first_arrival_wins_even_if_it_is_later_in_the_file(spark, tmp_path):
    first_in_time = ev("E-1", arrival="2026-11-02T14:00:01.000Z", quantity=100)
    second_in_time = ev("E-1", arrival="2026-11-02T14:00:09.000Z", quantity=999)
    _, silver, quarantine, _ = run(spark, tmp_path, [second_in_time, first_in_time])
    assert silver.collect()[0]["quantity"] == 100
    assert reasons(quarantine) == {"E-1": "event_id_conflict"}


BAD = {
    "missing_field": [("price", DROP), ("price", None), ("trade_id", DROP), ("event_time_utc", None)],
    "bad_value": [
        ("quantity", "abc"),
        ("quantity", 0),
        ("quantity", -5),
        ("price", 0),
        ("price", -1.5),
        ("side", "HOLD"),
        ("action", "MODIFY"),
        ("currency", "XXX"),
        ("event_time_utc", "not-a-time"),
        ("trade_version", 0),
    ],
    "unknown_reference": [("instrument_id", "UNKNOWN-1"), ("counterparty_id", "UNKNOWN-2"), ("book_id", "UNKNOWN-3")],
}


def test_bad_records_go_to_quarantine_with_a_reason(spark, tmp_path):
    records, expected = [], {}
    for reason, cases in BAD.items():
        for i, (field, value) in enumerate(cases):
            event_id = f"E-{reason}-{i}"
            records.append(ev(event_id, **{field: value}))
            expected[event_id] = reason
    _, silver, quarantine, duplicates = run(spark, tmp_path, records)
    assert silver.count() == 0 and duplicates.count() == 0
    assert reasons(quarantine) == expected


@pytest.mark.parametrize("minutes_after, late", [(4, False), (5, False), (6, True), (180, True)])
def test_late_flag_is_more_than_5_minutes(spark, tmp_path, minutes_after, late):
    arrival = f"2026-11-02T{14 + minutes_after // 60}:{minutes_after % 60:02d}:00.000Z"
    _, silver, _, _ = run(spark, tmp_path, [ev("E-1", arrival=arrival)])
    assert silver.collect()[0]["is_late"] is late


def test_late_event_is_never_dropped(spark, tmp_path):
    _, silver, quarantine, _ = run(spark, tmp_path, [ev("E-1", arrival="2026-11-02T20:00:00.000Z")])
    assert silver.count() == 1 and quarantine.count() == 0


# ---- the real run, compared with the generator's known answer ----


@pytest.fixture(scope="module", params=[("A", 50_000), ("B", 20_000)], ids=["profile_A_50000", "profile_B_20000"])
def run_known(request, spark, tmp_path_factory):
    profile, n = request.param
    out = tmp_path_factory.mktemp(f"gen_{profile}")
    records, known = generate(n_events=n, seed=42, profile=profile)
    write_output(records, known, out)
    bronze = to_bronze(read_events(spark, str(out / "events.ndjson"))).cache()
    silver, quarantine, duplicates = (df.cache() for df in build_silver(bronze, REF))
    yield bronze, silver, quarantine, duplicates, known
    for df in (bronze, silver, quarantine, duplicates):
        df.unpersist()


def test_nothing_is_lost(run_known):
    bronze, silver, quarantine, duplicates, known = run_known
    assert bronze.count() == known["n_events"]
    assert silver.count() + quarantine.count() + duplicates.count() == known["n_events"]


def test_silver_matches_known_answer(run_known):
    _, silver, _, _, known = run_known
    exp = known["expected"]
    assert silver.count() == exp["good_unique_events"]
    assert silver.select("event_id").distinct().count() == exp["good_unique_events"]
    assert silver.filter("is_late").count() == exp["late_flagged"]
    trades = silver.select("trade_id").distinct().count()
    assert trades == exp["trades"]


def test_silver_has_no_empty_required_field(run_known):
    _, silver, _, _, _ = run_known
    required = [c for c in silver.columns if c != "is_late"]
    assert silver.filter(" OR ".join(f"{c} IS NULL" for c in required)).count() == 0
    assert silver.filter("quantity <= 0 OR price <= 0").count() == 0


def test_duplicates_match_known_answer(run_known):
    _, _, _, duplicates, known = run_known
    assert duplicates.count() == known["expected"]["duplicates"]


def test_conflicts_in_the_real_run_differ_in_quantity_only(run_known):
    _, _, quarantine, _, known = run_known
    rows = quarantine.filter("reason = 'event_id_conflict'").collect()
    assert len(rows) == known["injected"]["conflict"]
    assert all(r["differing_fields"] == "quantity" for r in rows)  # the generator changes only the quantity
    assert all(r["kept_bronze_id"] is not None for r in rows)


def test_quarantine_matches_known_answer(run_known):
    _, _, quarantine, _, known = run_known
    exp = known["expected"]
    assert quarantine.count() == exp["quarantine"]
    got = {r["reason"]: r["n"] for r in quarantine.groupBy("reason").agg(F.count("*").alias("n")).collect()}
    assert got == exp["quarantine_by_reason"]
