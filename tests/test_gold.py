"""Gold report: active trades per book at a cut-off, with numbered versions.

A report counts events that happened at or before the cut-off (event time)
and that arrived by the publish time (ingest time).
A late event that changes the report makes a new version. Old versions never change.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from src.generator.generate import generate, write_output
from src.pipeline.bronze import read_events, to_bronze
from src.pipeline.gold import build_report, empty_gold, publish_report
from src.pipeline.silver import build_silver
from tests.helpers import REF, ev, run

CUT = "2026-11-02T14:15:00Z"


def silver_of(spark, tmp_path, records):
    return run(spark, tmp_path, records)[1]


def as_dict(report):
    return {
        (r["book_id"], r["currency"]): (r["active_trades"], r["total_quantity"], r["notional"])
        for r in report.collect()
    }


# ---- hand-made events: the three steps of the picture ----

T1 = ev("E-1", trade_id="T-1", arrival="2026-11-02T14:00:05.000Z", event_time_utc="2026-11-02T14:00:00Z")
T2_LATE = ev("E-2", trade_id="T-2", arrival="2026-11-02T16:00:00.000Z", event_time_utc="2026-11-02T14:05:00Z")
T3_AFTER_CUTOFF = ev("E-3", trade_id="T-3", arrival="2026-11-02T16:10:00.000Z", event_time_utc="2026-11-02T15:30:00Z")


def test_report_uses_event_time_and_arrival_time(spark, tmp_path):
    silver = silver_of(spark, tmp_path, [T1, T2_LATE, T3_AFTER_CUTOFF])
    at_1420 = as_dict(build_report(silver, CUT, "2026-11-02T14:20:00Z"))
    at_1600 = as_dict(build_report(silver, CUT, "2026-11-02T16:00:00Z"))
    at_1610 = as_dict(build_report(silver, CUT, "2026-11-02T16:10:00Z"))
    assert at_1420 == {("BOOK-01", "USD"): (1, 100, Decimal("1050.0000"))}  # the late T-2 has not arrived yet
    assert at_1600 == {("BOOK-01", "USD"): (2, 200, Decimal("2100.0000"))}  # T-2 happened before the cut-off
    assert at_1610 == at_1600  # T-3 happened after the cut-off


def test_currencies_in_one_book_are_never_added_together(spark, tmp_path):
    usd = ev("E-1", trade_id="T-1", currency="USD", quantity=100, price=10.5, arrival="2026-11-02T14:00:05.000Z")
    eur = ev("E-2", trade_id="T-2", currency="EUR", quantity=100, price=10.5, arrival="2026-11-02T14:00:06.000Z")
    silver = silver_of(spark, tmp_path, [usd, eur])
    assert as_dict(build_report(silver, CUT, "2026-11-02T14:20:00Z")) == {
        ("BOOK-01", "USD"): (1, 100, Decimal("1050.0000")),
        ("BOOK-01", "EUR"): (1, 100, Decimal("1050.0000")),
    }


def test_amend_after_the_cutoff_is_not_in_the_report(spark, tmp_path):
    amend = ev(
        "E-9",
        trade_id="T-1",
        trade_version=2,
        action="AMEND",
        quantity=500,
        event_time_utc="2026-11-02T14:30:00Z",
        arrival="2026-11-02T14:30:01.000Z",
    )
    silver = silver_of(spark, tmp_path, [T1, amend])
    assert as_dict(build_report(silver, CUT, "2026-11-02T15:00:00Z"))[("BOOK-01", "USD")][1] == 100


def test_cancel_before_the_cutoff_removes_the_trade(spark, tmp_path):
    cancel_early = ev(
        "E-8",
        trade_id="T-1",
        trade_version=2,
        action="CANCEL",
        event_time_utc="2026-11-02T14:10:00Z",
        arrival="2026-11-02T14:10:01.000Z",
    )
    cancel_late = ev(
        "E-7",
        trade_id="T-2",
        trade_version=2,
        action="CANCEL",
        event_time_utc="2026-11-02T14:40:00Z",
        arrival="2026-11-02T14:40:01.000Z",
    )
    t2 = ev("E-6", trade_id="T-2", event_time_utc="2026-11-02T14:01:00Z", arrival="2026-11-02T14:01:01.000Z")
    silver = silver_of(spark, tmp_path, [T1, cancel_early, t2, cancel_late])
    # T-1 is cancelled before the cut-off. T-2 is cancelled after it, so at the cut-off it is still active.
    assert as_dict(build_report(silver, CUT, "2026-11-02T15:00:00Z")) == {
        ("BOOK-01", "USD"): (1, 100, Decimal("1050.0000"))
    }


def test_late_event_makes_a_new_version_and_old_versions_never_change(spark, tmp_path):
    silver = silver_of(spark, tmp_path, [T1, T2_LATE, T3_AFTER_CUTOFF])
    gold = empty_gold(spark)
    for as_of in ("2026-11-02T14:20:00Z", "2026-11-02T16:00:00Z", "2026-11-02T16:10:00Z"):
        gold = publish_report(gold, build_report(silver, CUT, as_of), CUT, as_of)
    versions = {}
    for r in gold.collect():
        versions.setdefault(r["report_version"], []).append(r)
    assert sorted(versions) == [1, 2]  # 14:20 = v1, 16:00 = v2, 16:10 = same report, no v3
    assert [(r["active_trades"], r["total_quantity"]) for r in versions[1]] == [
        (1, 100)
    ]  # v1 still shows the old numbers
    assert [(r["active_trades"], r["total_quantity"]) for r in versions[2]] == [(2, 200)]
    assert versions[2][0]["published_at"].astimezone(UTC) == datetime(2026, 11, 2, 16, 0, tzinfo=UTC)


def test_a_publish_time_earlier_than_the_latest_version_makes_no_new_version(spark, tmp_path):
    silver = silver_of(spark, tmp_path, [T1, T2_LATE])  # T2_LATE arrives at 16:00
    gold = empty_gold(spark)
    for as_of in ("2026-11-02T16:00:00Z", "2026-11-02T14:20:00Z"):  # the second time is in the past
        gold = publish_report(gold, build_report(silver, CUT, as_of), CUT, as_of)
    assert {r["report_version"] for r in gold.collect()} == {1}  # the 14:20 numbers are history, not a new version


def test_publishing_the_same_report_again_makes_no_new_version(spark, tmp_path):
    silver = silver_of(spark, tmp_path, [T1])
    gold = empty_gold(spark)
    for as_of in ("2026-11-02T14:20:00Z", "2026-11-02T14:30:00Z"):
        gold = publish_report(gold, build_report(silver, CUT, as_of), CUT, as_of)
    assert {r["report_version"] for r in gold.collect()} == {1}


# ---- the real run, compared with a report built in plain Python from the generator's records ----

START = datetime(2026, 11, 2, 14, 0, tzinfo=UTC)


def expected_report(records, cutoff, as_of):
    """Independent calculation. Good events are the first delivery of each event_id that is not a bad record."""
    first = {}
    for rec in records:  # records are in arrival order
        event_id = rec.get("event_id")
        if event_id is None or str(event_id).startswith("E-B") or event_id in first:
            continue
        first[event_id] = rec
    latest = {}
    for rec in first.values():
        arrived = datetime.strptime(rec["arrival_time_utc"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
        happened = datetime.strptime(rec["event_time_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        if arrived <= as_of and happened <= cutoff:
            cur = latest.get(rec["trade_id"])
            if cur is None or rec["trade_version"] > cur["trade_version"]:
                latest[rec["trade_id"]] = rec
    out = {}
    for rec in latest.values():
        if rec["action"] != "CANCEL":
            n, q, v = out.get((rec["book_id"], rec["currency"]), (0, 0, Decimal(0)))
            out[(rec["book_id"], rec["currency"])] = (
                n + 1,
                q + rec["quantity"],
                v + rec["quantity"] * Decimal(str(rec["price"])),
            )
    return {k: (n, q, v.quantize(Decimal("0.0001"))) for k, (n, q, v) in out.items()}


@pytest.fixture(scope="module", params=[("A", 50_000), ("B", 20_000)], ids=["profile_A_50000", "profile_B_20000"])
def run_known(request, spark, tmp_path_factory):
    profile, n = request.param
    out = tmp_path_factory.mktemp(f"gen_{profile}")
    records, known = generate(n_events=n, seed=42, profile=profile)
    write_output(records, known, out)
    bronze = to_bronze(read_events(spark, str(out / "events.ndjson")))
    silver, _, _ = build_silver(bronze, REF)
    silver.cache()
    yield silver, records
    silver.unpersist()


def test_report_equals_independent_calculation(run_known):
    silver, records = run_known
    cutoff = datetime(2026, 11, 2, 14, 15, tzinfo=UTC)
    for as_of in ("2026-11-02T14:20:00Z", "2026-11-03T00:00:00Z"):
        report = build_report(silver, CUT, as_of)
        expected = expected_report(records, cutoff, datetime.fromisoformat(as_of))
        assert as_dict(report) == expected


def test_late_events_make_version_2_on_real_data(spark, run_known):
    silver, _records = run_known
    first, final = "2026-11-02T14:20:00Z", "2026-11-03T00:00:00Z"
    v1_report = build_report(silver, CUT, first)
    v2_report = build_report(silver, CUT, final)
    assert as_dict(v1_report) != as_dict(v2_report)  # late events happened before the cut-off
    gold = empty_gold(spark)
    for as_of, report in ((first, v1_report), (final, v2_report), ("2026-11-03T01:00:00Z", v2_report)):
        gold = publish_report(gold, report, CUT, as_of)
    assert {r["report_version"] for r in gold.collect()} == {1, 2}
    v1_now = {(r["book_id"], r["currency"]): r["active_trades"] for r in gold.filter("report_version = 1").collect()}
    assert v1_now == {book: vals[0] for book, vals in as_dict(v1_report).items()}  # v1 is unchanged
