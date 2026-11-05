"""Current state: for each trade, the event with the highest trade_version.

Version order wins, not arrival order (README.md, "The rules").
"""

import csv
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from src.generator.generate import generate, write_output
from src.pipeline.bronze import read_events, to_bronze
from src.pipeline.current_state import build_current_state
from src.pipeline.silver import build_silver
from tests.helpers import REF, ev, run

FIELDS = [
    "event_id",
    "trade_id",
    "trade_version",
    "action",
    "instrument_id",
    "counterparty_id",
    "book_id",
    "side",
    "quantity",
    "price",
    "currency",
    "event_time_utc",
]


def state_of(spark, tmp_path, records):
    _, silver, _, _ = run(spark, tmp_path, records)
    return build_current_state(silver)


def test_version_order_wins_over_arrival_order(spark, tmp_path):
    """The real trade T-003968: version 2 arrived 13 minutes after version 3."""
    records = [
        ev("E-1", arrival="2026-11-02T14:00:09.000Z", trade_version=1, action="NEW", quantity=770),
        ev("E-3", arrival="2026-11-02T14:10:31.000Z", trade_version=3, action="AMEND", quantity=610),
        ev("E-2", arrival="2026-11-02T14:20:38.000Z", trade_version=2, action="AMEND", quantity=990),
    ]
    rows = state_of(spark, tmp_path, records).collect()
    assert len(rows) == 1
    assert (rows[0]["trade_version"], rows[0]["quantity"], rows[0]["event_id"]) == (3, 610, "E-3")


def test_one_row_per_trade(spark, tmp_path):
    records = [
        ev("E-1", trade_id="T-1", trade_version=1),
        ev("E-2", trade_id="T-1", trade_version=2, action="AMEND", quantity=120),
        ev("E-3", trade_id="T-2", trade_version=1, quantity=50),
    ]
    rows = {r["trade_id"]: r for r in state_of(spark, tmp_path, records).collect()}
    assert set(rows) == {"T-1", "T-2"}
    assert rows["T-1"]["quantity"] == 120 and rows["T-2"]["quantity"] == 50


def test_cancel_is_the_current_state_and_not_active(spark, tmp_path):
    records = [
        ev("E-1", trade_id="T-1", trade_version=1),
        ev("E-2", trade_id="T-1", trade_version=2, action="CANCEL"),
        ev("E-3", trade_id="T-2", trade_version=1),
    ]
    rows = {r["trade_id"]: r for r in state_of(spark, tmp_path, records).collect()}
    assert rows["T-1"]["action"] == "CANCEL" and rows["T-1"]["is_active"] is False
    assert rows["T-2"]["is_active"] is True  # a trade that was never cancelled


def test_columns_are_silver_columns_plus_is_active(spark, tmp_path):
    _, silver, _, _ = run(spark, tmp_path, [ev("E-1")])
    assert build_current_state(silver).columns == [*silver.columns, "is_active"]


# ---- the real run, compared with the generator's known answer ----


def normal(row):
    return {
        **{k: row[k] for k in FIELDS},
        "trade_version": int(row["trade_version"]),
        "quantity": int(row["quantity"]),
        "price": float(Decimal(str(row["price"]))),
    }


@pytest.fixture(scope="module", params=[("A", 50_000), ("B", 20_000)], ids=["profile_A_50000", "profile_B_20000"])
def run_known(request, spark, tmp_path_factory):
    profile, n = request.param
    out = tmp_path_factory.mktemp(f"gen_{profile}")
    records, known = generate(n_events=n, seed=42, profile=profile)
    write_output(records, known, out)
    bronze = to_bronze(read_events(spark, str(out / "events.ndjson")))
    silver, _, _ = build_silver(bronze, REF)
    state = build_current_state(silver).cache()
    with open(out / "expected_current_state.csv", newline="") as f:
        expected = {r["trade_id"]: normal(r) for r in csv.DictReader(f)}
    yield state, expected, known
    state.unpersist()


def test_current_state_equals_known_answer(run_known):
    state, expected, _ = run_known
    got = {
        r["trade_id"]: normal(r)
        for r in state.withColumn(
            "event_time_utc", F.date_format("event_time_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'")
        ).collect()
    }
    assert got == expected


def test_current_state_counts_equal_known_answer(run_known):
    state, _, known = run_known
    exp = known["expected"]
    assert state.count() == exp["trades"]
    assert state.filter("is_active").count() == exp["active_trades"]
    assert state.filter("NOT is_active").count() == exp["cancelled_trades"]
