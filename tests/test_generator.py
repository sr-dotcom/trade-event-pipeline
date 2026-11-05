"""Generator tests. Write first, then the code.

The tests do not trust the generator's own labels. They check every record again
with their own small validator and compare the counts with the known answer.
"""

import csv
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import pytest

from src.generator.generate import generate, load_config, write_output

ROOT = Path(__file__).resolve().parent.parent
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
CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CHF"}


def _ids(name):
    with open(ROOT / "data" / "reference" / name, newline="") as f:
        return {next(iter(row.values())) for row in csv.DictReader(f)}


REF = {
    "instrument_id": _ids("instruments.csv"),
    "counterparty_id": _ids("counterparties.csv"),
    "book_id": _ids("books.csv"),
}


def parse_time(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def parse_arrival(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)


def classify(rec):
    """Return None for a valid record, else the reason."""
    if any(rec.get(f) is None for f in FIELDS):
        return "missing_field"
    try:
        if rec["action"] not in ("NEW", "AMEND", "CANCEL") or rec["side"] not in ("BUY", "SELL"):
            return "bad_value"
        if not (isinstance(rec["quantity"], int) and rec["quantity"] > 0):
            return "bad_value"
        if not (isinstance(rec["price"], (int, float)) and rec["price"] > 0):
            return "bad_value"
        if not (isinstance(rec["trade_version"], int) and rec["trade_version"] >= 1):
            return "bad_value"
        if rec["currency"] not in CURRENCIES:
            return "bad_value"
        parse_time(rec["event_time_utc"])
    except ValueError:
        return "bad_value"
    if any(rec[k] not in REF[k] for k in REF):
        return "unknown_reference"
    return None


def content(rec):
    return {k: rec[k] for k in FIELDS}


@pytest.fixture(scope="module")
def run_a():
    return generate(n_events=50_000, seed=42, profile="A")


@pytest.fixture(scope="module")
def run_b():
    return generate(n_events=50_000, seed=42, profile="B")


@pytest.fixture(scope="module")
def cfg_a():
    return load_config("A")


def split(records):
    """Return valid first deliveries, extra identical copies, conflicts, and bad records by reason."""
    first, dups, conflicts, bad = {}, [], [], defaultdict(list)
    for rec in records:  # records are in arrival order
        reason = classify(rec)
        if reason:
            bad[reason].append(rec)
        elif rec["event_id"] not in first:
            first[rec["event_id"]] = rec
        elif content(first[rec["event_id"]]) == content(rec):
            dups.append(rec)
        else:
            conflicts.append(rec)
    return first, dups, conflicts, bad


def test_same_seed_same_data(run_a):
    again = generate(n_events=50_000, seed=42, profile="A")
    assert again[0] == run_a[0]
    assert again[1] == run_a[1]


def test_other_seed_other_data(run_a):
    other = generate(n_events=50_000, seed=43, profile="A")
    assert other[0] != run_a[0]


def test_exact_event_count(run_a, run_b):
    assert len(run_a[0]) == 50_000
    assert len(run_b[0]) == 50_000
    assert run_a[1]["n_events"] == 50_000


def test_records_are_in_arrival_order(run_a):
    times = [parse_arrival(r["arrival_time_utc"]) for r in run_a[0]]
    assert times == sorted(times)


def test_rates_come_from_config(run_a, run_b, cfg_a):
    n = 50_000
    inj = run_a[1]["injected"]
    assert inj["duplicate_random"] == round(cfg_a["duplicate_rate"] * n)
    assert inj["duplicate_burst"] == cfg_a["duplicate_burst_size"]
    assert inj["out_of_order"] == round(cfg_a["out_of_order_rate"] * n)
    assert inj["late"] == round(cfg_a["late_rate"] * n)
    assert inj["very_late"] == round(cfg_a["very_late_rate"] * n)
    assert inj["bad_random"] == round(cfg_a["bad_rate"] * n)
    assert inj["conflict"] == cfg_a["conflict_count"]
    # profile B is the stress run: more of every random fault
    for key in ("duplicate_random", "out_of_order", "late", "very_late", "bad_random"):
        assert run_b[1]["injected"][key] > 3 * inj[key]


def test_valid_events_follow_the_contract(run_a):
    first, _, _, _ = split(run_a[0])
    for rec in first.values():
        assert set(rec) == set(FIELDS) | {"arrival_time_utc"}
    chains = defaultdict(list)
    for rec in first.values():
        chains[rec["trade_id"]].append(rec)
    for trade_id, recs in chains.items():
        recs.sort(key=lambda r: r["trade_version"])
        assert [r["trade_version"] for r in recs] == list(range(1, len(recs) + 1)), trade_id
        assert recs[0]["action"] == "NEW"
        assert all(r["action"] == "AMEND" for r in recs[1:-1])
        assert recs[-1]["action"] in ("AMEND", "CANCEL") or len(recs) == 1
        assert len({r["side"] for r in recs}) == 1
        times = [parse_time(r["event_time_utc"]) for r in recs]
        assert times == sorted(times)


def test_trade_mix(run_a):
    first, _, _, _ = split(run_a[0])
    actions = [r["action"] for r in first.values()]
    new, amend, cancel = (actions.count(a) for a in ("NEW", "AMEND", "CANCEL"))
    assert 0.30 <= amend / new <= 0.40  # about 35 AMEND per 100 NEW
    assert 0.04 <= cancel / new <= 0.08  # about 6 CANCEL per 100 NEW


def test_known_answer_unique_events_and_duplicates(run_a):
    known = run_a[1]
    first, dups, _, _ = split(run_a[0])
    assert known["expected"]["good_unique_events"] == len(first)
    assert known["expected"]["duplicates"] == len(dups)
    assert len(dups) == known["injected"]["duplicate_random"] + known["injected"]["duplicate_burst"]


def test_known_answer_quarantine(run_a):
    known = run_a[1]
    _, _, conflicts, bad = split(run_a[0])
    by_reason = known["expected"]["quarantine_by_reason"]
    assert by_reason["missing_field"] == len(bad["missing_field"])
    assert by_reason["bad_value"] == len(bad["bad_value"])
    assert by_reason["unknown_reference"] == len(bad["unknown_reference"])
    assert by_reason["event_id_conflict"] == len(conflicts) == 3
    total = len(conflicts) + sum(len(v) for v in bad.values())
    assert known["expected"]["quarantine"] == total
    assert total == known["injected"]["bad_random"] + known["injected"]["bad_window"] + 3


def test_bad_mix_is_roughly_as_configured(run_a, cfg_a):
    _, _, _, bad = split(run_a[0])
    total = sum(len(v) for v in bad.values())
    for reason, share in cfg_a["bad_mix"].items():
        assert abs(len(bad[reason]) / total - share) < 0.06, reason


def test_conflict_arrives_after_the_original(run_a):
    first, _, conflicts, _ = split(run_a[0])
    for rec in conflicts:
        assert parse_arrival(rec["arrival_time_utc"]) > parse_arrival(first[rec["event_id"]]["arrival_time_utc"])


def test_known_answer_late_events(run_a, cfg_a):
    known = run_a[1]
    first, _, _, _ = split(run_a[0])
    limit = cfg_a["late_after_seconds"]
    late = sum(
        (parse_arrival(r["arrival_time_utc"]) - parse_time(r["event_time_utc"])).total_seconds() > limit
        for r in first.values()
    )
    assert known["expected"]["late_flagged"] == late
    assert late >= known["injected"]["very_late"]  # hours late is always late


def test_known_answer_current_state(run_a):
    known = run_a[1]
    first, _, _, _ = split(run_a[0])
    latest = {}
    for rec in first.values():  # version order wins, not arrival order
        cur = latest.get(rec["trade_id"])
        if cur is None or rec["trade_version"] > cur["trade_version"]:
            latest[rec["trade_id"]] = rec
    state = known["current_state"]
    assert set(state) == set(latest)
    for trade_id, rec in latest.items():
        assert state[trade_id] == content(rec)
    cancelled = sum(r["action"] == "CANCEL" for r in latest.values())
    assert known["expected"]["trades"] == len(latest)
    assert known["expected"]["cancelled_trades"] == cancelled
    assert known["expected"]["active_trades"] == len(latest) - cancelled


def test_known_answer_version_inversions(run_a):
    known = run_a[1]
    first, _, _, _ = split(run_a[0])
    chains = defaultdict(list)
    for rec in first.values():
        chains[rec["trade_id"]].append(rec)
    inverted = 0
    for recs in chains.values():
        recs.sort(key=lambda r: parse_arrival(r["arrival_time_utc"]))
        versions = [r["trade_version"] for r in recs]
        inverted += versions != sorted(versions)
    assert known["expected"]["version_inversion_trades"] == inverted
    assert inverted > 0  # the data must really test "version order wins"


def test_outage_events_arrive_after_the_outage(run_a, cfg_a):
    known = run_a[1]
    first, _, _, _ = split(run_a[0])
    start, end = parse_time(known["outage"]["start_utc"]), parse_time(known["outage"]["end_utc"])
    assert (end - start).total_seconds() == cfg_a["outage_minutes"] * 60
    inside = [r for r in first.values() if start <= parse_time(r["event_time_utc"]) < end]
    assert len(inside) > 0
    assert all(parse_arrival(r["arrival_time_utc"]) >= end for r in inside)


def test_bad_window_has_many_bad_records(run_a, cfg_a):
    known = run_a[1]
    start, end = parse_time(known["bad_window"]["start_utc"]), parse_time(known["bad_window"]["end_utc"])
    assert (end - start).total_seconds() == cfg_a["bad_window_minutes"] * 60
    in_window = [
        r
        for r in run_a[0]
        if classify(r) and r.get("event_time_utc") and start <= parse_arrival(r["arrival_time_utc"]) < end
    ]
    assert len(in_window) >= known["injected"]["bad_window"] * 0.9


def test_write_output_and_reload(run_a, tmp_path):
    records, known = run_a
    write_output(records, known, tmp_path)
    lines = (tmp_path / "events.ndjson").read_text().splitlines()
    assert len(lines) == len(records)
    assert json.loads(lines[0]) == records[0]
    saved = json.loads((tmp_path / "known_answer.json").read_text())
    assert saved["expected"] == known["expected"]
    assert "current_state" not in saved  # large; stored as CSV
    with open(tmp_path / "expected_current_state.csv", newline="") as f:
        assert len(list(csv.DictReader(f))) == known["expected"]["trades"]
