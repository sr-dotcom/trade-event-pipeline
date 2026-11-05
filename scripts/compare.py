"""Compare the pipeline tables with the generator's known answer. Exit code 1 on any difference.

The known answer is a file the generator wrote before the pipeline ran. The script reads that file and the Delta tables.
It does not use any count that the pipeline made about itself.

Run: python -m scripts.compare --known data/generated/A --lake data/lake
"""

import argparse
import csv
import json
import sys
from pathlib import Path


def load_known(known_dir):
    """The summary and the expected current state: trade_id -> (trade_version, action, quantity)."""
    known = json.loads((Path(known_dir) / "known_answer.json").read_text(encoding="utf-8"))
    with open(Path(known_dir) / "expected_current_state.csv", encoding="utf-8", newline="") as f:
        trades = {r["trade_id"]: (int(r["trade_version"]), r["action"], int(r["quantity"])) for r in csv.DictReader(f)}
    return known, trades


def observe(spark, lake):
    """What the pipeline tables hold now."""
    read = lambda name: spark.read.format("delta").load(str(Path(lake) / name))
    state = read("current_state")
    return {
        "events_in": read("bronze").count(),
        "silver": read("silver").count(),
        "quarantine": read("quarantine").count(),
        "quarantine_by_reason": {r["reason"]: r["n"] for r in read("quarantine").groupBy("reason").count().withColumnRenamed("count", "n").collect()},
        "duplicates": read("duplicates").count(),
        "late_flagged": read("silver").filter("is_late").count(),
        "trades": state.count(),
        "active_trades": state.filter("is_active").count(),
        "state": {r["trade_id"]: (r["trade_version"], r["action"], r["quantity"]) for r in state.collect()},
    }


def compare(known, trades, seen):
    """Return (check, expected, observed, ok) rows."""
    exp = known["expected"]
    wrong = [t for t in trades.keys() | seen["state"].keys() if trades.get(t) != seen["state"].get(t)]
    parts = seen["silver"] + seen["quarantine"] + seen["duplicates"]
    rows = [
        ("events_in", known["n_events"], seen["events_in"]),
        ("silver", exp["good_unique_events"], seen["silver"]),
        ("quarantine", exp["quarantine"], seen["quarantine"]),
        ("quarantine_by_reason", exp["quarantine_by_reason"], seen["quarantine_by_reason"]),
        ("duplicates", exp["duplicates"], seen["duplicates"]),
        ("reconciliation", known["n_events"], parts),  # every event in the known answer ends in exactly one table
        ("late_flagged", exp["late_flagged"], seen["late_flagged"]),
        ("trades", exp["trades"], seen["trades"]),
        ("active_trades", exp["active_trades"], seen["active_trades"]),
        ("current_state_mismatches", 0, len(wrong)),
    ]
    return [(name, expected, observed, expected == observed) for name, expected, observed in rows]


def main(argv=None, spark=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--known", default="data/generated/A", help="folder with known_answer.json")
    p.add_argument("--lake", default="data/lake", help="folder with the Delta tables")
    a = p.parse_args(argv)
    own_session = spark is None
    if own_session:
        from src.common.spark import get_spark

        spark = get_spark("compare")
        spark.sparkContext.setLogLevel("ERROR")
    known, trades = load_known(a.known)
    rows = compare(known, trades, observe(spark, a.lake))
    for name, expected, observed, ok in rows:
        print(f"{'PASS' if ok else 'FAIL'}  {name:26} expected {expected}  observed {observed}")
    if own_session:
        spark.stop()
    return 0 if all(ok for *_, ok in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
