"""Synthetic trade-event generator with a known answer. Fake data only.

Steps:
1. Build clean trades (NEW, then AMEND, then CANCEL). These are the "good events".
2. Delay some of them (out of order, late, very late, outage).
3. Add extra records on top: duplicates, bad records, conflicts.
   The good events stay untouched, so the known answer is exact.

Run: python -m src.generator.generate --profile A --n 50000 --seed 42
"""

import argparse
import csv
import json
import random
import tomllib
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
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
TIME_FMT = "%Y-%m-%dT%H:%M:%SZ"


def load_config(profile="A", path=None):
    with open(path or ROOT / "config" / "faults.toml", "rb") as f:
        raw = tomllib.load(f)
    return {**raw["run"], **raw["mix"], **raw[f"profile_{profile.lower()}"]}


def _read_ids(name):
    with open(ROOT / "data" / "reference" / name, newline="") as f:
        return list(csv.DictReader(f))


def _build_trades(rng, cfg, base_n, instruments, cps, books):
    """Return base_n clean events (without event_id), sorted by event time. Times are seconds from start."""
    events, trade_no = [], 0
    gap_lo, gap_hi = cfg["version_gap_seconds"]
    while len(events) < base_n:
        trade_no += 1
        inst = rng.choice(instruments)
        base_price = float(inst["base_price"])
        common = {
            "trade_id": f"T-{trade_no:06d}",
            "instrument_id": inst["instrument_id"],
            "counterparty_id": rng.choice(cps)["counterparty_id"],
            "book_id": rng.choice(books)["book_id"],
            "side": rng.choice(["BUY", "SELL"]),
            "currency": inst["currency"],
        }
        qty, price = rng.randint(1, 100) * 10, max(0.01, round(base_price * (1 + rng.gauss(0, 0.01)), 2))
        t = rng.uniform(0, cfg["duration_minutes"] * 60)
        chain = [("NEW", t, qty, price)]
        n_amend = 0
        if rng.random() < cfg["amend_trade_rate"]:
            n_amend = 2 if rng.random() < cfg["second_amend_rate"] else 1
        for _ in range(n_amend):
            t += rng.uniform(gap_lo, gap_hi)
            qty, price = rng.randint(1, 100) * 10, max(0.01, round(price * (1 + rng.gauss(0, 0.005)), 2))
            chain.append(("AMEND", t, qty, price))
        if rng.random() < cfg["cancel_rate"]:
            chain.append(("CANCEL", t + rng.uniform(gap_lo, gap_hi), qty, price))
        for version, (action, when, q, p) in enumerate(chain[: base_n - len(events)], start=1):
            events.append(
                {**common, "trade_version": version, "action": action, "quantity": q, "price": p, "_t": int(when)}
            )
    events.sort(key=lambda e: (e["_t"], e["trade_id"], e["trade_version"]))
    return events


def _corrupt(rng, rec, kind):
    r = dict(rec)
    if kind == "missing_field":
        field = rng.choice(FIELDS)
        if rng.random() < 0.5:
            del r[field]
        else:
            r[field] = None
    elif kind == "bad_value":
        field, value = rng.choice(
            [
                ("quantity", 0),
                ("quantity", -5),
                ("quantity", "abc"),
                ("price", 0),
                ("price", -1.5),
                ("side", "HOLD"),
                ("action", "MODIFY"),
                ("currency", "XXX"),
                ("event_time_utc", "not-a-time"),
            ]
        )
        r[field] = value
    else:
        r[rng.choice(["instrument_id", "counterparty_id", "book_id"])] = f"UNKNOWN-{rng.randint(1, 999)}"
    return r


def generate(n_events=50_000, seed=42, profile="A", config_path=None):
    """Return (records, known). Records are in arrival order. known is the answer key."""
    rng = random.Random(seed)
    cfg = load_config(profile, config_path)
    n, dur = n_events, cfg["duration_minutes"] * 60
    start = datetime.strptime(cfg["start_utc"], TIME_FMT).replace(tzinfo=UTC)
    instruments, cps, books = (_read_ids(f) for f in ("instruments.csv", "counterparties.csv", "books.csv"))

    n_dup, n_ooo, n_late, n_vlate = (
        round(cfg[k] * n) for k in ("duplicate_rate", "out_of_order_rate", "late_rate", "very_late_rate")
    )
    n_bad = round(cfg["bad_rate"] * n)
    win_sec = cfg["bad_window_minutes"] * 60
    n_win_bad = round(round(n * win_sec / dur) * cfg["bad_window_rate"])
    n_conf, burst = cfg["conflict_count"], cfg["duplicate_burst_size"]
    base_n = n - (n_dup + burst + n_bad + n_win_bad + n_conf)

    # 1. good events
    base = _build_trades(rng, cfg, base_n, instruments, cps, books)
    for i, e in enumerate(base, start=1):
        e["event_id"] = f"E-{i:07d}"
    ev_str = lambda sec: (start + timedelta(seconds=sec)).strftime(TIME_FMT)
    for e in base:
        e["event_time_utc"] = ev_str(e["_t"])
        e["_arr"] = int(1000 * (e["_t"] + rng.uniform(0.05, 2.0)))  # arrival, ms from start

    # 2. outage: events in the window arrive after it ends
    out_start = int(rng.uniform(0.2 * dur, 0.8 * dur - cfg["outage_minutes"] * 60))
    out_end = out_start + cfg["outage_minutes"] * 60
    outage = [e for e in base if out_start <= e["_t"] < out_end]
    for e in outage:
        e["_arr"] = int(1000 * (out_end + rng.uniform(0, 30)))

    # 3. out of order, late, very late (disjoint sets, none in the outage)
    pool = [i for i, e in enumerate(base) if not out_start <= e["_t"] < out_end]
    chosen = rng.sample(pool, n_ooo + n_late + n_vlate)
    for idxs, key in (
        (chosen[:n_ooo], "out_of_order_seconds"),
        (chosen[n_ooo : n_ooo + n_late], "late_seconds"),
        (chosen[n_ooo + n_late :], "very_late_seconds"),
    ):
        lo, hi = cfg[key]
        for i in idxs:
            base[i]["_arr"] += int(1000 * rng.uniform(lo, hi))

    # entries: (arrival ms, is_good, record). Extra records are added below.
    entries = [(e["_arr"], True, {**{k: e[k] for k in FIELDS}}) for e in base]
    arr_of = [e["_arr"] for e in base]

    # 4. duplicates: random copies, plus one burst of the last events before a "reconnect"
    for i in rng.sample(range(base_n), n_dup):
        entries.append((arr_of[i] + int(1000 * rng.uniform(0.1, 60)), False, dict(entries[i][2])))
    order = sorted(range(base_n), key=lambda i: arr_of[i])
    pos = int(0.6 * base_n)
    burst = min(burst, pos)
    t_burst = arr_of[order[pos - 1]] + 1000
    for k, i in enumerate(order[pos - burst : pos]):
        entries.append((t_burst + 2 * k, False, dict(entries[i][2])))

    # 5. bad records: corrupted copies with a new event_id, random and inside one window
    kinds = list(cfg["bad_mix"])
    weights = list(cfg["bad_mix"].values())
    win_start = int(rng.uniform(0.1 * dur, 0.9 * dur - win_sec))
    bad_kinds = defaultdict(int)
    for k in range(n_bad + n_win_bad):
        kind = rng.choices(kinds, weights)[0]
        bad_kinds[kind] += 1
        rec = _corrupt(rng, {**entries[rng.randrange(base_n)][2], "event_id": f"E-B{k:06d}"}, kind)
        if k < n_bad:
            arr = arr_of[rng.randrange(base_n)] + int(1000 * rng.uniform(0.05, 1))
        else:  # window: event time and arrival both inside the window
            sec = win_start + int(rng.uniform(0, win_sec - 3))
            if rec.get("event_time_utc") not in (None, "not-a-time"):
                rec["event_time_utc"] = ev_str(sec)
            arr = int(1000 * (sec + rng.uniform(0.05, 1)))
        entries.append((arr, False, rec))

    # 6. conflicts: same event_id, different content, arrives after the original
    for i in rng.sample(range(base_n), n_conf):
        rec = dict(entries[i][2])
        rec["quantity"] += 10
        entries.append((arr_of[i] + int(1000 * rng.uniform(1, 120)), False, rec))

    # arrival order (stable: ties keep insertion order, so the original comes first)
    ordered = sorted(enumerate(entries), key=lambda x: (x[1][0], x[0]))
    records, good_in_order = [], []
    for _, (arr, is_good, rec) in ordered:
        at = start + timedelta(milliseconds=arr)
        rec = {**rec, "arrival_time_utc": f"{at:%Y-%m-%dT%H:%M:%S}.{at.microsecond // 1000:03d}Z"}
        records.append(rec)
        if is_good:
            good_in_order.append(rec)

    # known answer, from the good events only
    late_ms = cfg["late_after_seconds"] * 1000
    late_flagged = sum(e["_arr"] - 1000 * e["_t"] > late_ms for e in base)
    chains, current = defaultdict(list), {}
    for rec in good_in_order:
        chains[rec["trade_id"]].append(rec["trade_version"])
        if rec["trade_id"] not in current or rec["trade_version"] > current[rec["trade_id"]]["trade_version"]:
            current[rec["trade_id"]] = {k: rec[k] for k in FIELDS}
    cancelled = sum(r["action"] == "CANCEL" for r in current.values())
    known = {
        "seed": seed,
        "profile": profile.upper(),
        "n_events": n,
        "injected": {
            "duplicate_random": n_dup,
            "duplicate_burst": burst,
            "out_of_order": n_ooo,
            "late": n_late,
            "very_late": n_vlate,
            "bad_random": n_bad,
            "bad_window": n_win_bad,
            "conflict": n_conf,
            "outage_events": len(outage),
        },
        "expected": {
            "good_unique_events": base_n,
            "duplicates": n_dup + burst,
            "quarantine": n_bad + n_win_bad + n_conf,
            "quarantine_by_reason": {**{k: bad_kinds[k] for k in kinds}, "event_id_conflict": n_conf},
            "late_flagged": late_flagged,
            "trades": len(current),
            "cancelled_trades": cancelled,
            "active_trades": len(current) - cancelled,
            "version_inversion_trades": sum(v != sorted(v) for v in chains.values()),
        },
        "outage": {"start_utc": ev_str(out_start), "end_utc": ev_str(out_end)},
        "bad_window": {"start_utc": ev_str(win_start), "end_utc": ev_str(win_start + win_sec)},
        "current_state": current,
    }
    return records, known


def write_output(records, known, out_dir):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "events.ndjson", "w", encoding="utf-8", newline="\n") as f:
        f.writelines(json.dumps(rec) + "\n" for rec in records)
    summary = {k: v for k, v in known.items() if k != "current_state"}
    (out / "known_answer.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    with open(out / "expected_current_state.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(known["current_state"].values())


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--profile", default="A", choices=["A", "B"])
    p.add_argument("--n", type=int, default=50_000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default=None, help="default: data/generated/<profile>")
    a = p.parse_args()
    records, known = generate(a.n, a.seed, a.profile)
    out = a.out or ROOT / "data" / "generated" / a.profile
    write_output(records, known, out)
    print(f"Wrote {len(records)} events to {out}")
    for section in ("injected", "expected"):
        print(f"\n{section}:")
        for k, v in known[section].items():
            print(f"  {k:28} {v}")


if __name__ == "__main__":
    main()
