"""Shared test helpers: hand-made events and a small Bronze + Silver run."""

import json

from src.common.reference import load_reference
from src.pipeline.bronze import read_events, to_bronze
from src.pipeline.silver import build_silver

REF = load_reference()
DROP = object()  # marks a field to leave out


def ev(event_id, arrival="2026-11-02T14:00:01.000Z", **changes):
    rec = {
        "event_id": event_id,
        "trade_id": "T-000001",
        "trade_version": 1,
        "action": "NEW",
        "instrument_id": "FAKE001",
        "counterparty_id": "CP-001",
        "book_id": "BOOK-01",
        "side": "BUY",
        "quantity": 100,
        "price": 10.5,
        "currency": "USD",
        "event_time_utc": "2026-11-02T14:00:00Z",
        "arrival_time_utc": arrival,
    }
    rec.update(changes)
    return {k: v for k, v in rec.items() if v is not DROP}


def run(spark, tmp_path, records):
    path = tmp_path / "events.ndjson"
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")
    bronze = to_bronze(read_events(spark, str(path)))
    return bronze, *build_silver(bronze, REF)
