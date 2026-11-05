"""Reference data: the IDs a valid event may use."""

import csv
from pathlib import Path

REF_DIR = Path(__file__).resolve().parents[2] / "data" / "reference"
FILES = {
    "instrument_id": "instruments.csv",
    "counterparty_id": "counterparties.csv",
    "book_id": "books.csv",
    "currency": "currencies.csv",
}


def load_reference(ref_dir=None):
    """Return {field name: sorted list of allowed values}."""
    out = {}
    for field, name in FILES.items():
        with open(Path(ref_dir or REF_DIR) / name, newline="") as f:
            out[field] = sorted(next(iter(row.values())) for row in csv.DictReader(f))
    return out
