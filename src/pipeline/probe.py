"""Helpers for the Azure Stage 2 probe (`notebooks/azure_probe.py`). Plain Python, so a laptop can test them."""


def _as_int(value):
    try:
        return int(str(value))
    except ValueError:
        return None  # a non-numeric value never counts as equal


def compare_offsets(kafka_rows, capture_rows):
    """Match Kafka rows and Capture rows by the event text. Say which Capture field equals the Kafka offset.

    kafka_rows: dicts with `value` and `kafka_offset`. capture_rows: dicts with `Body`, `SequenceNumber` and `Offset`.
    The probe sends unique events, so the text identifies an event."""
    capture = {row["Body"]: row for row in capture_rows}
    pairs = [(k, capture[k["value"]]) for k in kafka_rows if k["value"] in capture]

    def same(field):
        return bool(pairs) and all(_as_int(c[field]) == int(k["kafka_offset"]) for k, c in pairs)

    return {
        "matched": len(pairs),
        "kafka_only": len(kafka_rows) - len(pairs),
        "capture_only": len(capture) - len(pairs),
        "sequence_number_equals_kafka_offset": same("SequenceNumber"),
        "offset_equals_kafka_offset": same("Offset"),
    }
