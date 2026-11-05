"""Helpers for the Azure Stage 2 probe. They run on a laptop with fake rows. The real answers come from Azure.

Open question for Stage 2: does the Capture `Offset` equal the Kafka offset, or does `SequenceNumber`?
`compare_offsets` answers it from two lists of rows. See docs/AZURE_PLAN.md (check P4, internal).
"""

from src.pipeline.kafka_source import kafka_options_service_credential
from src.pipeline.probe import compare_offsets


def kafka(n):
    return [{"value": f"e{i}", "kafka_partition": 0, "kafka_offset": i} for i in range(n)]


def test_it_reports_that_sequence_number_equals_the_kafka_offset():
    capture = [{"Body": f"e{i}", "SequenceNumber": i, "Offset": str(4096 * (i + 1))} for i in range(5)]
    assert compare_offsets(kafka(5), capture) == {
        "matched": 5, "kafka_only": 0, "capture_only": 0,
        "sequence_number_equals_kafka_offset": True, "offset_equals_kafka_offset": False,
    }


def test_it_reports_that_offset_equals_the_kafka_offset():
    capture = [{"Body": f"e{i}", "SequenceNumber": 100 + i, "Offset": str(i)} for i in range(5)]
    result = compare_offsets(kafka(5), capture)
    assert result["offset_equals_kafka_offset"] and not result["sequence_number_equals_kafka_offset"]


def test_it_counts_events_that_are_in_only_one_source_and_says_false_when_nothing_matches():
    capture = [{"Body": "x", "SequenceNumber": 0, "Offset": "0"}]
    result = compare_offsets(kafka(3), capture)
    assert (result["matched"], result["kafka_only"], result["capture_only"]) == (0, 3, 1)
    assert not result["sequence_number_equals_kafka_offset"] and not result["offset_equals_kafka_offset"]


def test_a_non_numeric_offset_never_counts_as_equal():
    capture = [{"Body": "e0", "SequenceNumber": 0, "Offset": "abc"}]
    assert compare_offsets(kafka(1), capture)["offset_equals_kafka_offset"] is False


def test_service_credential_options_have_no_password_and_no_sasl_settings():
    options = kafka_options_service_credential("fake-ns", "trade-events", "my-credential")
    assert options["databricks.serviceCredential"] == "my-credential"
    assert options["kafka.bootstrap.servers"] == "fake-ns.servicebus.windows.net:9093"
    assert not [k for k in options if k.startswith("kafka.sasl") or k == "kafka.security.protocol"]  # the docs forbid these
    assert kafka_options_service_credential("n", "t", "c", port=9092)["kafka.bootstrap.servers"] == "n.servicebus.windows.net:9092"
