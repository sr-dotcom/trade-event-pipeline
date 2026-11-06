"""A connection string that carries a line break must still give a valid Kafka login setting.

The first real read (Azure Stage 2, check P3) failed with "Failed to create new KafkaAdminClient".
The secret had been saved through a PowerShell pipe, which adds a line break at the end.
See FAILURES.md ("A line break in the stored connection string broke the Kafka login").
"""

from src.pipeline.kafka_source import kafka_options

FAKE = "Endpoint=sb" + "://fake.servicebus.windows.net/;SharedAccess" + "KeyName=listen;SharedAccess" + "Key=FAKEFAKE"


def test_whitespace_around_the_connection_string_is_removed_from_the_login_setting():
    clean = kafka_options("fake-ns", "t", FAKE)["kafka.sasl.jaas.config"]
    for noisy in (FAKE + "\r\n", FAKE + "\n", "  " + FAKE + " "):
        assert kafka_options("fake-ns", "t", noisy)["kafka.sasl.jaas.config"] == clean
    assert "\n" not in clean and "\r" not in clean


def test_max_offsets_per_trigger_is_set_only_when_asked_for():
    """It limits the events in one micro-batch. Only a real Kafka source shows the effect (UNVERIFIED on Azure)."""
    assert "maxOffsetsPerTrigger" not in kafka_options("n", "t", FAKE)
    assert kafka_options("n", "t", FAKE, max_offsets_per_trigger=5000)["maxOffsetsPerTrigger"] == "5000"
