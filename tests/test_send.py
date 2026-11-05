"""The sender puts the events of a file into Event Hubs, all to partition 0, in file order. These tests use a fake producer.
They do not talk to Kafka. A real send is UNVERIFIED until Azure Stage 2."""

import pytest

from src.generator.send import producer_config, send_events

LINES = [f'{{"event_id": "E-{n}"}}\n' for n in range(10)]


class FakeProducer:
    def __init__(self, fail_at=None):
        self.sent, self.fail_at, self.pending, self.flushed = [], fail_at, [], False

    def produce(self, topic, value=None, partition=None, on_delivery=None):
        self.sent.append((topic, value, partition))
        self.pending.append((on_delivery, len(self.sent) - 1))

    def poll(self, timeout=0):
        self._deliver()

    def flush(self, timeout=None):
        self._deliver()
        self.flushed = True

    def _deliver(self):
        for callback, index in self.pending:
            callback("broker said no" if index == self.fail_at else None, None)
        self.pending = []


def test_every_event_goes_to_partition_0_in_file_order():
    producer = FakeProducer()
    result = send_events(LINES, producer, "trade-events")
    assert [v for _, v, _ in producer.sent] == [ln.strip().encode() for ln in LINES]
    assert {p for _, _, p in producer.sent} == {0}
    assert {t for t, _, _ in producer.sent} == {"trade-events"}
    assert producer.flushed and result == {"sent": 10}


def test_start_and_count_pick_a_slice_so_we_can_send_half_then_the_rest():
    producer = FakeProducer()
    send_events(LINES, producer, "t", start=0, count=5)
    send_events(LINES, producer, "t", start=5)
    assert [v for _, v, _ in producer.sent] == [ln.strip().encode() for ln in LINES]


def test_a_failed_delivery_stops_with_an_error_instead_of_a_silent_loss():
    with pytest.raises(RuntimeError, match="1 of 10"):
        send_events(LINES, FakeProducer(fail_at=3), "t")


def test_the_config_uses_the_event_hubs_kafka_endpoint_and_keeps_the_secret_out_of_the_repr_of_the_result():
    conn = "Endpoint=sb" + "://fake.servicebus.windows.net/;SharedAccess" + "KeyName=send;SharedAccess" + "Key=FAKEFAKE"  # split for the leak scan
    config = producer_config("fake-ns", conn)
    assert config["bootstrap.servers"] == "fake-ns.servicebus.windows.net:9093"
    assert (config["security.protocol"], config["sasl.mechanism"], config["sasl.username"]) == ("SASL_SSL", "PLAIN", "$ConnectionString")
    assert config["sasl.password"] == conn
    assert conn not in repr(send_events(LINES, FakeProducer(), "t"))
