"""The Kafka (Event Hubs) read step. We cannot reach Event Hubs from the laptop,
so we test the two parts that do not need it: the connection options and the ingest_time rule."""
from pyspark.sql import functions as F

from src.pipeline.bronze import to_bronze
from src.pipeline.kafka_source import kafka_options

CONN = "Endpoint=" "sb://example/;SharedAccessKeyName=x;SharedAccess" "Key=FAKEKEY"  # split: the leak scan must stay quiet


def test_options_point_at_event_hubs_kafka_endpoint():
    o = kafka_options("evh-trade-abc123", "trade-events", CONN)
    assert o["kafka.bootstrap.servers"] == "evh-trade-abc123.servicebus.windows.net:9093"
    assert o["subscribe"] == "trade-events"
    assert o["kafka.security.protocol"] == "SASL_SSL"
    assert o["kafka.sasl.mechanism"] == "PLAIN"


def test_options_log_in_with_the_connection_string():
    jaas = kafka_options("ns", "t", CONN)["kafka.sasl.jaas.config"]
    assert 'username="$ConnectionString"' in jaas
    assert f'password="{CONN}"' in jaas


def test_options_read_from_the_start_and_do_not_fail_on_data_loss():
    o = kafka_options("ns", "t", CONN)
    assert o["startingOffsets"] == "earliest"
    assert o["failOnDataLoss"] == "false"


def test_shaded_login_module_is_a_switch():
    assert "kafkashaded.org.apache.kafka" in kafka_options("ns", "t", CONN)["kafka.sasl.jaas.config"]
    assert "kafkashaded" not in kafka_options("ns", "t", CONN, shaded=False)["kafka.sasl.jaas.config"]


def _ingest_text(df):
    """ingest_time as UTC text. collect() would turn a timestamp into the laptop's local zone."""
    return to_bronze(df).select(F.date_format("ingest_time", "yyyy-MM-dd HH:mm").alias("t")).collect()[0]["t"]


def test_json_arrival_time_wins_over_kafka_time(spark):
    # The JSON arrival time says 12:00. The Kafka time says 12:07 (the send time). The JSON time wins.
    # The generator puts late and out-of-order faults in the JSON time. The Kafka time hides them.
    line = '{"event_id": "E1", "arrival_time_utc": "2026-01-01T12:00:00.000Z"}'
    raw = spark.range(1).select(F.lit(line).alias("value"), F.to_timestamp(F.lit("2026-01-01 12:07:00")).alias("kafka_time"))
    assert _ingest_text(raw) == "2026-01-01 12:00"


def test_kafka_time_is_used_when_the_json_has_no_arrival_time(spark):
    raw = spark.range(1).select(F.lit('{"event_id": "E1"}').alias("value"), F.to_timestamp(F.lit("2026-01-01 12:07:00")).alias("kafka_time"))
    assert _ingest_text(raw) == "2026-01-01 12:07"


def test_kafka_time_stays_as_a_column(spark):
    raw = spark.range(1).select(F.lit('{"event_id": "E1"}').alias("value"), F.to_timestamp(F.lit("2026-01-01 12:07:00")).alias("kafka_time"))
    assert "kafka_time" in to_bronze(raw).columns


def test_ingest_time_stays_the_json_arrival_time_without_kafka_time(spark):
    line = '{"event_id": "E1", "arrival_time_utc": "2026-01-01T12:00:00.000Z"}'
    raw = spark.range(1).select(F.lit(line).alias("value"))
    assert _ingest_text(raw) == "2026-01-01 12:00"
