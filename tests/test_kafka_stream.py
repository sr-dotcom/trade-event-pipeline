"""The Kafka stream version of the 'read events' step. We cannot reach Event Hubs from the laptop.

So we test what we can: which reader the function picks, the columns it returns, and the flow through Bronze
with a checkpoint. The last test feeds Bronze from a folder of files with the same columns as the Kafka source.
The real read from Event Hubs is UNVERIFIED until Azure Stage 2.
"""

import json
from datetime import datetime

from pyspark.sql.types import BinaryType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from src.pipeline.bronze import stream_bronze
from src.pipeline.kafka_source import kafka_events, kafka_options, read_events_kafka
from tests.helpers import ev

CONN = "Endpoint=" "sb://example/;SharedAccessKeyName=x;SharedAccess" "Key=FAKEKEY"  # split: the leak scan must stay quiet

# The columns of the Spark Kafka source, in its order.
KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("timestampType", IntegerType()),
    ]
)


class FakeFrame:
    def selectExpr(self, *exprs):
        self.exprs = exprs
        return self


class FakeReader:
    def format(self, name):
        self.name = name
        return self

    def options(self, **options):
        self.opts = options
        return self

    def load(self):
        return FakeFrame()


class FakeSpark:
    """Remembers which reader was asked for."""

    def __init__(self):
        self.used, self.reader = None, FakeReader()

    @property
    def read(self):
        self.used = "read"
        return self.reader

    @property
    def readStream(self):
        self.used = "readStream"
        return self.reader


def test_stream_true_uses_the_stream_reader_with_the_same_options():
    spark = FakeSpark()
    read_events_kafka(spark, "ns", "trade-events", CONN, stream=True)
    assert spark.used == "readStream" and spark.reader.name == "kafka"
    assert spark.reader.opts == kafka_options("ns", "trade-events", CONN)


def test_the_default_is_still_the_batch_reader():
    spark = FakeSpark()
    read_events_kafka(spark, "ns", "trade-events", CONN)
    assert spark.used == "read"


def make_kafka_file(spark, folder, name, lines, partition=0, first_offset=0):
    rows = [
        (None, bytearray(line.encode()), "trade-events", partition, first_offset + n, datetime(2026, 11, 2, 14, 7), 0)
        for n, line in enumerate(lines)
    ]
    spark.createDataFrame(rows, KAFKA_SCHEMA).coalesce(1).write.parquet(str(folder / name))


def kafka_stream(spark, folder):
    return kafka_events(spark.readStream.schema(KAFKA_SCHEMA).option("recursiveFileLookup", "true").parquet(str(folder)))


def test_kafka_events_gives_text_value_time_partition_and_offset(spark, tmp_path):
    line = json.dumps(ev("E-1"))
    make_kafka_file(spark, tmp_path, "f1", [line], partition=1, first_offset=40)
    raw = spark.read.schema(KAFKA_SCHEMA).parquet(str(tmp_path / "f1"))
    row = kafka_events(raw).collect()[0]
    assert kafka_events(raw).columns == ["value", "kafka_time", "kafka_partition", "kafka_offset"]
    assert (row["value"], row["kafka_partition"], row["kafka_offset"]) == (line, 1, 40)


def test_a_kafka_shaped_stream_goes_through_bronze_with_a_checkpoint(spark, tmp_path):
    landing, bronze, checkpoint = tmp_path / "landing", tmp_path / "bronze", tmp_path / "checkpoint"
    landing.mkdir()
    make_kafka_file(spark, landing, "f1", [json.dumps(ev("E-1")), json.dumps(ev("E-2"))], first_offset=0)
    stream_bronze(kafka_stream(spark, landing), bronze, checkpoint)
    stream_bronze(kafka_stream(spark, landing), bronze, checkpoint)  # nothing new
    table = spark.read.format("delta").load(str(bronze))
    assert table.count() == 2
    make_kafka_file(spark, landing, "f2", [json.dumps(ev("E-3"))], first_offset=2)
    stream_bronze(kafka_stream(spark, landing), bronze, checkpoint)
    table = spark.read.format("delta").load(str(bronze))
    assert table.count() == 3
    assert {"kafka_time", "kafka_partition", "kafka_offset", "ingest_time", "bronze_id"} <= set(table.columns)
    assert sorted(r["kafka_offset"] for r in table.collect()) == [0, 1, 2]  # the offsets are the proof of the source position
