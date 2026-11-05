"""Bronze: keep every raw line, bad or not. Add ingest_time and source. Parse nothing else."""

from pathlib import Path

from pyspark.sql import functions as F

ARRIVAL_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
KAFKA_COLUMNS = ("kafka_time", "kafka_partition", "kafka_offset")  # only the Kafka source has them


def read_events(spark, path):
    """The 'read events' step. One column, `value`: the raw JSON text.
    On Azure this step reads Event Hubs (Kafka) instead. Everything after it is the same."""
    return spark.read.text(path)


def read_events_stream(spark, folder, max_files=None):
    """The streaming 'read events' step for a folder of NDJSON files. It stands in for Event Hubs on a laptop.
    One column, `value`. `max_files` limits the files in one micro-batch. Kafka gets its own version on Azure.
    A file stream needs a folder. If `folder` is one file, we read its parent folder and keep only that file name."""
    reader = spark.readStream
    if max_files:
        reader = reader.option("maxFilesPerTrigger", max_files)
    if Path(folder).is_file():
        reader = reader.option("pathGlobFilter", Path(folder).name)
        folder = Path(folder).parent
    return reader.text(str(folder))


def stream_bronze(raw_stream, bronze_path, checkpoint_path, source="synthetic"):
    """Add only the new events to the Bronze Delta table, then stop (`availableNow`). Not a continuous stream.
    The checkpoint folder remembers what was read. Lose it, and the next run reads everything again.
    So a new checkpoint always goes with an empty Bronze table."""
    query = (
        to_bronze(raw_stream, source)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_path))
        .trigger(availableNow=True)
        .start(str(bronze_path))
    )
    query.awaitTermination()
    return query


def to_bronze(raw, source="synthetic"):
    """ingest_time = `arrival_time_utc` from the JSON. The generator puts its late and out-of-order faults there.
    If the JSON has none, ingest_time = `kafka_time` (the time Event Hubs got the event), when that column exists.
    `kafka_time` stays as a column. On Azure it is the send time, so it cannot replace the JSON time.
    bronze_id = xxhash64 of `value`. A rebuild from the archive gives the same ids."""
    ingest_time = F.try_to_timestamp(F.get_json_object("value", "$.arrival_time_utc"), F.lit(ARRIVAL_FORMAT))
    cols = ["value", *[c for c in KAFKA_COLUMNS if c in raw.columns]]  # where the message came from, if known
    if "kafka_time" in raw.columns:
        ingest_time = F.coalesce(ingest_time, F.col("kafka_time"))
    return raw.select(
        *cols,
        ingest_time.alias("ingest_time"),
        F.lit(source).alias("source"),
        # A hash of the raw text, so the id is the same on every run, in every batch and on every source.
        # Two identical lines share an id. That is harmless: Silver treats the second one as a duplicate.
        F.xxhash64("value").alias("bronze_id"),
    )
