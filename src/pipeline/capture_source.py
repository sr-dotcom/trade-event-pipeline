"""The Capture version of the 'read events' step. Event Hubs Capture copies every event to ADLS as Avro files.

Same output as `read_events`: column `value`, the raw JSON text. Everything after it is the same.
The archive has no `kafka_time`. `ingest_time` comes from the JSON, so Bronze gets the same `ingest_time` and `bronze_id`.
Record layout (Azure docs, UNVERIFIED until a real file is read): SequenceNumber, Offset, EnqueuedTimeUtc,
SystemProperties, Properties, Body. Folder layout: {namespace}/{hub}/{partition}/{yyyy}/{MM}/{dd}/{HH}/{mm}/{ss}.
"""


def read_events_capture(spark, path):
    """Read all Avro files under `path`, in any folder depth. Databricks has the `avro` format built in."""
    raw = spark.read.format("avro").option("recursiveFileLookup", "true").load(str(path))
    return raw.selectExpr("CAST(Body AS STRING) AS value")
