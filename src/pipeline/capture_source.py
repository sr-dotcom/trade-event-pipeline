"""The Capture version of the 'read events' step. Event Hubs Capture copies every event to ADLS as Avro files.

Same output as `read_events`: column `value`, the raw JSON text. Everything after it is the same.
The archive has no `kafka_time`. `ingest_time` comes from the JSON, so Bronze gets the same `ingest_time` and `bronze_id`.
Record layout, read from one real Capture file (docs/azure-probe-results.md): SequenceNumber long, Offset string,
EnqueuedTimeUtc string, SystemProperties map, Properties map, Body bytes (may be null).
Folder layout: {namespace}/{hub}/{partition}/{yyyy}/{MM}/{dd}/{HH}/{mm}/{ss}.avro. The hour is UTC.
On 100 real events, SequenceNumber equals the Kafka offset and Offset does not.
"""

from pyspark.sql.types import BinaryType, LongType, StringType, StructField, StructType

# The streaming file source needs a schema. We read only these four fields. Avro skips the rest.
CAPTURE_STREAM_SCHEMA = StructType(
    [
        StructField("SequenceNumber", LongType()),
        StructField("Offset", StringType()),
        StructField("EnqueuedTimeUtc", StringType()),
        StructField("Body", BinaryType()),
    ]
)


def read_events_capture(spark, path):
    """Read all Avro files under `path`, in any folder depth. Databricks has the `avro` format built in."""
    raw = spark.read.format("avro").option("recursiveFileLookup", "true").load(str(path))
    return raw.selectExpr("CAST(Body AS STRING) AS value")


def read_events_capture_stream(spark, path, max_files=None):
    """The streaming version, for `stream_bronze`. A checkpoint remembers the files, so a run reads only new ones.
    `max_files` limits the files in one micro-batch (Capture has files, not offsets). Same output: column `value`.
    UNVERIFIED on a real archive: tested on a local fixture only."""
    reader = spark.readStream.format("avro").schema(CAPTURE_STREAM_SCHEMA)
    reader = reader.option("recursiveFileLookup", "true").option("pathGlobFilter", "*.avro")
    if max_files:
        reader = reader.option("maxFilesPerTrigger", max_files)
    return reader.load(str(path)).selectExpr("CAST(Body AS STRING) AS value")
