"""The Kafka version of the 'read events' step. Reads Event Hubs through its Kafka endpoint (port 9093)."""

LOGIN_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"


def kafka_options(namespace, topic, connection_string, shaded=True, max_offsets_per_trigger=None):
    """Spark Kafka options for Event Hubs. The connection string comes from the caller. It is never stored here.
    On Databricks the Kafka classes carry a `kafkashaded.` prefix. This works on serverless (Azure probe, docs/azure-probe-results.md)."""
    module = f"kafkashaded.{LOGIN_MODULE}" if shaded else LOGIN_MODULE
    # A secret saved through a PowerShell pipe ends with a line break. Inside the login string it makes the setting invalid.
    jaas = f'{module} required username="$ConnectionString" password="{connection_string.strip()}";'
    return {
        "kafka.bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
        "subscribe": topic,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
        **({"maxOffsetsPerTrigger": str(max_offsets_per_trigger)} if max_offsets_per_trigger else {}),
    }


def kafka_options_service_credential(namespace, topic, credential_name, port=9093):
    """The fallback login for serverless: a Unity Catalog service credential. No password, no `kafka.sasl.*` options.
    The Databricks page forbids those options with a service credential. Its example uses port 9092, Event Hubs uses 9093.
    Which port works is UNVERIFIED, so the probe tries both."""
    return {
        "kafka.bootstrap.servers": f"{namespace}.servicebus.windows.net:{port}",
        "subscribe": topic,
        "databricks.serviceCredential": credential_name,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }


def kafka_events(raw):
    """Turn the columns of the Spark Kafka source into the events columns.
    `value` is the raw JSON text, as in `read_events`. `kafka_time` is the time Event Hubs got the event.
    `kafka_partition` and `kafka_offset` say where the message sits. Stage 2 compares them with the Capture archive."""
    return raw.selectExpr(
        "CAST(value AS STRING) AS value",
        "timestamp AS kafka_time",
        "partition AS kafka_partition",
        "offset AS kafka_offset",
    )


def read_events_kafka(spark, namespace, topic, connection_string, shaded=True, stream=False, max_offsets_per_trigger=None):
    """The Kafka version of `read_events`. With `stream=True` it returns a streaming DataFrame for `stream_bronze`.
    `max_offsets_per_trigger` limits the events in one micro-batch. A batch read ignores it."""
    reader = spark.readStream if stream else spark.read
    options = kafka_options(namespace, topic, connection_string, shaded, max_offsets_per_trigger)
    raw = reader.format("kafka").options(**options).load()
    return kafka_events(raw)
