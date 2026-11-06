# Databricks notebook source
# ruff: noqa: F821, BLE001
# BLE001: a failed check is a result. The notebook records it and goes on.
# Azure Stage 2 probe. Four checks, one result line each. Run it on SERVERLESS compute, cell by cell.
# It ran on Azure on 2026-11-05 and P1 to P4 passed (docs/azure-probe-results.md). A failed check is a result. Log it in FAILURES.md. Do not retry in a loop.
# Before you run it:
#   1. Clone this repo into a Databricks Git folder. Put the folder path in `repo_path`.
#   2. Send 100 events from your laptop: python -m src.generator.send --events FILE --count 100
#   3. Wait one Capture window (60 seconds) after the send.
# The notebook prints no secret. Databricks hides secret values. We also cut the connection string out of error text.

# COMMAND ----------

dbutils.widgets.text("repo_path", "")  # for example /Workspace/Users/you/trade-event-pipeline
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container", "archive")  # the container the external location covers
dbutils.widgets.text("namespace", "")  # the Event Hubs namespace name, without .servicebus.windows.net
dbutils.widgets.text("topic", "trade-events")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("secret_key", "")  # holds the listen-only connection string
dbutils.widgets.text("service_credential", "")  # optional: the Unity Catalog service credential, for the fallback

# COMMAND ----------

import json
import sys

w = {
    name: dbutils.widgets.get(name)
    for name in (
        "repo_path",
        "storage_account",
        "container",
        "namespace",
        "topic",
        "secret_scope",
        "secret_key",
        "service_credential",
    )
}
sys.path.insert(0, w["repo_path"])
from src.generator.generate import generate
from src.pipeline.kafka_source import kafka_events, kafka_options, kafka_options_service_credential
from src.pipeline.probe import compare_offsets
from src.pipeline.run_local import run

BASE = f"abfss://{w['container']}" + "@" + f"{w['storage_account']}.dfs.core.windows.net"
ROOT = BASE + "/probe"
results, state = {}, {}


def check(name, fn):
    """Run one check. Record PASS or FAIL with a short reason. Never stop the notebook."""
    try:
        results[name] = "PASS" + (f" ({fn()})" if fn else "")
    except Exception as e:
        results[name] = f"FAIL {type(e).__name__}: {str(e)[:300]}"
    print(name, "->", results[name])


# COMMAND ----------

# P1: can serverless see the container through the external location?
check("P1 list the container", lambda: f"{len(dbutils.fs.ls(BASE))} entries")

# COMMAND ----------


# P2: write Bronze with a checkpoint to storage, run twice. The second run must read nothing new.
def p2():
    records, _known = generate(n_events=2000, seed=1, profile="A")
    text = "".join(json.dumps(r) + "\n" for r in records)
    dbutils.fs.put(f"{ROOT}/p2/landing/events.ndjson", text, True)
    first = run(spark, f"{ROOT}/p2/landing", f"{ROOT}/p2/lake")
    second = run(spark, f"{ROOT}/p2/landing", f"{ROOT}/p2/lake")
    assert first["events_in"] == 2000, first
    assert second == first, (first, second)
    return f"events_in={first['events_in']}, second run equal"


check("P2 Bronze checkpoint resumes on storage", p2)

# COMMAND ----------


# P3: read the 100 events from Event Hubs through Kafka. Try SASL/PLAIN first, then the service credential (ports 9093, 9092).
def kafka_batch(options):
    raw = spark.read.format("kafka").options(**options).load()
    rows = kafka_events(raw).collect()
    return [
        {"value": r["value"], "kafka_partition": r["kafka_partition"], "kafka_offset": r["kafka_offset"]} for r in rows
    ]


def p3():
    errors = []
    attempts = []
    if w["secret_scope"]:
        conn = dbutils.secrets.get(w["secret_scope"], w["secret_key"])
        attempts.append(("sasl_plain_shaded", kafka_options(w["namespace"], w["topic"], conn)))
    if w["service_credential"]:
        for port in (9093, 9092):
            attempts.append(
                (
                    f"service_credential_port_{port}",
                    kafka_options_service_credential(w["namespace"], w["topic"], w["service_credential"], port),
                )
            )
    for label, options in attempts:
        try:
            state["kafka_rows"] = kafka_batch(options)
            state["kafka_login"] = label
            return f"{label}: {len(state['kafka_rows'])} events"
        except Exception as e:
            msg = str(e)[:200]
            errors.append(f"{label}: {type(e).__name__} {msg}")
    raise RuntimeError(" | ".join(errors) or "no login configured")


check("P3 read Event Hubs through Kafka", p3)

# COMMAND ----------


# P4: read a real Capture file. Print its columns. Say which field equals the Kafka offset.
def p4():
    raw = spark.read.format("avro").option("recursiveFileLookup", "true").load(f"{BASE}/{w['namespace']}/{w['topic']}")
    print("Capture columns:", raw.columns)
    rows = raw.selectExpr("CAST(Body AS STRING) AS Body", "SequenceNumber", "Offset").collect()
    capture = [{"Body": r["Body"], "SequenceNumber": r["SequenceNumber"], "Offset": r["Offset"]} for r in rows]
    result = compare_offsets(state["kafka_rows"], capture)
    print(json.dumps(result))
    return f"{result['matched']} matched; SequenceNumber equals Kafka offset: {result['sequence_number_equals_kafka_offset']}; Offset equals: {result['offset_equals_kafka_offset']}"


check("P4 read Capture and compare offsets", p4)

# COMMAND ----------

print("\nRESULT SUMMARY")
for name, outcome in results.items():
    print(f"{name:42} {outcome}")
