# Databricks notebook source
# ruff: noqa: F821, BLE001
# BLE001: a failed check is a result. The notebook records it and goes on.
# The full Azure run. It ran on 2026-11-05 and R2 to R5 passed (docs/azure-run-results.md). Use SERVERLESS compute. Run the cells in order.
# Before you start, the repo folder in the workspace holds src/, config/, data/reference/, notebooks/ and known/A/
# (the two known answer files of the 50,000-event run). The hub holds NO events yet.
#
# The order of work:
#   1. Cell "setup".
#   2. On your laptop, send the FIRST half: python -m src.generator.send --events data/generated/A/events.ndjson --start 0 --count 25000
#   3. Cell "R1": read the first half into Bronze, build all tables.
#   4. Wait 5 minutes. This is the stop for the resume proof.
#   5. On your laptop, send the SECOND half: --start 25000 --count 25000
#   6. Cell "R2": read only the new events. Cells "R3" and "R4": the resume proof and the comparison with the known answer.
#   7. Wait 90 seconds. Cell "R5": build a second lake from the Capture archive and compare the two lakes.
# The notebook prints no secret.

# COMMAND ----------

dbutils.widgets.text("repo_path", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container", "archive")
dbutils.widgets.text("namespace", "")
dbutils.widgets.text("topic", "trade-events")
dbutils.widgets.text("secret_scope", "trade")
dbutils.widgets.text("secret_key", "eh-listen")
dbutils.widgets.text("max_offsets", "10000")  # events in one micro-batch. Use 0 to switch the limit off.

# COMMAND ----------

import json
import sys

w = {
    k: dbutils.widgets.get(k)
    for k in (
        "repo_path",
        "storage_account",
        "container",
        "namespace",
        "topic",
        "secret_scope",
        "secret_key",
        "max_offsets",
    )
}
sys.path.insert(0, w["repo_path"])
from pyspark.sql import functions as F

from scripts.compare import main as compare_main
from src.pipeline.capture_source import read_events_capture
from src.pipeline.kafka_source import read_events_kafka
from src.pipeline.replay import lake_fingerprints
from src.pipeline.run_local import run

BASE = f"abfss://{w['container']}" + "@" + f"{w['storage_account']}.dfs.core.windows.net"
LAKE, LAKE_CAPTURE = BASE + "/run/lake_kafka", BASE + "/run/lake_capture"
KNOWN = w["repo_path"] + "/known/A"
CONN = dbutils.secrets.get(w["secret_scope"], w["secret_key"]).strip()
MAX = int(w["max_offsets"]) or None
results = {}


def check(name, fn):
    """Run one step. Record PASS or FAIL with a short reason. Never stop the notebook. Secrets are cut out of error text."""
    try:
        results[name] = "PASS" + (f" ({fn()})" if fn else "")
    except Exception as e:
        results[name] = f"FAIL {type(e).__name__}: {str(e).replace(CONN, '<secret>')[:400]}"
    print(name, "->", results[name])


def kafka_stream():
    return read_events_kafka(spark, w["namespace"], w["topic"], CONN, stream=True, max_offsets_per_trigger=MAX)


def table(lake, name):
    return spark.read.format("delta").load(lake + "/" + name)


# COMMAND ----------


# R1: the first half. Bronze reads the new events from Kafka and saves its place in a checkpoint on storage.
def r1():
    counts = run(spark, kafka_stream(), LAKE)
    results["bronze_after_r1"] = counts["events_in"]
    return json.dumps(counts)


check("R1 first half into Bronze and all tables", r1)

# COMMAND ----------


# R2: the second half, after the pause. Bronze must read only the events that are new.
def r2():
    counts = run(spark, kafka_stream(), LAKE)
    return json.dumps(counts)


check("R2 second half, run again from the checkpoint", r2)

# COMMAND ----------


# R3: the resume proof. After two runs, every Kafka offset is in Bronze once. No double, no gap.
def r3():
    bronze = table(LAKE, "bronze")
    row = bronze.agg(
        F.count("*").alias("rows"),
        F.countDistinct("kafka_offset").alias("offsets"),
        F.min("kafka_offset").alias("lo"),
        F.max("kafka_offset").alias("hi"),
    ).first()
    ok = row["rows"] == row["offsets"] == row["hi"] - row["lo"] + 1
    assert ok, row.asDict()
    return f"{row['rows']} rows, offsets {row['lo']} to {row['hi']}, none twice, none missing; after R1 there were {results.get('bronze_after_r1')}"


check("R3 resume proof: no offset twice, none missing", r3)

# COMMAND ----------


# R4: compare the tables with the known answer that the generator wrote before the run. The script prints one line per check.
def r4():
    code = compare_main(["--known", KNOWN, "--lake", LAKE], spark=spark)
    assert code == 0, "at least one check differs from the known answer"
    return "all checks equal"


check("R4 compare with the known answer", r4)

# COMMAND ----------


# R5: the rebuild. Build a NEW lake from the Capture archive. Then compare it with the Kafka lake, table by table.
# Wait 90 seconds after the last send, so that Capture has written its last file.
def r5():
    archive = f"{BASE}/{w['namespace']}/{w['topic']}"
    in_archive = spark.read.format("avro").option("recursiveFileLookup", "true").load(archive).count()
    run(spark, read_events_capture(spark, archive), LAKE_CAPTURE)  # batch: serverless blocks avro as a stream
    left, right = lake_fingerprints(spark, LAKE), lake_fingerprints(spark, LAKE_CAPTURE)
    for name in left:
        print(f"{name:14} {'SAME' if left[name] == right[name] else 'DIFFERENT'}")
    assert all(left[k] == right[k] for k in left), "a table differs"
    return f"{in_archive} events in the archive; {len(left)} of {len(left)} tables the same (Bronze on the shared columns, Gold latest version)"


check("R5 rebuild from Capture equals the Kafka lake", r5)

# COMMAND ----------

print("\nRESULT SUMMARY")
for name, outcome in results.items():
    if not name.startswith("bronze_"):
        print(f"{name:52} {outcome}")
