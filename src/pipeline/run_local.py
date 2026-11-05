"""Run Bronze, Silver, current state and the Gold report on your laptop.

Writes Delta tables and prints a summary.

The Gold report settings live in config/pipeline.toml. The cut-off is 15 minutes after the start. Version 1 is published 5 minutes later.
Then we publish again at the end of the run. If late events changed the numbers, that makes version 2.
Bronze is append-only too. It reads new files only and keeps its place in `_checkpoint/` inside the output folder.
Gold is append-only: a later run adds versions to the table and never rewrites an old one.
Silver, quarantine, duplicates and current state are overwritten on each run. They are recomputed from all of Bronze.
Events can be one file or a folder of NDJSON files. Never put other files in that folder.

Run: python -m src.pipeline.run_local --events data/generated/A/events.ndjson --out data/lake
"""

import argparse
import tomllib
from datetime import datetime, timedelta
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from src.common.reference import load_reference
from src.common.spark import get_spark
from src.pipeline.bronze import read_events_stream, stream_bronze
from src.pipeline.current_state import build_current_state
from src.pipeline.gold import build_report, empty_gold, publish_report
from src.pipeline.silver import build_silver

TIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
ROOT = Path(__file__).resolve().parents[2]


def load_report_config(path=None):
    """The Gold report settings: start_utc, cutoff_minutes, first_publish_minutes. Default file: config/pipeline.toml."""
    with open(path or ROOT / "config" / "pipeline.toml", "rb") as f:
        return tomllib.load(f)["report"]


def _join(base, *parts):
    """Join path parts. A storage address (abfss://...) keeps its double slash. pathlib would break it."""
    base = str(base)
    if "://" in base:
        return "/".join([base.rstrip("/"), *parts])
    return str(Path(base, *parts))


def _has_files(spark, path):
    """True if the folder exists and holds a file. Uses Spark, so it works for a local folder and a storage address.
    It does not use the JVM handle, which Spark Connect (serverless) does not have."""
    try:
        return spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(path).limit(1).count() > 0
    except AnalysisException:  # Spark raises this when the path does not exist
        return False


def run(spark, events, out_dir, report=None):
    """`events` is a path (a file or a folder of NDJSON) or a streaming DataFrame with a `value` column, such as the Kafka read.
    `out_dir` is a local folder or a storage address."""
    report = report or load_report_config()
    # Bronze reads new events only and saves its place. Silver, current state and Gold recompute from all of Bronze.
    bronze_path, checkpoint = _join(out_dir, "bronze"), _join(out_dir, "_checkpoint", "bronze")
    if DeltaTable.isDeltaTable(spark, bronze_path) and not _has_files(spark, checkpoint):
        raise RuntimeError(
            f"{bronze_path} exists but has no checkpoint. A new stream would read every event again and double Bronze. "
            "Delete the lake folder, or restore the checkpoint folder."
        )
    raw = events if hasattr(events, "isStreaming") else read_events_stream(spark, events)
    stream_bronze(raw, bronze_path, checkpoint)
    bronze = spark.read.format("delta").load(bronze_path)
    silver, quarantine, duplicates = build_silver(bronze, load_reference())
    current_state = build_current_state(silver)

    start = datetime.strptime(report["start_utc"], TIME_FMT)
    cutoff = start + timedelta(minutes=report["cutoff_minutes"])
    first_publish = cutoff + timedelta(minutes=report["first_publish_minutes"])
    last_ingest = silver.agg(
        F.date_format(F.max("ingest_time") + F.expr("INTERVAL 1 SECOND"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    ).first()[0]
    # Gold is append-only. Start from the versions already published, so a published version is never rewritten.
    # A changed number becomes a new version. The same numbers again make no new version.
    gold_path = _join(out_dir, "gold_report")
    published = spark.read.format("delta").load(gold_path) if DeltaTable.isDeltaTable(spark, gold_path) else empty_gold(spark)
    gold = published
    for as_of in (first_publish.strftime(TIME_FMT), last_ingest):
        gold = publish_report(
            gold, build_report(silver, cutoff.strftime(TIME_FMT), as_of), cutoff.strftime(TIME_FMT), as_of
        )
    # Collect the few new rows first. The append below writes to the table that `published` reads.
    new_versions = spark.createDataFrame(gold.exceptAll(published).collect(), gold.schema)

    tables = (
        ("silver", silver),
        ("quarantine", quarantine),
        ("duplicates", duplicates),
        ("current_state", current_state),
    )
    for name, df in tables:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(_join(out_dir, name))
    new_versions.write.format("delta").mode("append").save(gold_path)
    read = lambda name: spark.read.format("delta").load(_join(out_dir, name))
    counts = {
        "events_in": read("bronze").count(),
        "silver": read("silver").count(),
        "quarantine": read("quarantine").count(),
        "duplicates": read("duplicates").count(),
        "late_flagged": read("silver").filter("is_late").count(),
        "trades": read("current_state").count(),
        "active_trades": read("current_state").filter("is_active").count(),
        "gold_versions": read("gold_report").agg(F.max("report_version")).first()[0],
    }
    counts["events_lost"] = counts["events_in"] - counts["silver"] - counts["quarantine"] - counts["duplicates"]
    return counts


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--events", default="data/generated/A/events.ndjson")
    p.add_argument("--out", default="data/lake")
    a = p.parse_args()
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    for k, v in run(spark, a.events, a.out).items():
        print(f"{k:14} {v}")
    spark.stop()


if __name__ == "__main__":
    main()
