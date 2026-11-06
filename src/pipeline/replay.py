"""Rebuild proof: delete every table, rebuild from the raw file, compare fingerprints.

The raw file is the archive. We never change it. Everything else can be rebuilt from it.

Run: python -m src.pipeline.replay --events data/generated/A/events.ndjson --out data/lake
"""

import argparse
import shutil
from pathlib import Path

from pyspark.sql import functions as F

from src.common.spark import get_spark
from src.pipeline.run_local import _join, run

TABLES = ("bronze", "silver", "quarantine", "duplicates", "current_state", "gold_report")


def fingerprint(df):
    """One code for a whole table. It does not depend on the order of the rows.

    Each row gets a hash. We sort the hashes and hash them again, together with the row count.
    """
    # ponytail: collects all row hashes into one value. Fine for 50,000 rows. Use a different method at millions.
    row = F.sha2(
        F.concat_ws("\x1f", *[F.coalesce(F.col(f"`{c}`").cast("string"), F.lit("\x00null")) for c in df.columns]), 256
    )
    hashes = F.sort_array(F.collect_list(row))
    return df.agg(F.sha2(F.concat_ws("|", F.size(hashes).cast("string"), F.concat_ws(",", hashes)), 256)).first()[0]


def fingerprints(spark, lake):
    return {name: fingerprint(spark.read.format("delta").load(str(Path(lake) / name))) for name in TABLES}


def replay(spark, events_path, lake):
    """Return the fingerprints before and after, and the tables we deleted."""
    before = fingerprints(spark, lake)
    deleted = []
    for name in TABLES:
        shutil.rmtree(Path(lake) / name)
        deleted.append(name)
    shutil.rmtree(Path(lake) / "_checkpoint", ignore_errors=True)  # a new Bronze needs a new checkpoint
    run(spark, events_path, str(lake))
    return {"before": before, "after": fingerprints(spark, lake), "deleted": deleted}


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--events", default="data/generated/A/events.ndjson")
    p.add_argument("--out", default="data/lake")
    a = p.parse_args()
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    result = replay(spark, a.events, a.out)
    for name in TABLES:
        same = result["before"][name] == result["after"][name]
        print(
            f"{name:14} before {result['before'][name][:12]}  after {result['after'][name][:12]}  {'SAME' if same else 'DIFFERENT'}"
        )
    spark.stop()


if __name__ == "__main__":
    main()


BRONZE_COMMON = ["value", "ingest_time", "source", "bronze_id"]  # Kafka-only columns are left out


def lake_fingerprints(spark, lake):
    """Fingerprints of a lake, for comparing two lakes built from different sources (file, Kafka, Capture).
    Bronze: the columns every source has. Gold: only the latest version, without its number, because a lake built
    in several runs keeps the older versions and a lake built in one run does not."""
    read = lambda name: spark.read.format("delta").load(_join(lake, name))
    gold = read("gold_report")
    latest = gold.agg(F.max("report_version")).first()[0]
    return {
        "bronze": fingerprint(read("bronze").select(*BRONZE_COMMON)),
        "silver": fingerprint(read("silver")),
        "quarantine": fingerprint(read("quarantine")),
        "duplicates": fingerprint(read("duplicates")),
        "current_state": fingerprint(read("current_state")),
        "gold_latest": fingerprint(gold.filter(F.col("report_version") == latest).drop("report_version")),
    }
