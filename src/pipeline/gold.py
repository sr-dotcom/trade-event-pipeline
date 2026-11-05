"""Gold report: active trades per book and currency at a cut-off, with numbered versions.

Two clocks decide what a report counts:
- event time: the trade happened at or before the cut-off
- ingest time: the event arrived by the publish time (as_of)

A late event can change a report that is already published. Then we publish a new version.
Old versions never change. Publishing the same numbers again makes no new version.
A publish time earlier than the latest version makes no new version either.
"""

from pyspark.sql import functions as F

from src.pipeline.current_state import build_current_state

# One row per book and currency. We never add amounts in different currencies.
REPORT_COLUMNS = ["book_id", "currency", "active_trades", "total_quantity", "notional"]


def build_report(silver, cutoff, as_of):
    """cutoff and as_of are UTC text, for example "2026-11-02T14:15:00Z"."""
    visible = silver.filter(
        (F.col("event_time_utc") <= F.lit(cutoff).cast("timestamp"))
        & (F.col("ingest_time") <= F.lit(as_of).cast("timestamp"))
    )
    return (
        build_current_state(visible)
        .filter("is_active")
        .groupBy("book_id", "currency")
        .agg(
            F.count("*").alias("active_trades"),
            F.sum("quantity").cast("bigint").alias("total_quantity"),
            F.sum(F.col("quantity") * F.col("price")).cast("decimal(38,4)").alias("notional"),
        )
    )


def empty_gold(spark):
    return spark.sql(
        "SELECT CAST(NULL AS TIMESTAMP) AS cutoff, CAST(NULL AS INT) AS report_version, "
        "CAST(NULL AS TIMESTAMP) AS published_at, CAST(NULL AS STRING) AS book_id, CAST(NULL AS STRING) AS currency, "
        "CAST(NULL AS BIGINT) AS active_trades, CAST(NULL AS BIGINT) AS total_quantity, "
        "CAST(NULL AS DECIMAL(38,4)) AS notional WHERE false"
    )


def publish_report(gold, report, cutoff, as_of):
    """Return gold plus the report as a new version, or gold unchanged if the numbers did not change."""
    # ponytail: a report with no rows has nothing to store, so it never gets a version.
    cut = F.lit(cutoff).cast("timestamp")
    mine = gold.filter(F.col("cutoff") == cut)
    last = mine.agg(F.max("report_version")).first()[0]
    if last is None:
        version = 1
    else:
        # Versions go forward in time. A publish time before the latest version is history, not a new version.
        # This also makes a rerun on the same events safe.
        if mine.filter((F.col("report_version") == last) & (F.col("published_at") > F.lit(as_of).cast("timestamp"))).head(1):
            return gold
        previous = mine.filter(F.col("report_version") == last).select(*REPORT_COLUMNS)
        if not previous.exceptAll(report).head(1) and not report.exceptAll(previous).head(1):
            return gold
        version = last + 1
    new = report.select(
        cut.alias("cutoff"),
        F.lit(version).cast("int").alias("report_version"),
        F.lit(as_of).cast("timestamp").alias("published_at"),
        *REPORT_COLUMNS,
    )
    return gold.unionByName(new)
