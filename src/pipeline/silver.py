"""Silver: validate, deduplicate, flag late. Bad records go to quarantine with a reason.

Returns three tables:
- silver:      one row per good event, typed, with is_late
- quarantine:  rows with a reason (missing_field, bad_value, unknown_reference, event_id_conflict)
- duplicates:  exact repeated deliveries (same event_id, same content). Dropped from Silver, but counted.
Silver + quarantine + duplicates = every Bronze row. Nothing is lost.
"""

from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

FIELDS = [
    "event_id",
    "trade_id",
    "trade_version",
    "action",
    "instrument_id",
    "counterparty_id",
    "book_id",
    "side",
    "quantity",
    "price",
    "currency",
    "event_time_utc",
]
EVENT_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"
SCHEMA = ", ".join(f"{f} STRING" for f in FIELDS)  # read everything as text first, so bad values survive


def _reason(reference):
    c = F.col
    missing = reduce(lambda a, b: a | b, [c(f).isNull() for f in FIELDS])
    price = F.expr("try_cast(price AS DECIMAL(18,4))")
    bad_value = (
        ~c("action").isin("NEW", "AMEND", "CANCEL")
        | ~c("side").isin("BUY", "SELL")
        | ~c("quantity").rlike("^[1-9][0-9]*$")
        | F.expr("try_cast(quantity AS INT)").isNull()
        | ~c("trade_version").rlike("^[1-9][0-9]*$")
        | F.expr("try_cast(trade_version AS INT)").isNull()
        | price.isNull()
        | (price <= 0)
        | ~c("currency").isin(*reference["currency"])
        | F.try_to_timestamp(c("event_time_utc"), F.lit(EVENT_TIME_FORMAT)).isNull()
    )
    unknown = (
        ~c("instrument_id").isin(*reference["instrument_id"])
        | ~c("counterparty_id").isin(*reference["counterparty_id"])
        | ~c("book_id").isin(*reference["book_id"])
    )
    # order matters: a missing field is checked first, so no check below sees a null
    return F.when(missing, "missing_field").when(bad_value, "bad_value").when(unknown, "unknown_reference")


def build_silver(bronze, reference, late_after_seconds=300):
    # ponytail: reference IDs go into isin(). Use a join if the lists grow beyond a few thousand.
    parsed = bronze.select("value", "ingest_time", "source", "bronze_id", F.from_json("value", SCHEMA).alias("e"))
    parsed = parsed.select("value", "ingest_time", "source", "bronze_id", *[F.col(f"e.{f}").alias(f) for f in FIELDS])
    checked = parsed.withColumn("reason", _reason(reference))

    bad = (
        checked.filter("reason IS NOT NULL")
        .select("value", "ingest_time", "source", "reason")
        .withColumn("kept_bronze_id", F.lit(None).cast("long"))
        .withColumn("differing_fields", F.lit(None).cast("string"))
    )

    # first arrival wins. Later rows with the same event_id: same content = duplicate, else conflict.
    w = Window.partitionBy("event_id").orderBy("ingest_time", "bronze_id")
    content = F.sha2(F.concat_ws("|", *[F.col(f) for f in FIELDS[1:]]), 256)
    ranked = (
        checked.filter("reason IS NULL")
        .withColumn("rn", F.row_number().over(w))
        .withColumn("same_as_first", content == F.first(content).over(w))
        .withColumn("kept_bronze_id", F.first("bronze_id").over(w))
        # names of the fields whose text differs from the row that stayed, in contract order
        .withColumn(
            "differing_fields",
            F.array_join(F.array(*[F.when(F.col(f) != F.first(f).over(w), F.lit(f)) for f in FIELDS[1:]]), ","),
        )
    )
    later = ranked.filter("rn > 1")
    duplicates = later.filter("same_as_first").select("event_id", "value", "ingest_time", "source")
    conflicts = later.filter("NOT same_as_first").select(
        "value",
        "ingest_time",
        "source",
        F.lit("event_id_conflict").alias("reason"),
        "kept_bronze_id",
        "differing_fields",
    )
    quarantine = bad.unionByName(conflicts)

    silver = (
        ranked.filter("rn = 1")
        .select(
            "event_id",
            "trade_id",
            F.col("trade_version").cast("int"),
            "action",
            "instrument_id",
            "counterparty_id",
            "book_id",
            "side",
            F.col("quantity").cast("int"),
            F.col("price").cast("decimal(18,4)"),
            "currency",
            F.to_timestamp("event_time_utc", EVENT_TIME_FORMAT).alias("event_time_utc"),
            "ingest_time",
            "source",
        )
        .withColumn(
            "is_late",
            F.col("ingest_time") > F.col("event_time_utc") + F.expr(f"INTERVAL {int(late_after_seconds)} SECONDS"),
        )
    )
    return silver, quarantine, duplicates
