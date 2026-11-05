"""Current state: for each trade, the event with the highest trade_version.

Version order wins, not arrival order. A late event with a lower version never replaces a newer one.
A cancelled trade stays in the table with is_active = false.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F


def build_current_state(silver):
    # ponytail: assumes one event per (trade_id, trade_version). Silver dedupe makes this true for event_id only.
    w = Window.partitionBy("trade_id").orderBy(F.col("trade_version").desc())
    latest = silver.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    return latest.withColumn("is_active", F.col("action") != "CANCEL")
