"""A run that stops in the middle of Bronze, then runs again from the same checkpoint.

The crash is a poison event in file 3. A UDF raises on it while a flag file exists. Delete the flag and run again.
This tests a failed micro-batch, not a crash inside a Delta commit. See DECISIONS.md.
"""

import os

import pytest
from pyspark.errors import StreamingQueryException
from pyspark.sql import functions as F

from src.pipeline.bronze import read_events_stream, stream_bronze
from src.pipeline.replay import fingerprint
from tests.helpers import ev
from tests.test_bronze_stream import Lake


class CrashLake(Lake):
    def run_with_poison(self, flag):
        """One event per micro-batch of one file. The UDF raises on E-POISON while `flag` exists."""

        @F.udf("string")
        def crash_on_poison(value):
            if "E-POISON" in value and os.path.exists(flag):
                raise ValueError("test crash")
            return value

        raw = read_events_stream(self.spark, self.landing, max_files=1)
        return stream_bronze(raw.withColumn("value", crash_on_poison("value")), self.bronze, self.checkpoint)


FILES = {
    "f1": [ev("E-1"), ev("E-2"), ev("E-3")],
    "f2": [ev("E-4"), ev("E-5"), ev("E-6")],
    "f3": [ev("E-7"), ev("E-POISON"), ev("E-9")],
}


def test_a_failed_batch_is_redone_and_bronze_has_no_gap_and_no_double(spark, tmp_path):
    lake = CrashLake(spark, tmp_path)
    for name, records in FILES.items():
        lake.drop(name, records)
    flag = tmp_path / "crash.flag"
    flag.write_text("crash", encoding="utf-8")

    with pytest.raises(StreamingQueryException):
        lake.run_with_poison(str(flag))
    assert lake.table().count() == 6  # rows in Bronze after the crashed run

    flag.unlink()
    lake.run_with_poison(str(flag))
    bronze = lake.table()
    assert bronze.count() == 9
    assert bronze.select("bronze_id").distinct().count() == 9  # no event twice
    assert fingerprint(bronze) == fingerprint(lake.one_shot())  # same as a clean one-shot read
