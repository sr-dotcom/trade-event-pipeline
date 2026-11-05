"""Local Spark session with Delta, and the Windows set-up (see FAILURES.md: "Local Delta write fails on Windows", "Python worker crashes on Windows")."""

import os
import sys
from pathlib import Path


def setup_windows():
    """Call before the first Spark session. Does nothing on other systems."""
    if sys.platform != "win32":
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        return
    # F1: Java loads hadoop.dll from PATH. HADOOP_HOME alone is not enough.
    if os.environ.get("HADOOP_HOME"):
        os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ["PATH"]
    # F8: `python -m pyspark.worker` crashes. Use the launcher in src/common/winspark.
    os.environ["PYSPARK_REAL_PYTHON"] = sys.executable
    os.environ["PYSPARK_PYTHON"] = str(Path(__file__).parent / "winspark" / "python.cmd")


def get_spark(app_name="trade-event-pipeline"):
    setup_windows()
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
    )
    # spark-avro reads the Event Hubs Capture archive. Databricks has it built in. Match the pyspark version.
    return configure_spark_with_delta_pip(builder, extra_packages=["org.apache.spark:spark-avro_2.13:4.1.1"]).getOrCreate()
