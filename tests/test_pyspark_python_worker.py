"""F8: Python rows and Python UDFs must work in local PySpark on Windows."""

from pyspark.sql.functions import udf


def test_create_dataframe_and_udf(spark):
    df = spark.createDataFrame([("a", 1), ("b", 2)], "k string, v int")
    assert df.count() == 2
    double = udf(lambda x: x * 2, "int")
    assert sorted(r[0] for r in df.select(double("v")).collect()) == [2, 4]
