"""T1: local PySpark can write and read a Delta table on Windows."""


def test_delta_roundtrip(spark, tmp_path):
    path = str(tmp_path / "t1")
    spark.sql("SELECT * FROM VALUES ('T-1', 1), ('T-2', 2) AS t(trade_id, v)").write.format("delta").save(path)
    assert spark.read.format("delta").load(path).count() == 2
