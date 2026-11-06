"""run() accepts a batch DataFrame (not only a stream). The Capture rebuild needs this.

Databricks serverless does not allow the `avro` format as a streaming source
(UNSUPPORTED_STREAMING_SOURCE_PERMISSION_ENFORCED). See FAILURES.md. A rebuild starts from an empty lake,
so it needs no checkpoint. It reads the archive once, in one batch.
"""

from src.generator.generate import generate, write_output
from src.pipeline.run_local import run


def test_a_batch_dataframe_gives_the_same_counts_as_a_file_and_a_second_run_does_not_double_bronze(spark, tmp_path):
    records, known = generate(n_events=2_000, seed=7, profile="A")
    write_output(records, known, tmp_path / "gen")
    path = str(tmp_path / "gen" / "events.ndjson")
    expected = run(spark, path, str(tmp_path / "lake_a"))
    batch = spark.read.text(path)  # column `value`, not a stream
    assert not batch.isStreaming
    lake = str(tmp_path / "lake_b")
    assert run(spark, batch, lake) == expected
    assert run(spark, batch, lake) == expected  # a rebuild again replaces Bronze, it does not add to it
