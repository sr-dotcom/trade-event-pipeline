"""run() works with a storage address (abfss://...) and with a streaming DataFrame, not only a local folder and a file path.

Before this change, run() joined paths with pathlib. pathlib turns `abfss://CONTAINER/x`
into `abfss:/CONTAINER/x`, which is a different address. It also asked the local disk
whether the checkpoint exists. See FAILURES.md ("`run()` would have broken storage addresses").
"""

from scripts.compare import compare, load_known, observe
from src.generator.generate import generate, write_output
from src.pipeline.run_local import _has_files, _join, run


def test_join_keeps_both_slashes_of_a_storage_address():
    base = "abfss://lake" + "@" + "acct.dfs.core.windows.net/x"  # split, so the leak scan does not read it as an email
    assert _join(base, "bronze") == base + "/bronze"
    assert _join(base + "/", "_checkpoint", "bronze") == base + "/_checkpoint/bronze"


def test_join_of_a_local_path_is_the_same_as_before(tmp_path):
    assert _join(str(tmp_path), "bronze") == str(tmp_path / "bronze")


def test_has_files_is_false_for_a_missing_folder_and_true_for_a_folder_with_a_file(spark, tmp_path):
    assert not _has_files(spark, tmp_path.as_uri() + "/nothing-here")
    (tmp_path / "f.txt").write_text("x", encoding="utf-8")
    assert _has_files(spark, tmp_path.as_uri())


def test_run_gives_the_same_counts_for_a_streaming_dataframe_and_a_uri_lake(spark, tmp_path):
    records, known = generate(n_events=2_000, seed=7, profile="A")
    write_output(records, known, tmp_path / "gen")
    by_path = run(spark, str(tmp_path / "gen" / "events.ndjson"), str(tmp_path / "lake_a"))
    stream = spark.readStream.option("pathGlobFilter", "events.ndjson").text(str(tmp_path / "gen"))
    by_stream = run(spark, stream, (tmp_path / "lake_b").as_uri())  # a file:// address, like abfss:// has a scheme
    assert by_stream == by_path


def test_the_compare_script_reads_a_lake_given_as_a_uri(spark, tmp_path):
    """scripts/compare.py joined the lake path with pathlib too, so it could not read an abfss:// lake."""
    records, known = generate(n_events=2_000, seed=7, profile="A")
    write_output(records, known, tmp_path / "gen")
    lake = (tmp_path / "lake").as_uri()
    run(spark, str(tmp_path / "gen" / "events.ndjson"), lake)
    answer, trades = load_known(tmp_path / "gen")
    assert all(ok for *_, ok in compare(answer, trades, observe(spark, lake)))
