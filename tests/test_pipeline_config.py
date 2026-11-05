"""The pipeline does not depend on the generator. It reads its own report settings from config/pipeline.toml.

Before this change, run_local took the report start time from the generator's config. See DECISIONS.md.
"""

import ast
import json
import tomllib
from pathlib import Path

from src.pipeline.run_local import load_report_config, run
from tests.helpers import ev

ROOT = Path(__file__).resolve().parents[1]


def test_no_pipeline_module_imports_the_generator():
    offenders = []
    for path in (ROOT / "src" / "pipeline").glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            names = [node.module] if isinstance(node, ast.ImportFrom) else [a.name for a in getattr(node, "names", [])]
            offenders += [f"{path.name}: {n}" for n in names if n and n.startswith("src.generator")]
    assert offenders == []


def test_the_report_start_follows_the_settings_it_is_given(spark, tmp_path):
    """Events on another day. With the generator's date the cut-off is before them and Gold stays empty."""
    event = ev("E-1", arrival="2030-01-01T10:00:01.000Z", event_time_utc="2030-01-01T10:00:00Z")
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "part-0.ndjson").write_text(json.dumps(event) + "\n", encoding="utf-8")
    report = {"start_utc": "2030-01-01T10:00:00Z", "cutoff_minutes": 15, "first_publish_minutes": 5}
    run(spark, str(landing), str(tmp_path / "lake"), report)
    gold = spark.read.format("delta").load(str(tmp_path / "lake" / "gold_report")).collect()
    assert [(r.report_version, r.book_id, r.active_trades) for r in gold] == [(1, "BOOK-01", 1)]


def test_the_report_start_equals_the_generator_start():
    """Two files hold the same date. If they drift apart, the 50,000-event run gets an empty Gold report."""
    with open(ROOT / "config" / "faults.toml", "rb") as f:
        generator_start = tomllib.load(f)["run"]["start_utc"]
    assert load_report_config()["start_utc"] == generator_start
