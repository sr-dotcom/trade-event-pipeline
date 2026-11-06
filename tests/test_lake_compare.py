"""Two lakes built from the same events through different sources must hold the same data.

Source 1 is a file. Source 2 is a Capture archive, read as a stream. This is the rebuild proof for Azure: build a new lake
from the archive, then compare it with the lake built from Kafka. Bronze keeps the Kafka columns only for Kafka data,
so the comparison uses the columns both sources have. Gold keeps its version history, so only the latest version is compared.
The test uses a local fixture. A real archive is UNVERIFIED (see docs/azure-probe-results.md).
"""

import json

from src.generator.generate import generate
from src.pipeline.capture_source import read_events_capture_stream
from src.pipeline.replay import lake_fingerprints
from src.pipeline.run_local import run
from tests.test_capture_source import write_capture


def test_a_lake_built_from_a_capture_archive_equals_a_lake_built_from_the_file(spark, tmp_path):
    records, _known = generate(n_events=2_000, seed=7, profile="A")
    lines = [json.dumps(r) for r in records]
    events = tmp_path / "events.ndjson"
    events.write_text("".join(line + "\n" for line in lines), encoding="utf-8")
    write_capture(spark, tmp_path / "archive", 0, lines)
    run(spark, str(events), str(tmp_path / "lake_file"))
    run(spark, read_events_capture_stream(spark, str(tmp_path / "archive")), str(tmp_path / "lake_capture"))
    left = lake_fingerprints(spark, str(tmp_path / "lake_file"))
    right = lake_fingerprints(spark, str(tmp_path / "lake_capture"))
    assert set(left) == {"bronze", "silver", "quarantine", "duplicates", "current_state", "gold_latest"}
    assert {k: left[k] == right[k] for k in left} == dict.fromkeys(left, True)
