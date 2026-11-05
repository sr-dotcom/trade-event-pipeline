"""scripts/compare.py checks the pipeline tables against the generator's known answer. It trusts nothing else.

The known answer is a file the generator wrote before the pipeline ran. So a loss before Bronze cannot hide.
"""

import pytest

from scripts.compare import compare, load_known, main, observe
from src.generator.generate import generate, write_output
from src.pipeline.run_local import run


def make_run(spark, root, drop_last_line=False):
    records, known = generate(n_events=3_000, seed=7, profile="A")
    gen = root / "gen"
    write_output(records, known, gen)
    if drop_last_line:  # a loss on the way to Bronze
        events = gen / "events.ndjson"
        lines = events.read_text(encoding="utf-8").splitlines(keepends=True)
        events.write_text("".join(lines[:-1]), encoding="utf-8", newline="")
    counts = run(spark, str(gen / "events.ndjson"), str(root / "lake"))
    return gen, root / "lake", counts


@pytest.fixture(scope="module")
def clean(spark, tmp_path_factory):
    return make_run(spark, tmp_path_factory.mktemp("clean"))


@pytest.fixture(scope="module")
def lossy(spark, tmp_path_factory):
    return make_run(spark, tmp_path_factory.mktemp("lossy"), drop_last_line=True)


def verdicts(spark, gen, lake, change=None):
    known, trades = load_known(gen)
    if change:
        change(known, trades)
    return {name: ok for name, _, _, ok in compare(known, trades, observe(spark, lake))}


def test_a_clean_run_passes_every_check(spark, clean):
    gen, lake, _ = clean
    result = verdicts(spark, gen, lake)
    assert len(result) == 10  # a check that is missing must not pass by silence
    assert all(result.values()), result


def test_an_event_lost_before_bronze_is_found(spark, lossy):
    gen, lake, counts = lossy
    assert counts["events_lost"] == 0  # the pipeline's own check is blind to this loss
    result = verdicts(spark, gen, lake)
    assert not result["events_in"] and not result["reconciliation"], result


def test_a_wrong_number_in_the_known_answer_is_found(spark, clean):
    gen, lake, _ = clean
    result = verdicts(spark, gen, lake, lambda known, trades: known["expected"].update(duplicates=known["expected"]["duplicates"] + 1))
    assert not result["duplicates"]
    assert result["silver"] and result["quarantine"]  # the other checks do not change


def test_a_wrong_trade_version_in_the_expected_state_is_found(spark, clean):
    gen, lake, _ = clean

    def change(known, trades):
        trade_id = next(iter(trades))
        version, action, quantity = trades[trade_id]
        trades[trade_id] = (version + 1, action, quantity)

    assert not verdicts(spark, gen, lake, change)["current_state_mismatches"]


def test_the_exit_code_is_0_for_a_clean_run_and_1_for_a_loss(spark, clean, lossy):
    assert main(["--known", str(clean[0]), "--lake", str(clean[1])], spark) == 0
    assert main(["--known", str(lossy[0]), "--lake", str(lossy[1])], spark) == 1
