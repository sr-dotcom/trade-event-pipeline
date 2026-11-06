"""The probe notebook makes its own test events. The size it asks for must work with the generator.

The first real run of notebooks/azure_probe.py failed in check P2 with "Sample larger than population".
The notebook asked for 200 events, and the generator needs more. See FAILURES.md ("Probe notebook asked the generator for too few events").
"""

import importlib
import re
from pathlib import Path

import pytest

from src.generator.generate import generate

NOTEBOOK = Path(__file__).resolve().parents[1] / "notebooks" / "azure_probe.py"


def test_the_notebook_asks_the_generator_for_a_size_it_can_make():
    text = NOTEBOOK.read_text(encoding="utf-8")
    n = int(re.search(r"generate\(n_events=(\d+)", text).group(1))
    records, _known = generate(n_events=n, seed=1, profile="A")
    assert len(records) == n
    assert f'first["events_in"] == {n}' in text  # the check compares with the same number


@pytest.mark.parametrize("name", ["azure_probe.py", "azure_run.py"])
def test_the_notebooks_import_only_names_that_exist(name):
    """A notebook cannot run on a laptop. This catches a missing function or a typo in an import before Azure does."""
    text = (NOTEBOOK.parent / name).read_text(encoding="utf-8")
    imports = re.findall(r"^from ((?:src|scripts)[\w.]*) import (.+)$", text, re.MULTILINE)
    assert imports
    for module, names in imports:
        for item in (n.split(" as ")[0].strip() for n in names.split(",")):  # "main as compare_main" checks `main`
            assert hasattr(importlib.import_module(module), item), f"{module}.{item}"
