"""Rules for .github/workflows/ci.yml. These tests read the text only."""
from pathlib import Path

CI = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"
TEXT = CI.read_text(encoding="utf-8") if CI.exists() else ""


def test_workflow_exists():
    assert TEXT


def test_ci_holds_no_secrets_and_never_touches_azure():
    assert "secrets." not in TEXT
    assert "az " not in TEXT
    assert "azure/login" not in TEXT.lower()


def test_leak_scan_runs_before_the_tests():
    assert "scripts.leak_scan" in TEXT
    assert TEXT.index("scripts.leak_scan") < TEXT.index("pytest")


def test_ci_runs_ruff_and_pytest_on_the_pinned_versions():
    assert "ruff check" in TEXT
    assert 'python-version: "3.12"' in TEXT
    assert 'java-version: "21"' in TEXT
