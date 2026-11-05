"""Shared test set-up. The Windows fixes live in src/common/spark.py (see FAILURES.md: "Local Delta write fails on Windows", "Python worker crashes on Windows")."""

import pytest

from src.common.spark import get_spark, setup_windows

setup_windows()


@pytest.fixture(scope="session")
def spark():
    session = get_spark("tests")
    yield session
    session.stop()
