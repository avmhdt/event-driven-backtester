"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from edbt._edbt import cli_main

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / "tests" / "fixtures"


@pytest.fixture(scope="session")
def canonical_fixture_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Committed fixture CSVs converted to canonical Parquet in a temp data root."""
    root = tmp_path_factory.mktemp("canonical")
    csvs = sorted(FIXTURES.glob("*_small.csv"))
    assert csvs, "no fixture CSVs found"
    args = ["edbt", "data", "synth", "--out", str(root)]
    for csv in csvs:
        args += ["--fixture", str(csv)]
    assert cli_main(args) == 0
    return root
