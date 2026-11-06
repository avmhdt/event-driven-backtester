"""Shared pytest fixtures."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

import edbt
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


def fixture_config_dict(root: Path, out_dir: Path, **strategy: Any) -> dict[str, Any]:
    """A backtest config over the L2 fixture (`SYN`) with constant latencies."""
    return {
        "run": {"name": "fixture", "seed": 7, "out_dir": str(out_dir), "event_log": "full"},
        "data": {
            "roots": [str(root)],
            "venue": "synthetic",
            "symbols": ["SYN"],
            "book": "book_l2",
            "instruments": str(FIXTURES / "instruments.toml"),
        },
        "latency": {
            "feed": {"model": "constant", "constant_ns": 500_000},
            "order": {"model": "constant", "entry_ns": 1_000_000, "response_ns": 1_000_000},
        },
        "risk": {"kill_stale_feed_ms": 0, "price_band_bps": 500.0},
        "strategy": strategy or {"kind": "rust", "path": "noop"},
    }


@pytest.fixture
def fixture_config(canonical_fixture_dir: Path, tmp_path: Path) -> edbt.Config:
    """The fixture config with run directories under `tmp_path`."""
    return edbt.Config.from_dict(fixture_config_dict(canonical_fixture_dir, tmp_path / "runs"))


@pytest.fixture(autouse=True)
def _results_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep experiment-log rows written by tests out of the repository."""
    monkeypatch.setenv("EDBT_RESULTS_DIR", str(tmp_path / "results"))


def _mock(tmp_path: Path, proto: str) -> Iterator[dict[str, str]]:
    """The local mock venue in a subprocess (`edbt mock-venue`)."""
    port_file = tmp_path / f"mock-{proto}.json"
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "from edbt._cli import main; main()",
            "mock-venue",
            *("--port-file", str(port_file), "--duration-s", "300", "--proto", proto),
        ],
        stdout=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 30
    while not port_file.exists():
        assert proc.poll() is None, "mock venue exited"
        assert time.monotonic() < deadline, "mock venue did not start"
        time.sleep(0.05)
    urls: dict[str, str] = json.loads(port_file.read_text())
    yield urls
    proc.terminate()
    proc.wait(timeout=20)


@pytest.fixture
def mock_venue(tmp_path: Path) -> Iterator[dict[str, str]]:
    """The Binance-protocol mock venue."""
    yield from _mock(tmp_path, "binance")


@pytest.fixture
def deribit_mock(tmp_path: Path) -> Iterator[dict[str, str]]:
    """The Deribit-protocol mock venue."""
    yield from _mock(tmp_path, "deribit")
