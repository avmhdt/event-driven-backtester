"""Contract tests for the public Python API (grows to the full section 10 surface in M4)."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

import edbt
from edbt import Settings
from edbt._edbt import cli_main


def test_versions() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+", edbt.__version__)
    assert re.fullmatch(r"\d+\.\d+\.\d+", edbt.API_VERSION)


def test_settings_expand_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_ROOT", "~/data/x")
    monkeypatch.setenv("EDBT_DATA_DIR", "")
    monkeypatch.setenv("EDBT_EXTERNAL_DATA_ROOTS", "~/a: /b")
    s = Settings()
    assert s.data_root == Path.home() / "data" / "x"
    assert s.data_dir == s.data_root
    assert s.external_roots == [Path.home() / "a", Path("/b")]
    monkeypatch.setenv("EDBT_DATA_DIR", str(tmp_path))
    assert Settings().data_dir == tmp_path


def test_fixtures_convert_to_canonical_parquet(canonical_fixture_dir: Path) -> None:
    l2 = sorted((canonical_fixture_dir / "book_l2").rglob("*.parquet"))
    assert l2
    table = pq.read_table(l2[0])
    assert {
        "ts_exchange",
        "ts_recv",
        "venue",
        "symbol",
        "seq",
        "side",
        "price",
        "qty",
        "is_snapshot",
        "batch_id",
    } <= set(table.column_names)
    assert pq.ParquetFile(l2[0]).metadata.created_by == "edbt"


def test_cli_synth_and_validate(tmp_path: Path) -> None:
    out = tmp_path / "data"
    assert (
        cli_main(
            ["edbt", "data", "synth", "--kind", "l2", "--events", "5000", "--seed", "3", "--out", str(out)]
        )
        == 0
    )
    assert cli_main(["edbt", "data", "validate", "--dataset", "book_l2", "--root", str(out)]) == 0
    assert cli_main(["edbt", "data", "validate", "--dataset", "book_l3", "--root", str(out)]) == 1
    assert cli_main(["edbt", "run", "--config", "x.toml"]) == 2
    assert cli_main(["edbt", "--version"]) == 0
    assert cli_main(["edbt", "no-such-command"]) == 2


def test_console_script_module() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "edbt._cli", "--help"], capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0
    assert "replay-check" in proc.stdout
