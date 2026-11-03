from __future__ import annotations

import os
import time
from pathlib import Path

import pytest
from typer.testing import CliRunner

from edbt.ingest import recordings as rec
from edbt.ingest.cli import app

runner = CliRunner()


def _touch(path: Path, age_s: float = 3600.0, size: int = 10) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    t = time.time() - age_s
    os.utime(path, (t, t))


@pytest.fixture
def sibling_root(tmp_path: Path) -> Path:
    root = tmp_path / "crypto-lob-alpha"
    base = root / "book_l2" / "venue=binance-usdm"
    _touch(base / "symbol=BTCUSDT" / "date=2026-11-01" / "part-00.parquet")
    _touch(base / "symbol=BTCUSDT" / "date=2026-11-02" / "part-00.parquet")
    _touch(base / "symbol=BTCUSDT" / "date=2026-11-02" / "part-01.parquet", age_s=1.0)
    _touch(base / "symbol=ETHUSDT" / "date=2026-11-01" / "part-00.parquet")
    _touch(root / "trades" / "venue=kraken" / "symbol=BTC%2FUSD" / "date=2026-11-01" / "part-00.parquet")
    (root / "status.json").write_text("{}")
    return root


def test_scan_filters_and_skips_open_files(sibling_root: Path) -> None:
    parts = rec.scan(sibling_root, rec.Filter.parse(venue="binance-usdm", symbols="BTCUSDT", dates=None))
    assert [(p.date, p.files, p.skipped_open) for p in parts] == [("2026-11-01", 1, 0), ("2026-11-02", 1, 1)]
    parts = rec.scan(sibling_root, rec.Filter.parse(venue=None, symbols=None, dates="2026-11-01"))
    assert {(p.dataset, p.symbol) for p in parts} == {
        ("book_l2", "BTCUSDT"),
        ("book_l2", "ETHUSDT"),
        ("trades", "BTC/USD"),
    }
    parts = rec.scan(sibling_root, rec.Filter.parse(venue=None, symbols=None, dates="2026-11-02:2026-11-30"))
    assert len(parts) == 1


def test_recordings_dry_run_lists_and_writes_nothing(sibling_root: Path, tmp_path: Path) -> None:
    registry = tmp_path / "datasets.toml"
    before = sorted(p for p in sibling_root.rglob("*"))
    result = runner.invoke(
        app, ["recordings", "--from", str(sibling_root), "--dry-run", "--registry", str(registry)]
    )
    assert result.exit_code == 0, result.output
    assert "BTCUSDT" in result.output
    assert "4 partitions" in result.output
    assert "open file(s) skipped" in result.output
    assert not registry.exists()
    assert sorted(p for p in sibling_root.rglob("*")) == before


def test_recordings_registers(sibling_root: Path, tmp_path: Path) -> None:
    registry = tmp_path / "configs" / "datasets.toml"
    result = runner.invoke(
        app, ["recordings", "--from", str(sibling_root), "--venue", "kraken", "--registry", str(registry)]
    )
    assert result.exit_code == 0, result.output
    text = registry.read_text()
    assert 'symbol = "BTC/USD"' in text
    assert text.count("[[dataset]]") == 1


def test_recordings_needs_a_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("EDBT_EXTERNAL_DATA_ROOTS", "")
    result = runner.invoke(app, ["recordings", "--dry-run"])
    assert result.exit_code == 1


@pytest.mark.parametrize("cmd", ["tardis", "lobster", "instruments", "hftbt-export"])
def test_later_commands_say_not_yet(cmd: str) -> None:
    result = runner.invoke(app, [cmd])
    assert result.exit_code == 2
