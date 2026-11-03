"""The console script, the quickstart example and strategy loading, in process."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

import edbt
from conftest import REPO, fixture_config_dict
from edbt import _cli
from edbt.examples import quickstart
from edbt.strategy import load_strategy


def _main(monkeypatch: pytest.MonkeyPatch, *argv: str) -> int:
    monkeypatch.setattr(sys, "argv", ["edbt", *argv])
    with pytest.raises(SystemExit) as e:
        _cli.main()
    code = e.value.code
    assert isinstance(code, int)
    return code


def test_console_script_python_and_rust_runs(
    canonical_fixture_dir: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    py = fixture_config_dict(
        canonical_fixture_dir, tmp_path / "runs", kind="python", path=str(REPO / "strategies" / "noop.py")
    )
    py_cfg = tmp_path / "py.toml"
    py_cfg.write_text(edbt.Config.from_dict(py).to_toml())
    code = _main(
        monkeypatch,
        "run",
        "--config",
        str(py_cfg),
        "--seed",
        "3",
        "--set",
        "queue.model=none",
        "--out",
        str(tmp_path / "o1"),
    )
    assert code == 0
    assert "callbacks" in capsys.readouterr().out
    rust = fixture_config_dict(canonical_fixture_dir, tmp_path / "runs")
    rust_cfg = tmp_path / "rust.toml"
    rust_cfg.write_text(edbt.Config.from_dict(rust).to_toml())
    assert (
        _main(
            monkeypatch,
            "run",
            "--config",
            str(rust_cfg),
            "--no-log-experiment",
            "--out",
            str(tmp_path / "o2"),
        )
        == 0
    )
    assert _main(monkeypatch, "run", "--bogus") == 2
    assert _main(monkeypatch, "--version") == 0
    assert _cli._value("3") == 3
    assert _cli._value("risk_averse") == "risk_averse"


def test_quickstart_runs_in_under_a_minute() -> None:
    res = quickstart.main(events=20_000)
    assert res.summary.n_events > 0
    assert res.perf.wall_time_s < 60


def test_load_strategy_variants(fixture_config: edbt.Config, tmp_path: Path) -> None:
    cfg = fixture_config.with_overrides(
        {
            "strategy.kind": "python",
            "strategy.path": str(REPO / "strategies" / "mm_simple.py"),
            "strategy.params": {"refresh_ns": 5},
        }
    )
    s = load_strategy(cfg)
    assert type(s).__name__ == "MarketMaker"
    assert s.refresh_ns == 5  # type: ignore[attr-defined]
    s2 = load_strategy(cfg.with_overrides({"strategy.class": "MarketMaker"}))
    assert type(s2).__name__ == "MarketMaker"
    with pytest.raises(edbt.ConfigError):
        load_strategy(cfg.with_overrides({"strategy.class": "Nope"}))
    two = tmp_path / "two.py"
    two.write_text("from edbt import Strategy\nclass A(Strategy): ...\nclass B(Strategy): ...\n")
    with pytest.raises(edbt.ConfigError, match="found 2"):
        load_strategy(cfg.with_overrides({"strategy.path": str(two)}))
    mod = load_strategy(
        cfg.with_overrides({"strategy.path": "edbt.examples.quickstart", "strategy.params": {}})
    )
    assert type(mod).__name__ == "MarketMaker"
    with pytest.raises(edbt.ConfigError):
        load_strategy(cfg.with_overrides({"strategy.path": str(tmp_path / "missing.py")}))
