"""AC-W-07: the Rust and Python mm_simple produce byte-identical
orders.parquet on fixture data, and strategies/mm_simple.py runs unchanged in
backtest, paper and testnet mode (CI: against the local mock venue)."""

from __future__ import annotations

import copy
import importlib.util
import sys
from pathlib import Path

import pytest

import edbt
from conftest import FIXTURES, REPO, fixture_config_dict
from edbt import _cli
from edbt.strategy import load_strategy


def _python_mm() -> type[edbt.Strategy]:
    spec = importlib.util.spec_from_file_location(
        "edbt_strategies_mm_simple", REPO / "strategies" / "mm_simple.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    cls: type[edbt.Strategy] = module.MarketMaker
    return cls


@pytest.mark.parametrize("refresh_ns", [0, 50_000_000])
def test_rust_and_python_mm_simple_identical(
    canonical_fixture_dir: Path, tmp_path: Path, refresh_ns: int
) -> None:
    params = {
        "refresh_ns": refresh_ns,
        "order_qty": 0.002,
        "skew_ticks_per_unit": 300.0,
        "max_inventory": 0.006,
    }
    d = fixture_config_dict(
        canonical_fixture_dir, tmp_path / "runs", kind="rust", path="mm_simple", params=params
    )
    d["risk"].update({"max_orders_per_sec": 0, "max_cancels_per_sec": 0})
    cfg = edbt.Config.from_dict(d)
    rust = edbt.run_backtest("mm_simple", cfg, out_dir=tmp_path / "rust", log_experiment=False)
    py = edbt.run_backtest(_python_mm()(**params), cfg, out_dir=tmp_path / "py", log_experiment=False)
    assert rust.summary.n_orders > 50 and rust.summary.n_fills > 0
    for name in ("orders.parquet", "fills.parquet", "ledger.parquet"):
        assert (tmp_path / "rust" / name).read_bytes() == (tmp_path / "py" / name).read_bytes(), name
    assert rust.summary == py.summary


def _replay(monkeypatch: pytest.MonkeyPatch, run: Path) -> int:
    monkeypatch.setattr(sys, "argv", ["edbt", "log", "replay-strategy", "--run", str(run)])
    with pytest.raises(SystemExit) as e:
        _cli.main()
    code = e.value.code
    assert isinstance(code, int)
    return code


def test_same_strategy_all_modes(
    canonical_fixture_dir: Path,
    tmp_path: Path,
    mock_venue: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy = {
        "kind": "python",
        "path": str(REPO / "strategies" / "mm_simple.py"),
        "class": "MarketMaker",
        "params": {"refresh_ns": 200_000_000},
    }
    base = fixture_config_dict(canonical_fixture_dir, tmp_path / "runs", **strategy)
    bt = edbt.Config.from_dict(base)
    res = edbt.run_backtest(load_strategy(bt), bt, log_experiment=False)
    assert res.mode == edbt.Mode.BACKTEST and res.summary.n_orders > 0

    # Paper and testnet: the same strategy file; run.mode and [live] differ,
    # and the data section names the mock venue's instruments.
    live = copy.deepcopy(base)
    live["data"].update(
        {"venue": "mock", "symbols": ["BTCUSDT"], "instruments": str(FIXTURES / "instruments_mock.toml")}
    )
    live["risk"]["kill_stale_feed_ms"] = 5000
    live["live"] = {
        "venue": "mock",
        "rest_url": mock_venue["rest_url"],
        "ws_market_url": mock_venue["ws_url"],
        "allowed_hosts": ["127.0.0.1"],
        "record": False,
        "session_dir": str(tmp_path / "live"),
        "tick_interval_ms": 200,
        "status_interval_s": 1,
        "reconcile_interval_s": 1,
    }
    monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "mock-api-key")
    monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "mock-api-secret")
    for mode in ("paper", "testnet"):
        d = copy.deepcopy(live)
        d["run"]["mode"] = mode
        cfg = edbt.Config.from_dict(d)
        out = edbt.run_live(load_strategy(cfg), cfg, duration_s=6, confirm_testnet=True)
        assert out.mode.value == mode
        assert out.summary.n_orders > 0, mode
        assert out.summary.reconciliation_violations == 0
        assert out.out_dir is not None and (out.out_dir / "live.json").exists()
        if mode == "testnet":
            assert out.latency().height > 0
        # FR-33: the logged inputs reproduce every outbound intent.
        assert _replay(monkeypatch, out.out_dir) == 0


def test_run_live_validates_before_connecting(tmp_path: Path) -> None:
    d = fixture_config_dict(tmp_path, tmp_path / "runs")
    d["run"]["mode"] = "testnet"
    cfg = edbt.Config.from_dict(d)
    with pytest.raises(edbt.EdbtError, match="confirm_testnet"):
        edbt.run_live("noop", cfg)
    d["run"]["mode"] = "paper"
    with pytest.raises(edbt.EdbtError, match="live"):
        edbt.run_live("noop", edbt.Config.from_dict(d), duration_s=1)
    d["live"] = {"allowed_hosts": ["fapi.binance.com"], "rest_url": "https://fapi.binance.com"}
    with pytest.raises(edbt.LiveGuardError):
        edbt.run_live("noop", edbt.Config.from_dict(d), duration_s=1)
    with pytest.raises(edbt.EdbtError, match="positive"):
        edbt.run_live("noop", edbt.Config.from_dict(d), duration_s=0)
