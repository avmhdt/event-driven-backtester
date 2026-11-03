"""AC-W-07 (backtest part): the Rust and Python mm_simple produce byte-identical
orders.parquet on fixture data. The paper and testnet halves arrive in M5."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

import edbt
from conftest import REPO, fixture_config_dict


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
