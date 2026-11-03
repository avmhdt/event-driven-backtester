"""Result recomputation (AC-W-02) and config round trips (hypothesis)."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import polars as pl
from hypothesis import given, settings
from hypothesis import strategies as st

import edbt
from conftest import fixture_config_dict

NQ = 10**9


def _nq(s: str) -> int:
    return int(Decimal(s) * NQ)


def test_pnl_recompute(canonical_fixture_dir: Path, tmp_path: Path) -> None:
    """An independent Polars recomputation of P&L from fills.parquet matches
    summary.json to 1 nano-quote."""
    d = fixture_config_dict(
        canonical_fixture_dir,
        tmp_path / "runs",
        kind="rust",
        path="taker_imbalance",
        params={"threshold": 0.2, "cooldown_ns": 20_000_000, "max_inventory": 0.004},
    )
    res = edbt.run_backtest(
        "taker_imbalance", edbt.Config.from_dict(d), out_dir=tmp_path / "r", log_experiment=False
    )
    fills = res.fills()
    assert fills.height > 5
    k = 10_000  # tick 0.01 x lot 0.001 = 1e-5 quote = 10,000 nano-quote
    signed = fills.with_columns(
        s=pl.when(pl.col("side") == "buy").then(1).otherwise(-1),
    ).select(
        flow=(-(pl.col("s") * pl.col("price_ticks") * pl.col("qty_lots") * k)).sum(),
        fees=pl.col("fee_nq").sum(),
        pos=(pl.col("s") * pl.col("qty_lots")).sum(),
    )
    flow, fees, pos = (int(signed[c][0]) for c in ("flow", "fees", "pos"))
    last = res.ledger().sort("seq").tail(1)
    mark = int(last["mark_ticks"][0])
    assert int(last["position_lots"][0]) == pos
    import json

    summary = json.loads((tmp_path / "r" / "summary.json").read_text())
    pnl = flow - fees + pos * mark * k
    assert abs(pnl - _nq(summary["pnl_total"])) <= 1
    assert abs(fees - _nq(summary["fees_total"])) <= 1
    assert summary["reconciliation_violations"] == 0


models = st.sampled_from(["none", "risk_averse", "prob_power", "prob_log", "l3_fifo"])


@settings(max_examples=60, deadline=None)
@given(
    seed=st.integers(min_value=0, max_value=2**63 - 1),
    model=models,
    power=st.floats(min_value=0.1, max_value=8.0, allow_nan=False),
    entry=st.integers(min_value=0, max_value=10**9),
    params=st.dictionaries(
        st.text("abcdefgh", min_size=1, max_size=5),
        st.integers(min_value=-(2**63), max_value=2**63 - 1)
        | st.floats(allow_nan=False, allow_infinity=False)
        | st.text(max_size=5),
        max_size=4,
    ),
)
def test_config_round_trip(
    seed: int, model: str, power: float, entry: int, params: dict[str, object]
) -> None:
    c = edbt.Config.from_dict(
        {
            "run": {"seed": seed},
            "queue": {"model": model, "power": power},
            "latency": {"order": {"model": "constant", "entry_ns": entry, "response_ns": entry}},
            "strategy": {"params": params},
        }
    )
    again = edbt.Config.from_dict(c.to_dict())
    assert again.to_dict() == c.to_dict()
    assert again.config_hash() == c.config_hash()
    assert again.to_toml() == c.to_toml()
    assert c.seed == seed
