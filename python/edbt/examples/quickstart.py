"""Quickstart: a market maker on seeded synthetic data (`python -m edbt.examples.quickstart`).

Self-contained: it generates a small synthetic L2 dataset and an instrument
file in a temporary directory, so it runs from an installed wheel without
the repository. It prints the P&L, the run hash and the throughput.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import edbt
from edbt import Side, Strategy, Subscriptions, TimeInForce
from edbt._edbt import cli_main

INSTRUMENT = """
[[instrument]]
venue = "synthetic"
symbol = "SYN"
tick_size = 0.01
lot_size = 0.001
contract_type = "linear_perp"
maker_bps = -0.5
taker_bps = 2.0
effective_from = "2020-01-01"
"""


class MarketMaker(Strategy):
    """Quote the touch, post-only, re-quoting on every BBO change."""

    subscriptions = Subscriptions(book="bbo_change", trades=False)

    def __init__(self, half_spread_ticks: int = 1, order_qty: float = 0.001, **kw: object) -> None:
        """Quote offset in ticks and size in base units."""
        self.h, self.q = half_spread_ticks, order_qty

    def on_book(self, ctx: edbt.Context, book: edbt.BookView) -> None:
        """Cancel and re-quote both sides."""
        if not book.valid:
            return
        ctx.cancel_all(book.symbol)
        bid, ask = book.best_bid(), book.best_ask()
        if bid and ask:
            t = book.tick_size
            ctx.submit_limit(book.symbol, Side.BUY, bid[0] - (self.h - 1) * t, self.q, tif=TimeInForce.GTX)
            ctx.submit_limit(book.symbol, Side.SELL, ask[0] + (self.h - 1) * t, self.q, tif=TimeInForce.GTX)


def main(events: int = 200_000) -> edbt.BacktestResult:
    """Generate data, run the backtest and print the headline numbers."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cli_main(
            [
                "edbt",
                "data",
                "synth",
                "--kind",
                "l2",
                "--events",
                str(events),
                "--seed",
                "7",
                "--out",
                str(root / "data"),
            ]
        )
        (root / "instruments.toml").write_text(INSTRUMENT, encoding="utf-8")
        config = edbt.Config.from_dict(
            {
                "run": {"name": "quickstart", "seed": 7, "out_dir": str(root / "runs")},
                "data": {
                    "roots": [str(root / "data")],
                    "venue": "synthetic",
                    "symbols": ["SYN"],
                    "instruments": str(root / "instruments.toml"),
                },
                "latency": {"feed": {"model": "constant", "constant_ns": 1_000_000}},
                "risk": {"kill_stale_feed_ms": 0, "max_orders_per_sec": 0, "max_cancels_per_sec": 0},
            }
        )
        res = edbt.run_backtest(MarketMaker(), config, log_experiment=False)
        print(res.summary.pnl_total, res.run_hash, res.perf.events_per_s)
        print(f"fills={res.summary.n_fills} orders={res.summary.n_orders} events={res.summary.n_events}")
        return res


if __name__ == "__main__":
    main()
