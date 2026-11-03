"""Throughput study (FR-27, AC-H-03): events per second through every path.

Modes, each run `reps` times with the median and interquartile range:

* `rust_engine`: pre-decoded events through the engine, Rust noop.
* `rust_e2e`: the whole run from Parquet, Rust mm_simple, prob_power queue.
* `python_callback_bbo`: Python mm_simple, BBO-change subscription.
* `python_callback_every`: Python noop on every book update and trade.
* `python_stepping`: mm_simple written as a `Backtest.wait_next` loop.

Events are counted by the engine, not by callbacks. The report states the
hardware; numbers from debug builds or CI runners are not claims (D-08:
published runs wait until no multi-week capture runs on the PC).
"""

from __future__ import annotations

import json
import math
import platform
import statistics
import subprocess
import tempfile
import time
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import edbt
from edbt import Side, Strategy, Subscriptions, TimeInForce
from edbt._edbt import cli_main

INSTRUMENT = """[[instrument]]
venue = "synthetic"
symbol = "SYN"
tick_size = 0.01
lot_size = 0.001
contract_type = "linear_perp"
maker_bps = -0.5
taker_bps = 2.0
effective_from = "2020-01-01"
"""


class PyMarketMaker(Strategy):
    """mm_simple as in `strategies/mm_simple.py` (kept here so the study runs from the wheel)."""

    subscriptions = Subscriptions(book="bbo_change", trades=False)

    def __init__(self, refresh_ns: int = 100_000_000, **_: object) -> None:
        """Re-quote at most every `refresh_ns`."""
        self.refresh_ns = refresh_ns
        self._next = -(2**63)

    def on_book(self, ctx: edbt.Context, book: edbt.BookView) -> None:
        """Quote the touch."""
        if not book.valid:
            return
        now = ctx.now()
        if now < self._next:
            return
        bids, asks = book.depth_ticks(1)
        if len(bids) == 0 or len(asks) == 0:
            return
        self._next = now + self.refresh_ns
        ctx.cancel_all(book.symbol)
        ctx.submit_limit_ticks(book.symbol, Side.BUY, int(bids[0, 0]), 1, tif=TimeInForce.GTX)
        ctx.submit_limit_ticks(book.symbol, Side.SELL, int(asks[0, 0]), 1, tif=TimeInForce.GTX)


class PyNoop(Strategy):
    """Receives every book update and trade."""

    subscriptions = Subscriptions(book="every_update", trades=True)

    def on_book(self, ctx: edbt.Context, book: edbt.BookView) -> None:
        """Touch the view so the callback does real work."""
        book.valid  # noqa: B018

    def on_trade(self, ctx: edbt.Context, trade: edbt.Trade) -> None:
        """Nothing."""


@dataclass(frozen=True, slots=True)
class Row:
    """One mode's result."""

    mode: str
    strategy: str
    events: int
    median: float
    iqr: float
    rates: tuple[float, ...]
    peak_rss_mb: float | None


def _median_iqr(v: list[float]) -> tuple[float, float]:
    q = statistics.quantiles(v, n=4, method="inclusive") if len(v) > 1 else [v[0]] * 3
    return statistics.median(v), q[2] - q[0]


def _config(root: Path, out: Path) -> edbt.Config:
    (root / "instruments.toml").write_text(INSTRUMENT, encoding="utf-8")
    return edbt.Config.from_dict(
        {
            "run": {"seed": 7, "out_dir": str(out), "event_log": "off"},
            "data": {
                "roots": [str(root / "data")],
                "venue": "synthetic",
                "symbols": ["SYN"],
                "instruments": str(root / "instruments.toml"),
            },
            "latency": {"feed": {"model": "constant", "constant_ns": 1_000_000}},
            "queue": {"model": "prob_power"},
            "risk": {"kill_stale_feed_ms": 0, "max_orders_per_sec": 0, "max_cancels_per_sec": 0},
        }
    )


def _rust(edbt_bin: str, mode: str, strategy: str, events: int, reps: int) -> Row:
    out = subprocess.run(
        [
            edbt_bin,
            "bench",
            "--mode",
            mode,
            "--strategy",
            strategy,
            "--source",
            "synthetic",
            "--events",
            str(events),
            "--reps",
            str(reps),
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    d = json.loads(out.strip().splitlines()[-1])
    return Row(
        f"rust_{mode}",
        strategy,
        d["events"],
        d["median_events_per_s"],
        d["iqr_events_per_s"],
        tuple(d["events_per_s"]),
        None,
    )


def _python(cfg: edbt.Config, make: type[Strategy], name: str, mode: str, reps: int, out: Path) -> Row:
    rates, events, rss = [], 0, None
    for i in range(reps):
        res = edbt.run_backtest(make(), cfg, out_dir=out / f"{mode}{i}", log_experiment=False)
        rates.append(res.perf.events_per_s)
        events = res.perf.events_processed
        rss = max(rss or 0.0, res.perf.peak_rss_mb)
    m, iqr = _median_iqr(rates)
    return Row(mode, name, events, m, iqr, tuple(rates), rss)


def _stepping(cfg: edbt.Config, reps: int) -> Row:
    rates, events = [], 0
    for _ in range(reps):
        t0 = time.perf_counter()
        bt = edbt.Backtest(cfg)
        ctx, nxt = bt.ctx, -(2**63)
        while (ev := bt.wait_next()) != "end":
            if ev != "book":
                continue
            now = ctx.now()
            if now < nxt:
                continue
            book = ctx.book("SYN")
            bids, asks = book.depth_ticks(1)
            if not book.valid or len(bids) == 0 or len(asks) == 0:
                continue
            nxt = now + 100_000_000
            ctx.cancel_all("SYN")
            ctx.submit_limit_ticks("SYN", Side.BUY, int(bids[0, 0]), 1, tif=TimeInForce.GTX)
            ctx.submit_limit_ticks("SYN", Side.SELL, int(asks[0, 0]), 1, tif=TimeInForce.GTX)
        res = bt.finish(log_experiment=False)
        events = res.perf.events_processed
        rates.append(events / (time.perf_counter() - t0))
    m, iqr = _median_iqr(rates)
    return Row("python_stepping", "mm_simple", events, m, iqr, tuple(rates), None)


def hardware() -> dict[str, str]:
    """CPU, memory and OS of this machine (P-13)."""
    info: dict[str, str] = {"os": platform.platform(), "python": platform.python_version()}
    try:
        lscpu = subprocess.run(["lscpu"], capture_output=True, text=True, check=False).stdout
        for line in lscpu.splitlines():
            k, _, v = line.partition(":")
            if k.strip() in ("Model name", "CPU(s)", "Thread(s) per core", "Core(s) per socket", "L3"):
                info[k.strip()] = v.strip()
        mem = Path("/proc/meminfo").read_text().splitlines()[0].split()[1]
        info["memory"] = f"{int(mem) / 1024 / 1024:.1f} GiB"
    except (OSError, ValueError, IndexError):
        pass
    return info


def run(config: Path | None, out: Path) -> list[Row]:
    """Run every mode and write `throughput.md`, `tables/throughput.csv` and `hardware.md` under `out`."""
    params: dict[str, Any] = {"events": 20_000_000, "reps": 5, "edbt": "edbt"}
    if config is not None:
        params.update(tomllib.loads(config.read_text(encoding="utf-8")).get("throughput", {}))
    events, reps, edbt_bin = int(params["events"]), int(params["reps"]), str(params["edbt"])
    rows: list[Row] = []
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
        cfg = _config(root, root / "runs")
        rows.append(_rust(edbt_bin, "engine", "noop", events, reps))
        rows.append(_rust(edbt_bin, "e2e", "mm_simple", events, reps))
        rows.append(_python(cfg, PyMarketMaker, "mm_simple", "python_callback_bbo", reps, root / "runs"))
        rows.append(_python(cfg, PyNoop, "noop", "python_callback_every", reps, root / "runs"))
        rows.append(_stepping(cfg, reps))
    write_report(rows, hardware(), out, events, reps)
    return rows


def write_report(rows: list[Row], hw: dict[str, str], out: Path, events: int, reps: int) -> None:
    """Write the report, its table and the hardware page."""
    (out / "tables").mkdir(parents=True, exist_ok=True)
    lines = ["mode,strategy,events,median_events_per_s,iqr_events_per_s,peak_rss_mb"]
    for r in rows:
        rss = "" if r.peak_rss_mb is None else f"{r.peak_rss_mb:.0f}"
        lines.append(f"{r.mode},{r.strategy},{r.events},{r.median:.0f},{r.iqr:.0f},{rss}")
    (out / "tables" / "throughput.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
    e2e = next((r.median for r in rows if r.mode == "rust_e2e"), math.nan)
    md = [
        "# Throughput",
        "",
        f"Synthetic L2 data, {events:,} rows, {reps} repetitions per mode, median and IQR of events per",
        "second counted by the engine (spec FR-27). Release build. Hardware in `hardware.md`.",
        "",
        "| Mode | Strategy | Engine events | Median events/s | IQR | Share of Rust e2e |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for r in rows:
        share = f"{r.median / e2e:.0%}" if r.mode.startswith("python") and e2e == e2e else ""
        md.append(f"| {r.mode} | {r.strategy} | {r.events:,} | {r.median:,.0f} | {r.iqr:,.0f} | {share} |")
    md += ["", "Generated by `edbt-study throughput` (`just study-throughput`)."]
    (out / "throughput.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    hw_md = ["# Hardware", "", "| Item | Value |", "|---|---|", *[f"| {k} | {v} |" for k, v in hw.items()]]
    (out / "hardware.md").write_text("\n".join(hw_md) + "\n", encoding="utf-8")
