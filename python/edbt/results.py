"""Run results as Polars DataFrames, and the run entry points (FR-20).

`run_backtest` runs a whole backtest, `Backtest` steps through one
(hftbacktest-style; the fastest Python path), and `load_result` reopens a
run directory written by either, or by the `edbt run` CLI.
"""

from __future__ import annotations

import datetime as dt
import json
import shutil
from pathlib import Path
from typing import Any, Literal

import polars as pl

from edbt import _edbt
from edbt.config import Config, _expand
from edbt.strategy import Context, Strategy
from edbt.types import EdbtError, Mode, Perf, Summary

__all__ = ["Backtest", "BacktestResult", "load_result", "run_backtest", "run_live"]

ORDER_LATENCY_SCHEMA = {
    "ts_req_local": pl.Int64,
    "ts_exch": pl.Int64,
    "ts_resp_local": pl.Int64,
    "kind": pl.String,
    "channel": pl.String,
    "venue": pl.String,
    "symbol": pl.String,
    "client_order_id": pl.String,
    "status": pl.String,
    "entry_ns": pl.Int64,
    "response_ns": pl.Int64,
    "rtt_ns": pl.Int64,
}


def _summary(d: dict[str, Any]) -> Summary:
    return Summary(
        pnl_total=d["pnl_total_f"],
        pnl_realized=d["pnl_realized_f"],
        pnl_unrealized=d["pnl_unrealized_f"],
        fees_total=d["fees_total_f"],
        rebates_total=d["rebates_total_f"],
        n_events=d["n_events"],
        n_orders=d["n_orders"],
        n_cancels=d["n_cancels"],
        n_rejects=d["n_rejects"],
        n_fills=d["n_fills"],
        fill_ratio=d["fill_ratio"],
        maker_fill_share=d["maker_fill_share"],
        traded_notional=d["traded_notional_f"],
        max_drawdown=d["max_drawdown_f"],
        final_positions=dict(d["final_positions"]),
        reconciliation_violations=d["reconciliation_violations"],
        book_gaps=d["book_gaps"],
        kill_switch_triggered=d["kill_switch_triggered"],
    )


def _perf(d: dict[str, Any]) -> Perf:
    return Perf(
        events_processed=d["events_processed"],
        wall_time_s=d["wall_time_s"],
        events_per_s=d["events_per_s"],
        python_callbacks=d["python_callbacks"],
        peak_rss_mb=float(d["peak_rss_mb"] or 0.0),
    )


class BacktestResult:
    """A finished run and its files."""

    def __init__(
        self,
        run_id: str,
        mode: Mode,
        config: Config,
        summary: Summary,
        perf: Perf,
        run_hash: str,
        out_dir: Path | None,
    ) -> None:
        """Hold the run's metadata; tables are read on demand."""
        self.run_id = run_id
        self.mode = mode
        self.config = config
        self.summary = summary
        self.perf = perf
        self.run_hash = run_hash
        self.out_dir = out_dir

    def _dir(self) -> Path:
        if self.out_dir is None:
            raise EdbtError("this result has no run directory")
        return self.out_dir

    def orders(self) -> pl.DataFrame:
        """One row per order state transition."""
        return pl.read_parquet(self._dir() / "orders.parquet")

    def fills(self) -> pl.DataFrame:
        """One row per fill."""
        return pl.read_parquet(self._dir() / "fills.parquet")

    def ledger(self) -> pl.DataFrame:
        """Ledger rows (after every fill, periodic marks, end of run)."""
        return pl.read_parquet(self._dir() / "ledger.parquet")

    def events(self, kinds: list[str] | tuple[str, ...] | None = None) -> pl.DataFrame:
        """The decoded event log, optionally only some record kinds."""
        cols = _edbt.read_events(str(self._dir() / "events.log.zst"), list(kinds) if kinds else None)
        return pl.DataFrame(
            cols,
            schema={
                "seq": pl.UInt64,
                "ts": pl.Int64,
                "rank": pl.UInt8,
                "kind": pl.String,
                "detail": pl.String,
            },
        )

    def latency(self) -> pl.DataFrame:
        """Measured order latency (paper and testnet runs only; empty here)."""
        path = self._dir() / "order_latency.parquet"
        if path.exists():
            return pl.read_parquet(path)
        return pl.DataFrame(schema=ORDER_LATENCY_SCHEMA)

    def save(self, path: str | Path) -> Path:
        """Copy the run directory to `path`."""
        dest = Path(path)
        shutil.copytree(self._dir(), dest, dirs_exist_ok=True)
        return dest

    def __repr__(self) -> str:
        """Short form."""
        return f"BacktestResult({self.run_id}, pnl={self.summary.pnl_total:.6f}, hash={self.run_hash[:12]})"


def load_result(path: str | Path) -> BacktestResult:
    """Reopen a run directory."""
    d = Path(path)
    try:
        summary = json.loads((d / "summary.json").read_text(encoding="utf-8"))
        perf = json.loads((d / "perf.json").read_text(encoding="utf-8"))
        hashes = json.loads((d / "hashes.json").read_text(encoding="utf-8"))
    except OSError as e:
        raise EdbtError(f"{d} is not a run directory: {e}") from e
    config = Config.from_toml(d / "config.resolved.toml")
    return BacktestResult(
        run_id=d.name,
        mode=config.mode,
        config=config,
        summary=_summary(summary),
        perf=_perf(perf),
        run_hash=hashes["run_hash"],
        out_dir=d,
    )


def _as_config(config: Config | str | Path) -> Config:
    return config if isinstance(config, Config) else Config.from_toml(config)


def _with_strategy(cfg: Config, strategy: Strategy | str) -> tuple[Config, Strategy | None]:
    if isinstance(strategy, str):
        return cfg.with_overrides({"strategy.kind": "rust", "strategy.path": strategy}), None
    if not isinstance(strategy, Strategy):
        raise EdbtError("strategy must be an edbt.Strategy instance or a Rust registry name")
    return cfg, strategy


def _run_id(cfg: Config) -> str:
    now = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%S")
    return f"{now}-{cfg.config_hash()[:8]}-s{cfg.seed}"


def run_backtest(
    strategy: Strategy | str,
    config: Config | str | Path,
    *,
    out_dir: str | Path | None = None,
    log_experiment: bool = True,
) -> BacktestResult:
    """Run a backtest. `strategy` is an `edbt.Strategy` or a Rust registry name."""
    cfg, obj = _with_strategy(_as_config(config), strategy)
    if cfg.mode != Mode.BACKTEST:
        raise EdbtError(f"run.mode = {cfg.mode.value}: use run_live for paper and testnet sessions")
    params = cfg.to_dict()["strategy"].get("params", {})
    r = _edbt.run_backtest(
        json.dumps(cfg.to_dict()), obj, params, None if out_dir is None else str(out_dir), log_experiment
    )
    return load_result(r["out_dir"])


def run_live(
    strategy: Strategy | str,
    config: Config | str | Path,
    *,
    duration_s: float | None = None,
    confirm_testnet: bool = False,
) -> BacktestResult:
    """Run a paper or testnet session (the live adapter arrives in M5)."""
    cfg, _ = _with_strategy(_as_config(config), strategy)
    if cfg.mode == Mode.BACKTEST:
        raise EdbtError("run_live needs run.mode = paper or testnet; use run_backtest")
    if cfg.mode == Mode.TESTNET and not confirm_testnet:
        raise EdbtError("testnet sessions need confirm_testnet=True")
    if duration_s is not None and duration_s <= 0:
        raise EdbtError("duration_s must be positive")
    raise EdbtError("live sessions arrive with the Binance testnet adapter (M5)")


class Backtest:
    """Step through a backtest from Python (hftbacktest-style)."""

    def __init__(self, config: Config | str | Path, strategy: Strategy | None = None) -> None:
        """Build the engine; the run directory is `run.out_dir/<run_id>`."""
        cfg = _as_config(config)
        if strategy is not None and not isinstance(strategy, Strategy):
            raise EdbtError("strategy must be an edbt.Strategy instance or None")
        self._config = cfg
        self._run_id = _run_id(cfg)
        out = _expand(cfg.to_dict()["run"]["out_dir"]) / self._run_id
        params = cfg.to_dict()["strategy"].get("params", {})
        self._inner = _edbt.Backtest(json.dumps(cfg.to_dict()), strategy, params, str(out), self._run_id)
        self._result: BacktestResult | None = None

    @property
    def ctx(self) -> Context:
        """A context for orders and queries between steps."""
        return self._inner.ctx

    @property
    def events_processed(self) -> int:
        """Events processed by the engine so far."""
        return self._inner.events_processed

    def elapse(self, duration_ns: int) -> bool:
        """Advance local time; `False` when the data is exhausted."""
        return self._inner.elapse(duration_ns)

    def wait_next(
        self, timeout_ns: int | None = None
    ) -> Literal["book", "trade", "order", "timer", "timeout", "end"]:
        """Process events until one reaches the strategy, a timeout, or the end."""
        r = self._inner.wait_next(timeout_ns)
        if r not in ("book", "trade", "order", "timer", "timeout", "end"):
            raise EdbtError(f"unexpected step result {r!r}")
        return r  # type: ignore[return-value]

    def run_to_end(self) -> None:
        """Process every remaining event."""
        self._inner.run_to_end()

    def finish(self, *, log_experiment: bool = True) -> BacktestResult:
        """Finish, write the run files and return the result."""
        if self._result is None:
            r = self._inner.finish(log_experiment)
            self._result = load_result(r["out_dir"])
        return self._result

    def __enter__(self) -> Backtest:
        """Use as a context manager; the run finishes on exit."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Finish the run unless an exception is propagating."""
        if exc and exc[0] is None:
            self.finish()
