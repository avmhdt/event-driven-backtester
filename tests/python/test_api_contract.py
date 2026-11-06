"""AC-W-11: every public symbol of the section 10 Python API, exercised."""

from __future__ import annotations

import dataclasses
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq
import pytest

import edbt
from edbt import (
    Backtest,
    BacktestResult,
    BookView,
    Config,
    ConfigError,
    Context,
    DataError,
    EdbtError,
    Fill,
    InvalidOrderError,
    Liquidity,
    LiveGuardError,
    Mode,
    OrderStatus,
    OrderUpdate,
    OrdType,
    Perf,
    Position,
    ReconciliationError,
    RiskEvent,
    RiskState,
    Settings,
    Side,
    Strategy,
    Subscriptions,
    Summary,
    TimeInForce,
    Trade,
    load_result,
    run_backtest,
    run_live,
)
from edbt._edbt import cli_main


def test_versions_and_exports() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+", edbt.__version__)
    assert edbt.API_VERSION.startswith("1.")
    for name in edbt.__all__:
        assert hasattr(edbt, name), name
    assert [Side.BUY, Side.SELL] == [1, -1]
    assert OrdType.LIMIT == "limit" and OrdType.MARKET == "market"
    assert {t.value for t in TimeInForce} == {"gtc", "ioc", "fok", "gtx"}
    assert len(OrderStatus) == 8 and OrderStatus.PENDING_NEW == "pending_new"
    assert Liquidity.MAKER == "maker" and Mode.TESTNET == "testnet"
    for err in (ConfigError, DataError, InvalidOrderError, ReconciliationError, LiveGuardError):
        assert issubclass(err, EdbtError)
    for dc in (Trade, OrderUpdate, Fill, Position, RiskEvent, RiskState, Subscriptions, Summary, Perf):
        assert dataclasses.is_dataclass(dc)
    assert Subscriptions().book == "bbo_change"


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


def test_config_api(tmp_path: Path) -> None:
    p = tmp_path / "c.toml"
    p.write_text('[run]\nseed = 3\nstart = 2026-01-01T00:00:00Z\n[queue]\nmodel = "risk_averse"\n')
    c = Config.from_toml(p)
    assert c.seed == 3 and c.mode == Mode.BACKTEST
    assert c.to_dict()["queue"]["model"] == "risk_averse"
    assert c.to_dict()["run"]["start"] == "2026-01-01T00:00:00Z"
    assert "risk_averse" in c.to_toml()
    d = c.with_overrides({"queue.model": "none", "strategy.params.x": 1})
    assert d.to_dict()["queue"]["model"] == "none" and d.config_hash() != c.config_hash()
    assert Config.from_dict(c.to_dict()).config_hash() == c.config_hash()
    assert "Config(" in repr(c)
    with pytest.raises(ConfigError):
        Config.from_dict({"queue": {"model": "magic"}})
    with pytest.raises(ConfigError):
        Config.from_toml(tmp_path / "missing.toml")
    with pytest.raises(ConfigError):
        c.with_overrides({"queue.model.x": 1})


class Exerciser(Strategy):
    """Touches every Context and BookView method once."""

    subscriptions = Subscriptions(book="every_update", trades=True, depth_levels=3)

    def __init__(self, **params: Any) -> None:
        self.seen: dict[str, Any] = {}
        self.kept: BookView | None = None
        self.params = params

    def on_start(self, ctx: Context) -> None:
        self.seen["mode"] = ctx.mode
        self.seen["params"] = dict(ctx.params)
        self.seen["timer"] = ctx.set_timer(every_ns=50_000_000)
        ctx.set_timer(at_ns=ctx.now() + 1)
        self.seen["rand"] = (ctx.random(), ctx.rng_normal(), ctx.rng_normal(1.0, 0.0))
        ctx.log("started", a=1, b="x")

    def on_book(self, ctx: Context, book: BookView) -> None:
        if not book.valid or "book" in self.seen:
            return
        bid, ask = book.best_bid(), book.best_ask()
        assert bid is not None and ask is not None
        bids, _asks = book.depth(3)
        tb, ta = book.depth_ticks(3)
        assert bids.shape[1] == 2 and not bids.flags.writeable
        assert tb.dtype == np.int64 and bids[0, 0] == pytest.approx(bid[0])
        assert book.mid() == pytest.approx((bid[0] + ask[0]) / 2)
        assert book.spread_ticks() == int(ta[0, 0] - tb[0, 0])
        assert (book.symbol, book.tick_size, book.lot_size) == ("SYN", 0.01, 0.001)
        assert book.ts_local == ctx.now() and book.ts_exchange <= book.ts_local
        assert ctx.book("SYN").best_bid() == bid
        self.kept = book
        self.snap = book.snapshot()
        self.seen["book"] = repr(book)
        t = book.tick_size
        # The market order goes first: our own resting sell would expire it (STP).
        ctx.submit_market("SYN", Side.BUY, 0.001, tag="mkt")
        a = ctx.submit_limit("SYN", Side.BUY, bid[0] - 5 * t, 0.002, tif=TimeInForce.GTC, tag="far")
        b = ctx.submit_limit_ticks("SYN", Side.SELL, int(ta[0, 0]) + 5, 2)
        assert ctx.order(a) is not None and ctx.order(10_000) is None
        assert len(ctx.open_orders()) >= 2 and len(ctx.open_orders("SYN")) >= 2
        new = ctx.modify(a, price=bid[0] - 6 * t)
        assert new != a
        ctx.cancel(b)
        with pytest.raises(InvalidOrderError):
            ctx.submit_limit("SYN", Side.BUY, bid[0] + t / 3, 0.001)
        with pytest.raises(InvalidOrderError):
            ctx.submit_limit("NOPE", Side.BUY, 1.0, 0.001)
        with pytest.raises(InvalidOrderError):
            ctx.submit_limit("SYN", Side.BUY, bid[0], 0.0001)
        with pytest.raises(InvalidOrderError):
            ctx.modify(99_999, price=1.0)
        self.seen["ids"] = (a, b, new)

    def on_trade(self, ctx: Context, trade: Trade) -> None:
        self.seen.setdefault("trade", trade)

    def on_order_update(self, ctx: Context, update: OrderUpdate) -> None:
        self.seen.setdefault("updates", []).append(update)

    def on_fill(self, ctx: Context, fill: Fill) -> None:
        self.seen.setdefault("fills", []).append(fill)
        self.seen["position"] = ctx.position("SYN")
        self.seen["positions"] = ctx.positions()

    def on_timer(self, ctx: Context, timer_id: int) -> None:
        n = self.seen.get("timers", 0) + 1
        self.seen["timers"] = n
        if n == 3:
            ctx.cancel_timer(self.seen["timer"])
            ctx.cancel_all()
            ctx.cancel_all("SYN")
            ctx.kill_switch("done")

    def on_risk_event(self, ctx: Context, event: RiskEvent) -> None:
        self.seen.setdefault("risk", []).append(event)

    def on_stop(self, ctx: Context) -> None:
        self.seen["risk_state"] = ctx.risk_state()


def test_strategy_context_and_results(fixture_config: Config, tmp_path: Path) -> None:
    s = Exerciser()
    cfg = fixture_config.with_overrides({"strategy.params": {"p": 1}})
    res = run_backtest(s, cfg, out_dir=tmp_path / "run")
    assert isinstance(res, BacktestResult)
    assert s.seen["mode"] == Mode.BACKTEST and s.seen["params"] == {"p": 1}
    assert s.seen["timers"] == 3
    assert isinstance(s.seen["trade"], Trade) and s.seen["trade"].symbol == "SYN"
    statuses = {u.status for u in s.seen["updates"]}
    assert {OrderStatus.NEW, OrderStatus.CANCELED}.issubset(statuses)
    assert all(isinstance(u, OrderUpdate) for u in s.seen["updates"])
    assert isinstance(s.seen["fills"][0], Fill) and s.seen["fills"][0].liquidity == Liquidity.TAKER
    assert isinstance(s.seen["position"], Position) and set(s.seen["positions"]) == {"SYN"}
    assert isinstance(s.seen["risk_state"], RiskState) and s.seen["risk_state"].killed
    assert any(isinstance(e, RiskEvent) and e.kind == "kill_switch" for e in s.seen["risk"])
    assert s.snap.best_bid() is not None
    assert s.kept is not None
    with pytest.raises(EdbtError, match="outside its callback"):
        s.kept.best_bid()
    assert res.summary.kill_switch_triggered and res.summary.n_fills >= 1
    assert isinstance(res.summary, Summary) and isinstance(res.perf, Perf)
    assert res.perf.python_callbacks > 0
    assert res.orders().height > 0 and res.fills().height >= 1 and res.ledger().height > 0
    ev = res.events(["start", "stop", "log"])
    assert set(ev["kind"].to_list()) == {"start", "stop", "log"}
    assert any("started a=1 b='x'" in d for d in ev["detail"].to_list())
    assert res.latency().height == 0
    saved = res.save(tmp_path / "copy")
    again = load_result(saved)
    assert again.run_hash == res.run_hash and again.summary == res.summary
    assert "BacktestResult(" in repr(res)


def test_rust_strategy_by_name_and_errors(fixture_config: Config, tmp_path: Path) -> None:
    res = run_backtest("mm_simple", fixture_config, out_dir=tmp_path / "mm", log_experiment=True)
    assert res.config.to_dict()["strategy"]["path"] == "mm_simple"
    assert res.summary.n_orders > 0
    assert (tmp_path / "results" / "experiment_log.csv").exists()
    with pytest.raises(EdbtError):
        run_backtest(object(), fixture_config)  # type: ignore[arg-type]
    with pytest.raises(EdbtError):
        run_backtest("no_such_strategy", fixture_config, out_dir=tmp_path / "x")
    with pytest.raises(ConfigError):
        run_backtest(
            "noop", fixture_config.with_overrides({"data.symbols": ["NONE"]}), out_dir=tmp_path / "y"
        )
    with pytest.raises(DataError):
        run_backtest(
            "noop", fixture_config.with_overrides({"data.symbols": ["SYN3"]}), out_dir=tmp_path / "z"
        )
    with pytest.raises(EdbtError, match="run_live"):
        run_backtest("noop", fixture_config.with_overrides({"run.mode": "paper"}))
    with pytest.raises(EdbtError):
        load_result(tmp_path / "nowhere")


class Boom(Strategy):
    def on_book(self, ctx: Context, book: BookView) -> None:
        raise ValueError("strategy bug")


def test_python_exception_aborts_with_traceback(fixture_config: Config, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="strategy bug") as info:
        run_backtest(Boom(), fixture_config, out_dir=tmp_path / "boom")
    assert info.traceback[-1].name == "on_book"


def test_run_live_guards(fixture_config: Config) -> None:
    with pytest.raises(EdbtError, match="paper or testnet"):
        run_live("noop", fixture_config)
    with pytest.raises(EdbtError, match="confirm_testnet"):
        run_live("noop", fixture_config.with_overrides({"run.mode": "testnet"}))
    with pytest.raises(EdbtError, match="positive"):
        run_live("noop", fixture_config.with_overrides({"run.mode": "paper"}), duration_s=0)
    with pytest.raises(EdbtError, match=r"\[live\] is required"):
        run_live("noop", fixture_config.with_overrides({"run.mode": "testnet"}), confirm_testnet=True)


def test_stepping_api(fixture_config: Config) -> None:
    with Backtest(fixture_config) as bt:
        assert bt.events_processed == 0
        assert bt.elapse(10_000_000)
        assert bt.events_processed > 0
        kinds = {bt.wait_next() for _ in range(20)}
        assert kinds <= {"book", "trade", "order", "timer"}
        book = bt.ctx.book("SYN")
        bid = book.best_bid()
        assert bid is not None
        oid = bt.ctx.submit_limit("SYN", Side.BUY, bid[0], 0.001, tif=TimeInForce.GTX)
        assert bt.wait_next(timeout_ns=1) in ("book", "trade", "order", "timer", "timeout")
        while bt.wait_next() not in ("order", "end"):
            pass
        order = bt.ctx.order(oid)
        assert order is not None and order.status != OrderStatus.PENDING_NEW
        bt.run_to_end()
        assert bt.wait_next() == "end"
        assert not bt.elapse(1)
    res = bt.finish()
    assert res.summary.n_orders >= 1
    assert bt.finish() is res
    with pytest.raises(EdbtError):
        Backtest(fixture_config, strategy=object())  # type: ignore[arg-type]


def test_fixtures_convert_to_canonical_parquet(canonical_fixture_dir: Path) -> None:
    l2 = sorted((canonical_fixture_dir / "book_l2").rglob("*.parquet"))
    assert l2
    cols = set(pq.read_table(l2[0]).column_names)
    want = {
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
    }
    assert want <= cols
    assert pq.ParquetFile(l2[0]).metadata.created_by == "edbt"


def test_cli_synth_and_validate(tmp_path: Path) -> None:
    out = tmp_path / "data"
    args = ["edbt", "data", "synth", "--kind", "l2", "--events", "5000", "--seed", "3", "--out", str(out)]
    assert cli_main(args) == 0
    assert cli_main(["edbt", "data", "validate", "--dataset", "book_l2", "--root", str(out)]) == 0
    assert cli_main(["edbt", "data", "validate", "--dataset", "book_l3", "--root", str(out)]) == 1
    with pytest.raises(RuntimeError, match=r"x\.toml"):
        cli_main(["edbt", "run", "--config", "x.toml"])
    assert cli_main(["edbt", "--version"]) == 0
    assert cli_main(["edbt", "no-such-command"]) == 2


def test_console_script_runs_python_strategy(canonical_fixture_dir: Path, tmp_path: Path) -> None:
    from conftest import REPO, fixture_config_dict

    d = fixture_config_dict(
        canonical_fixture_dir,
        tmp_path / "runs",
        kind="python",
        path=str(REPO / "strategies" / "mm_simple.py"),
        **{"class": "MarketMaker"},
    )
    cfg_path = tmp_path / "py.toml"
    cfg_path.write_text(Config.from_dict(d).to_toml())
    cmd = [sys.executable, "-m", "edbt._cli", "run", "--config", str(cfg_path), "--no-log-experiment"]
    proc = subprocess.run([*cmd, "--set", "run.seed=9"], capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    assert "run_hash" in proc.stdout and "callbacks" in proc.stdout
    proc = subprocess.run(
        [sys.executable, "-m", "edbt._cli", "--help"], capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0 and "replay-check" in proc.stdout
