# Python API

`import edbt` — **API_VERSION 1.0.0**. This page is the contract that
`market-making-sim` codes against (NFR-14): symbols are not renamed or
removed; additive changes bump the minor version, breaking changes need the
owner's approval, a major bump, a `CHANGELOG.md` entry and contract-test
updates (`tests/python/test_api_contract.py`).

Install: the wheel `edbt-sim` (one abi3 wheel per platform, Python >= 3.12)
or, from the repository, `uv sync`. `python -m edbt.examples.quickstart`
runs a self-contained example.

## Conventions

- Prices and quantities are floats in instrument units at this boundary,
  checked against the tick and lot grid (off-grid values raise
  `InvalidOrderError`). `*_ticks` variants take integers. Inside the engine
  everything is integer ticks, lots and nano-quote.
- Timestamps are integer nanoseconds since the Unix epoch, UTC.
- Order entry returns a client order id at once. Acceptance, rejection
  (risk rejections carry `reason = "RISK:<rule>"`) and fills arrive later,
  in local time, through callbacks. For one fill the order is: ledger
  updated, then `on_fill`, then `on_order_update`.

## A strategy

```python
import edbt
from edbt import Side, Strategy, Subscriptions, TimeInForce

class MarketMaker(Strategy):
    subscriptions = Subscriptions(book="bbo_change", trades=False)

    def __init__(self, half_spread_ticks: int = 1, order_qty: float = 0.001, **kw: object) -> None:
        self.h, self.q = half_spread_ticks, order_qty

    def on_book(self, ctx: edbt.Context, book: edbt.BookView) -> None:
        if not book.valid:
            return
        ctx.cancel_all(book.symbol)
        bid, ask = book.best_bid(), book.best_ask()
        if bid and ask:
            t = book.tick_size
            ctx.submit_limit(book.symbol, Side.BUY, bid[0] - (self.h - 1) * t, self.q, tif=TimeInForce.GTX)
            ctx.submit_limit(book.symbol, Side.SELL, ask[0] + (self.h - 1) * t, self.q, tif=TimeInForce.GTX)

res = edbt.run_backtest(MarketMaker(), "configs/backtest_synthetic.toml")
print(res.summary.pnl_total, res.run_hash, res.perf.events_per_s)
```

Callbacks you do not override are never called. `subscriptions` is applied
in Rust before any Python runs: `book` is `"none"`, `"bbo_change"` (best
bid or ask price or size changed, or validity changed) or `"every_update"`
(after every atomic book update); `trades` turns trade callbacks on;
`symbols` restricts callbacks to some symbols.

A Python exception in a callback stops all further callbacks, aborts the
run, and is re-raised by `run_backtest` with its traceback.

## Strategy callbacks

| Callback | When |
|---|---|
| `on_start(ctx)` | once, at `run.start` |
| `on_book(ctx, book)` | book update (per subscription) |
| `on_trade(ctx, trade)` | trade (per subscription) |
| `on_order_update(ctx, update)` | any order state change |
| `on_fill(ctx, fill)` | a fill, before its `on_order_update` |
| `on_timer(ctx, timer_id)` | a timer from `ctx.set_timer` |
| `on_risk_event(ctx, event)` | risk reject, warning, kill switch or reset |
| `on_stop(ctx)` | once, at the end |

## `Context`

| Method | Notes |
|---|---|
| `mode`, `params` | run mode; `[strategy.params]` |
| `now()` | local time, ns |
| `book(symbol)` | the local, latency-delayed `BookView` |
| `position(symbol)`, `positions()` | `Position` dataclasses |
| `open_orders(symbol=None)`, `order(id)` | `OrderUpdate` dataclasses |
| `submit_limit(symbol, side, price, qty, *, tif=GTC, tag=None)` | returns the client order id |
| `submit_limit_ticks(symbol, side, price_ticks, qty_lots, *, tif, tag)` | integer variant |
| `submit_market(symbol, side, qty, *, tag=None)` | IOC market order |
| `cancel(id)`, `cancel_all(symbol=None)` | unknown or finished orders are ignored |
| `modify(id, *, price=None, qty=None)` | cancel-replace; returns the new id; loses priority |
| `set_timer(*, at_ns=None, every_ns=None)`, `cancel_timer(id)` | `at_ns` defaults to now + `every_ns` |
| `random()`, `rng_normal(mu=0, sigma=1)` | the strategy's seeded stream (FR-17); the only randomness strategies may use |
| `log(msg, **fields)` | into the event log |
| `risk_state()` | `RiskState` |
| `kill_switch(reason)` | cancels everything, blocks new orders |

A context (and a `BookView`) is valid only inside the callback that
received it. Using one later raises `EdbtError`; `BookView.snapshot()`
returns an owned copy that can be kept.

## `BookView`

`symbol`, `ts_local`, `ts_exchange`, `valid`, `tick_size`, `lot_size`;
`best_bid()` and `best_ask()` as `(price, qty)` or `None`; `mid()`;
`spread_ticks()`; `depth(levels=10)` and `depth_ticks(levels=10)` as
`(bids, asks)`, each a read-only `(n, 2)` NumPy array of `[price, qty]`
(bids descending, asks ascending); `snapshot()`.

## Running

| Function | Purpose |
|---|---|
| `run_backtest(strategy, config, *, out_dir=None, log_experiment=True)` | whole run; `strategy` is a `Strategy` or a Rust registry name (`"noop"`, `"mm_simple"`, `"taker_imbalance"`) |
| `Backtest(config, strategy=None)` | stepping API: `ctx`, `events_processed`, `elapse(ns)`, `wait_next(timeout_ns=None)` (returns `"book"`, `"trade"`, `"order"`, `"timer"`, `"timeout"` or `"end"`), `run_to_end()`, `finish()`; usable as a context manager |
| `run_live(strategy, config, *, duration_s=None, confirm_testnet=False)` | paper and testnet sessions (M5); testnet needs `confirm_testnet=True` |
| `load_result(path)` | reopen a run directory |

`BacktestResult` has `run_id`, `mode`, `config`, `summary` (`Summary`),
`perf` (`Perf`), `run_hash`, `out_dir`, and `orders()`, `fills()`,
`ledger()`, `events(kinds=None)`, `latency()` as Polars DataFrames, and
`save(path)`.

`Config.from_toml(path)` / `Config.from_dict(d)` resolve every default in
Rust; `to_dict()`, `to_toml()`, `with_overrides({"queue.model": "none"})`,
`config_hash()`, `mode`, `seed`.

## Errors

`EdbtError` is the base of `ConfigError`, `DataError`, `InvalidOrderError`,
`ReconciliationError` and `LiveGuardError` (a refused non-testnet host).

## Performance

The fastest Python paths are the BBO-change subscription and the stepping
API: book events reach Python only when they matter, filtering happens in
Rust, and order entry is a direct call into the engine. Throughput is
measured by `just study-throughput` (see `reports/throughput.md`).
