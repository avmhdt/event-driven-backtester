# Architecture

One engine and one event queue, with interchangeable exchange back-ends.
Only the event source and the gateway change between backtest, paper and
testnet modes; the strategy, order manager and risk checks are the same code
in every mode.

## Crates

| Crate | Role | Depends on |
|---|---|---|
| `edbt-book` | L2 price-level and L3 order-level books, `BookView` | nothing in the workspace |
| `edbt-ledger` | Fixed-point ledger and reconciliation identities | nothing in the workspace |
| `edbt-risk` | Pre-trade checks, rate limits, kill switch | nothing in the workspace |
| `edbt-core` | Engine, scheduler, config, RNG streams, event log, order manager, `Strategy`/`Context`/`Gateway` traits | book, ledger, risk |
| `edbt-data` | Canonical Parquet readers and writers, `ReplaySource`, instruments, validation, synthetic data | core, book |
| `edbt-exchange` | `SimGateway`: exchange-time books, latency, matching, queue models, fees | core, book |
| `edbt-strategies` | Rust reference strategies and their registry | core |
| `edbt-live` | Binance USD-M testnet adapter, host guard, recorder | core |
| `edbt-cli` | `edbt` binary and the run wiring shared with Python | all of the above |
| `edbt-py` | PyO3 module `edbt._edbt` | cli |

The three leaf crates take primitive integers, so each is tested on its own
and the domain types (`Side`, `OrderRequest`, `MarketEvent`) live in
`edbt-core`.

## Two clocks, two books

The simulated exchange works in exchange time and the strategy in local
time. Each symbol has two books:

- the **exchange book** inside `SimGateway`, updated when an event reaches
  the exchange (rank 0, at its effective `ts_exchange`); orders are matched
  against it;
- the **local book** inside the engine, updated when the same event arrives
  locally (rank 3, after the feed latency); the strategy only ever sees this
  one (DQ-08).

An L2 batch is applied atomically: the strategy is called, and the local book
is inspected, only after the batch's last row.

## Life of one event

1. The `ReplaySource` merges the Parquet streams by effective exchange time
   and hands the engine the next `MarketEvent`.
2. The engine logs it and passes it to the gateway (`on_market`). The
   gateway updates its exchange book and schedules local delivery at
   `ts_exchange + feed latency`, made non-decreasing per stream.
3. At the local time the engine applies the event to the local book, updates
   the ledger mark (mid, half-even rounded to a tick) and calls the strategy
   if its subscriptions ask for it.
4. After every event the ledger is reconciled (FR-16).

Orders (M3) follow the same pattern: `Context::submit` runs the order manager
and risk checks, the gateway schedules the arrival at the exchange after the
entry latency (rank 1), matches it there, and schedules responses back at
local time (rank 2).

## Run directory

`edbt run` writes `results/runs/<run_id>/` with `config.resolved.toml`,
`orders.parquet`, `fills.parquet`, `ledger.parquet`, `events.log.zst`,
`summary.json`, `perf.json` and `hashes.json` (spec section 7(b)). See
`determinism.md` for what is hashed.
