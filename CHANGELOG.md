# Changelog

Python API changes are flagged **[API]** for `market-making-sim`, which codes
against `edbt` (`docs/python_api.md`). `edbt.API_VERSION` follows semver
(NFR-14); 1.0.0 is the first stable contract (M4).

## Unreleased

### M4: Python API 1.0.0 and throughput

- **[API] 1.0.0 for `market-making-sim`.** The full section 10 surface:
  `Strategy`, `Context`, `BookView`, `Config`, `run_backtest`, `Backtest`
  (stepping), `run_live` (validates; sessions arrive in M5), `load_result`,
  `BacktestResult` with Polars tables, the enums, dataclasses and errors, and
  `edbt._edbt` stubs. Contract in `docs/python_api.md`.
- Python `strategies/` (noop, mm_simple, taker_imbalance); the Python and
  Rust `mm_simple` produce byte-identical run files on fixture data.
- `edbt bench`, criterion benchmarks, `edbt-study throughput`.
- Wheels workflow (Linux manylinux and Windows, abi3) with a clean-venv
  quickstart.

### M3: simulated exchange and risk

- Order manager with the FR-07 state machine, cancel-replace and fills
  applied to the ledger before `on_fill`.
- Simulated exchange: aggressive matching with a consumed-liquidity shadow,
  GTC/IOC/FOK/post-only and market orders, self-trade prevention, queue
  models `none`, `risk_averse`, `prob_power`, `prob_log`, `l3_fifo`,
  trade-through and book-cross fills, point-in-time fees and rebates.
- Feed and order latency models (`recorded`, `constant`, `lognormal`,
  `empirical`, `interpolated`).
- `edbt-risk`: pre-trade checks, rate limits and the kill switch, run inside
  order entry in every mode.
- Rust `mm_simple` and `taker_imbalance`.
- `edbt data bbo` derives canonical bbo and checks it against a source bbo.

### M2: core engine, books, ledger, event log

- Deterministic event loop, L2/L3 books, `ReplaySource`, fixed-point ledger
  with per-event reconciliation, event log, run directory with run hash,
  `edbt run`, `replay-check`, `log inspect|verify`.

### M1: scaffolding, CI and data-access smoke test

- Cargo workspace with every crate of spec section 6; `edbt` CLI skeleton.
- Canonical Parquet readers and writers for `trades`, `book_l2` and
  `book_l3` with schema validation (DQ-01), point-in-time instruments
  (DQ-07) and tick/lot conversion (DQ-05).
- `edbt data synth` (seeded synthetic L2/L3 market, fixture CSV conversion)
  and `edbt data validate` (DQ-01, DQ-02, DQ-03, DQ-05).
- Testnet host guard and secret redaction (AC-W-12).
- **[API]** Python package `edbt` (distribution `edbt-sim`) with
  `API_VERSION = "0.1.0"` (pre-contract), `Settings`, `edbt-ingest recordings`.
