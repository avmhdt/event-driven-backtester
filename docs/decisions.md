# Decisions

Owner decisions (D-xx), the owner-level rulings that apply to this project,
pinned versions, and choices made by the build agent where the spec left
room. Newest agent entries are appended at the end of their section.

## Owner decisions (spec section 3)

| ID | Decision | Answer | Source |
|---|---|---|---|
| D-01 | Country and Binance testnet eligibility | Allowed. Country of residence is the Cayman Islands (`OWNER_COUNTRY=KY`). Binance spot and USD-M market data and the USD-M futures testnet are allowed; OKX is not allowed and is not used. Build M1–M4 without the testnet; in M5 test the adapter against the local mock venue first, then connect once P-10 keys are in `.env` and `just testnet-ping` succeeds. | Owner, 2026-10-31 |
| D-02 | Fallback testnet venue | Not needed while the Binance testnet is reachable. Only if it becomes unreachable or is retired: Bybit testnet as a second gateway, after the owner confirms eligibility. | Spec default |
| D-03 | PyPI name and publishing | Distribution `edbt-sim`, import name `edbt`. Not published; wheels are built in CI only. | Owner, 2026-11-03 (task brief) |
| D-04 | Paid Tardis.dev budget | Not approved. Sibling-repo recordings, Tardis free first-of-month days and synthetic data only. | Owner, `DECISIONS.md` (Money: everything else USD 0) |
| D-05 | Host for the 21-day testnet run | The owner's always-on Windows 11 Pro PC, under WSL2. Downtime is logged and excluded. | Owner, 2026-10-31 |
| D-06 | Gap-study design | Spec default: 21 calendar days, BTCUSDT and ETHUSDT on testnet, `mm_simple` at minimum size, first 7 days calibration and the rest evaluation. The run's plan and timing must be agreed with the owner before M5 ends, because other long captures share the PC. | Spec default; timing open |
| D-07 | Licence | MIT OR Apache-2.0 (code only; licensed data is never redistributed). | Owner, 2026-11-03 (task brief) |
| D-08 | Reference hardware | The owner's machine under WSL2; every number reports CPU, RAM and OS. Published benchmark runs happen only when no multi-week capture is running on this PC. | Owner, `DECISIONS.md` (Machine) |
| D-09 | Docs site | README memo, `docs/*.md` and `cargo doc`; no site. | Spec default |

## Owner rulings from `DECISIONS.md` that apply here

Copied at M1 (2026-11-03) from the owner-level `DECISIONS.md`, which wins
over the spec where they differ.

- **Plan.** Project #11 is in the third batch (2026-11-03) with
  `llm-text-alpha`, `stat-arb-pairs` and `regime-portfolio-construction`.
- **Merging.** Agents push feature branches and open one PR per milestone.
  PRs merge automatically once CI is green, with
  `gh pr merge --auto --squash` run as `avmhdt`. Branch protection and
  auto-merge are turned on once CI exists (M1).
- **Identity.** `gh` runs as `avmhdt` with admin on the repo; Actions is
  enabled. No AI attribution anywhere in commits, PRs, code or docs.
- **Testnet only.** Binance testnet is allowed; mainnet stays hard-blocked
  (host guard, FR-21); OKX is not used.
- **Data.** Downloads approved: Binance public data, Tardis free samples and
  Nasdaq ITCH samples. LOBSTER stays manual (the owner accepts its terms);
  samples are at `~/data/low-latency-matching-engine/raw/lobster` (AAPL and
  MSFT, 2012-06-21, 10 levels). Raw and row-level data stay out of git.
- **Shared crypto capture.** Mainnet market data is the capture recorded by
  `crypto-lob-alpha` at `~/data/crypto-lob-alpha` (book_l2, bbo, trades from
  2026-11-01), read in place via `EDBT_EXTERNAL_DATA_ROOTS`. It is still
  being written: read closed hourly files only and never write, lock or
  move anything there. ITCH `book_l3` is at `~/data/itch-feed-handler/book_l3`
  (50 symbols, 2019-01-30 and two more days).
- **Money.** Everything not listed in the owner's budget table is USD 0, so
  no paid Tardis and no VPS.
- **Machine.** Live recorders run on this PC (the crypto capture's 14-day
  count started 2026-11-03, a Deribit cron and Tardis downloads). Heavy runs
  use at most 8 threads; agents never restart WSL, reboot or edit
  `.wslconfig`. Data and build trees live on WSL ext4, never on `/mnt/*` or
  `C:`, and never in git.
- **Benchmarks.** Published benchmark runs for project 11 happen only when no
  multi-week capture is running on this PC.
- **Accepted.** Every other spec default stands.

## Pinned toolchain and library versions (M1, 2026-11-03)

Exact versions are locked in `Cargo.lock` and `uv.lock`; this table records
the set chosen at M1. Cargo uses version ranges with the MSRV-aware resolver
(`resolver = "3"`, `rust-version = "1.85"`), so the lock holds the newest
release of each crate that still builds on Rust 1.85.

| Component | Version | Note |
|---|---|---|
| Rust (local) | 1.99.1 | MSRV 1.85, tested in CI on stable and 1.85 |
| arrow-array / arrow-schema / parquet | 59.3.2 | same major; 60.x needs Rust 1.88 |
| pyo3 | 0.30.1 | `abi3-py312` |
| rand_core / rand_chacha | 0.9.5 / 0.9.0 | pinned to the 0.9 line so RNG streams cannot change under an upgrade (R-06) |
| libm | 0.2 | portable `exp`/`ln`/`sqrt` for samplers |
| zstd | 0.13.4 | |
| toml | 1.1.8 | |
| clap | 4.6.7 | |
| Python (local) | 3.12.15 via uv 0.12.23 | CI also runs 3.13 |
| numpy / polars / pyarrow | 2.5.4 / 2.1.0 / 26.0.1 | |
| pydantic-settings / typer | 2.16.2 / 0.27.3 | |
| pytest / hypothesis / mypy / ruff | 9.2.0 / 6.170.14 / 2.4.1 / 0.17.3 | |
| just / cargo-nextest / cargo-llvm-cov / cargo-deny | 1.59.0 / 0.9.150 / 0.9.1 / 0.20.2 | local tools |

## Venue facts checked

| Date | Fact | Evidence |
|---|---|---|
| 2026-11-03 | Unsigned `GET /fapi/v1/time` answers HTTP 200 on both testnet REST families, `https://testnet.binancefuture.com` (RTT 809 ms) and `https://demo-fapi.binance.com` (RTT 995 ms), from this PC. Reachability only; the URLs used for trading are verified against the current docs in M5. | `just testnet-ping`, `results/testnet_ping.csv` |

## Agent choices

- **2026-11-03 (M1) Crate layering.** `edbt-book`, `edbt-risk` and
  `edbt-ledger` are leaf crates with primitive-integer interfaces;
  `edbt-core` depends on them and owns the domain types (`Side`,
  `OrderRequest`, ...). This keeps the spec's crate list without a separate
  types crate and lets each leaf be tested on its own.
- **2026-11-03 (M1) Lints.** The workspace lint table denies `unsafe_code`,
  `missing_docs`, `unwrap_used`, `expect_used`, `float_cmp` and the NFR-04
  disallowed wall-clock, thread and hash-map APIs (`clippy.toml`). `edbt-live`,
  `edbt-cli` and `edbt-py` carry their own table without the NFR-04 bans,
  since they are not simulation crates; `edbt-py` relaxes `unsafe_code` to
  `deny` because PyO3 macros expand to unsafe code. Tests may unwrap.
- **2026-11-03 (M1) `edbt-py` is not built by `cargo test`.** The extension is
  built by maturin (`uv sync`); Rust tests run with `--exclude edbt-py`, and
  clippy covers it with `PYO3_PYTHON` set to the uv 3.12 interpreter.
- **2026-11-03 (M1) Extension build profile in CI.** CI builds the extension
  with `MATURIN_PEP517_ARGS=--profile dev` for speed; release builds are used
  for wheels and all local performance numbers.
- **2026-11-03 (M1) Dev venv location.** The justfile sets
  `UV_PROJECT_ENVIRONMENT=~/build/event-driven-backtester/venv` so the venv
  lives on WSL ext4, not in the repo on `/mnt/d`.
- **2026-11-03 (M1) Console script.** The `edbt` binary is also installed by
  the wheel as a console script: `edbt._cli:main` calls the Rust CLI through
  `edbt._edbt.cli_main`, so one wheel carries both.
- **2026-11-03 (M1) `trade_id` as integer.** Readers keep `trades.trade_id`
  as `u64` (venue ids are decimal); a non-numeric id is replaced by a stable
  63-bit BLAKE3 hash.
- **2026-11-03 (M1) Optional producer columns.** The `book_l2` reader also
  reads `seq_prev` (Binance `pu`) when present, for DQ-04 gap detection; the
  shared crypto capture writes it. Other extra columns are ignored (DQ-01).
- **2026-11-03 (M1) Part-file order.** Files in a date partition are read in
  `(hour, restart)` order for `part-HH.parquet` and `part-HH-rN.parquet`, as
  the shared crypto capture names them after a restart, then by name.
- **2026-11-03 (M1) `l3_price_unit`.** `configs/instruments.toml` may give
  `l3_price_unit`, the producer's integer price unit for `book_l3.price_ticks`
  (XNAS: 0.0001 USD), so `price_ticks / (tick_size / l3_price_unit)` is the
  engine tick. Default: the tick size.
- **2026-11-03 (M1) Fees as micro-basis-points.** Fee rates are stored as
  integers in units of 1e-6 bps; a rate that is not a multiple of 1e-6 bps is
  rejected at load, so fee arithmetic stays integer.
- **2026-11-03 (M1) Synthetic venue.** `edbt data synth` simulates an
  order-level market (adds near the touch, cancels, partial cancels,
  replaces, market orders) and writes it either as `book_l3` or aggregated to
  `book_l2` batches, both with `trades`, under venue `synthetic`. The L2
  stream carries `seq_prev`, so gap detection uses the same `pu` rule as
  Binance. Symbol `SYN` holds the L2 fixtures and `SYN3` the L3 fixtures. Its
  fees (maker -0.5 bps, taker 2 bps) are the generator's own convention.
- **2026-11-03 (M1) Open files in sibling roots.** `edbt-ingest recordings`
  lists only Parquet files not modified for 120 s, because the live capture
  may still be writing the current hour.
- **2026-11-03 (M1) `testnet-ping`.** Implemented as `scripts/testnet_ping.py`
  (standard library only). It refuses mainnet and non-allowlisted hosts with
  the same rule as the Rust guard before connecting, and appends to
  `results/testnet_ping.csv`.
- **2026-11-03 (M1) `rustfmt.toml`.** `max_width = 120` with
  `use_small_heuristics = "Max"`.
- **2026-11-03 (M2) Scheduler layout.** Exchange-side market data comes
  straight from the source (its `seq` is taken when the engine pulls it),
  local market data waits in one FIFO per feed stream, and everything else
  sits in a binary heap; the engine takes the smallest `(ts, rank, seq)`
  across them. This is the spec's total order with fewer heap operations.
- **2026-11-03 (M2) Feed latency per message.** Feed latency is sampled once
  per venue message (an L2 batch, a trade, an L3 event), so the rows of one
  batch arrive together; `recorded` latency is `ts_recv` minus the effective
  `ts_exchange`, which reproduces the recorded receive time.
- **2026-11-03 (M2) Tie order between streams.** At equal effective exchange
  time the replay emits a symbol's trades before its book rows, so a trade
  and the depth update that reflects it reach the queue models in causal
  order.
- **2026-11-03 (M2) Gap rule.** A `book_l2` batch is a gap (DQ-04) when the
  producer's `seq_prev` does not equal the previous batch's `seq`. The first
  incremental batch after a snapshot is not checked (Binance's first diff
  after a snapshot only has to straddle `lastUpdateId`). Without `seq_prev`
  there is no gap detection.
- **2026-11-03 (M2) Rejected L2 rows.** An off-grid `book_l2` row (DQ-05) is
  counted and kept as a `NOOP` row so its batch boundaries still apply.
- **2026-11-03 (M2) Warm-up.** Rows from `run.start - data.warmup_max` are
  replayed into both books without strategy callbacks; the strategy's
  `on_start` runs at the first event at or after `run.start`. A book is
  valid only after a snapshot (DQ-11).
- **2026-11-03 (M2) Mark price.** `ledger.mark = "mid"` marks at the local
  mid, rounded half-even to a whole tick so the ledger stays integer; the
  mark only changes on valid books.
- **2026-11-03 (M2) Ledger rows.** `ledger.parquet` gets a row per symbol at
  each `ledger.mark_interval` boundary crossed (stamped at the boundary,
  describing the state before the event that crossed it), after every fill
  (M3) and at the end of the run. Empty intervals do not produce rows.
- **2026-11-03 (M2) Event-log seq.** `seq` in the event log, `orders.parquet`,
  `fills.parquet` and `ledger.parquet` is the engine's record counter. Under
  `event_log = "orders_only"` market records are skipped but still counted,
  so the seqs in the other files stay comparable.
- **2026-11-03 (M2) Money columns.** Nano-quote amounts are `int64` in the
  run Parquet files (up to about 9.2e9 quote units) and the writer fails on
  overflow rather than wrapping.
- **2026-11-03 (M2) Synthetic generator fix.** The fixture cross-check
  (`crates/edbt-data/tests/fixtures_l3_l2.rs`) found that the generator did
  not reduce a level's total when an execution or cancel emptied an order,
  so its `book_l2` output overstated sizes. Fixed before any golden hash
  existed, and the committed fixtures were regenerated (`just fixtures`).
