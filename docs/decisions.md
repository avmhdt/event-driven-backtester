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
- **2026-11-03 (M3) Order path ordering.** Requests reach the exchange in send
  order (`max(previous arrival, ts_send + entry)`) and responses reach the
  strategy in the order produced, as on one connection, so a cancel can never
  overtake the order it cancels.
- **2026-11-03 (M3) Queue position in micro-lots.** L2 queue models keep the
  quantity ahead in micro-lots and apply the expected share of each cancel
  (no random draw), so `prob_power` and `prob_log` are deterministic and the
  `queue` RNG stream is reserved for future stochastic variants. Level
  changes are applied once per L2 batch (also covering snapshots).
- **2026-11-03 (M3) Trade-through and book-cross fills.** Both fill the whole
  remainder at our price. A book cross counts our consumed-liquidity shadow,
  so liquidity we already took cannot fill a resting remainder again.
- **2026-11-03 (M3) Unknown-side trades.** A trade without a side is
  classified against the exchange BBO before the trade (DQ-10); one that
  stays unknown does not interact with our orders at all.
- **2026-11-03 (M3) Several of our orders at one price.** Each keeps its own
  queue estimate; a trade's quantity is shared out in arrival order so the
  same printed quantity does not fill two of our orders.
- **2026-11-03 (M3) Self-trade prevention.** Our orders are not in the
  replayed book, so STP is checked against our own resting orders that the
  incoming order would cross: `cancel_newest` expires the incoming order
  (`STP`), `cancel_oldest` cancels the resting ones first.
- **2026-11-03 (M3) Risk rejections arrive as callbacks.** A risk reject
  marks the order `Rejected` (`RISK:<rule>`) at once and is delivered to the
  strategy at the same local time, after the current callback, as an order
  update and a risk event. Rejected orders do not use rate tokens. A cancel
  over the cancel-rate limit is not sent and is reported as a risk warning;
  kill-switch cancels bypass the limit.
- **2026-11-03 (M3) Reject burst excludes post-only crosses.** The
  reject-burst kill trigger counts risk rejects and venue rejects except
  `POST_ONLY_WOULD_CROSS`, which Binance reports as an expired order and
  which is the normal outcome of quoting at the touch with latency. Without
  this, `mm_simple` was killed within seconds on synthetic data.
- **2026-11-03 (M3) Stale-feed kill in backtests.** The trigger is generic
  (`kill_stale_feed_ms`, no mode branch). Backtest configs on recorded data
  set it to 0, because recording outages are data gaps that the book-validity
  rule already handles; live configs keep the 2 s default.
- **2026-11-03 (M3) Receive-clock regressions (DQ-03).** The shared crypto
  capture has one row on 2026-11-02 (of 102M) whose `ts_recv` steps back
  0.74 ms while its `batch_id` and the Binance `pu` chain continue: a host
  clock step (see the WSL clock note in the shared notes). Sorting by
  `ts_recv`, as `allow_reorder` would, would break the update chain and
  invalidate the book until the next snapshot. So a step back of at most
  1 s with an increasing producer key (`batch_id`, or `trade_id` for trades)
  keeps file order and is counted as `clock_regressions` in `summary.json`
  and `edbt data validate`; anything else is still a DQ-03 hard failure.
- **2026-11-03 (M3) Reconstructed-BBO check (AC-W-09).** Binance's depth
  stream is aggregated over 100 ms while `bookTicker` is per update, so their
  timestamps never line up. The check compares the book after a depth batch
  with every source `bbo` row whose update id equals the batch's final
  update id (`seq`), where both describe the same venue state:
  `edbt data bbo --compare`.
- **2026-11-03 (M3) Binance instruments.** `configs/instruments.toml` copies
  BTCUSDT, ETHUSDT and SOLUSDT from crypto-lob-alpha's
  `config/instruments.yaml` (tick and lot from `fapi/v1/exchangeInfo`, fees
  2.0/5.0 bps from Binance's USD-M fee page for Regular users, both read on
  2026-11-01) with that source recorded per entry.
- **2026-11-03 (M3) Golden snapshots.** `tests/golden/` holds `summary.json`
  and the hashes of every run file except `config.resolved.toml` (which
  contains temp paths) for three fixture scenarios.
- **2026-11-03 (M4) One unsafe block.** A Python callback's `Context` holds a
  lifetime-erased pointer to the engine's context, set for exactly the
  duration of the callback and cleared by a drop guard; every use checks it,
  so a context or `BookView` kept after its callback raises `EdbtError`
  instead of reading freed state. `edbt-py` is the only crate with unsafe
  code (spec NFR-07); everything else forbids it.
- **2026-11-03 (M4) Depth arrays are small copies.** `BookView.depth()` and
  `depth_ticks()` return read-only NumPy arrays built from the top `levels`
  of the book on each call. A true zero-copy view would have to expose the
  engine's book storage beyond the callback's borrow, which only unsafe code
  could do; copying at most `levels` rows costs well under a microsecond and
  keeps the binding safe. FR-20's "zero-copy depth views" is therefore met in
  spirit (no per-event allocation unless the strategy asks for depth), not
  literally.
- **2026-11-03 (M4) Exact float parsing.** The Python `Config` hands its
  values to Rust as JSON. `serde_json` is built with `float_roundtrip`, after
  the config round-trip property test found a float that came back one ULP
  off without it, which would have changed the config hash.
- **2026-11-03 (M4) Python strategy errors.** The `Strategy` trait gained
  `take_error`; the engine checks it after every event and aborts, and
  `run_backtest` re-raises the original Python exception with its traceback.
- **2026-11-03 (M4) Console script.** `edbt run` on a config with
  `strategy.kind = "python"` is handled by the Python wrapper (it needs the
  Python class); every other command runs in the Rust CLI.
- **2026-11-03 (M4) Benchmarks not published yet.** D-08 says published
  benchmark runs wait until no multi-week capture runs on this PC, and the
  crypto capture's 14-day count started on 2026-11-03. The harness (`edbt
  bench`, criterion benches, `edbt-study throughput`) is merged and was run
  locally to check it works; `reports/throughput.md` holds no numbers until
  the reference run.
- **2026-11-03 (M5) Testnet endpoints and routing.** The USD-M "General Info"
  page (last modified 2026-10-30, read 2026-11-03) gives the testnet REST
  base `https://demo-fapi.binance.com` and WebSocket base
  `wss://demo-fstream.binance.com`. Since 2026-04-23 the WebSocket routes by
  path: depth and bookTicker under `/public`, aggTrade under `/market`, user
  data under `/private` (the "Important WebSocket Change Notice"). The user
  stream page shows `/private/ws/<listenKey>`; the notice also shows a
  `?listenKey=` form, so the path is config (`live.ws_private_path`). Checked
  on 2026-11-03: a 2-minute paper session on the `/public/stream` and
  `/market/stream` routes of `demo-fstream` ran with uptime 0.98, 0 book
  gaps and 0 drops, and its replay reproduced all 652 logged intents. The
  private route waits for a working key (next entry).
- **2026-11-03 (M5) Testnet keys refused.** With the keys in `.env`, both
  `demo-fapi.binance.com` and `testnet.binancefuture.com` answer `-2015
  Invalid API-key, IP, or permissions for action`, even to the key-only
  `POST /fapi/v1/listenKey`; public endpoints answer 200. No order was sent.
  Raised with the owner (P-10). The 24 h soak and the gap study wait for a
  working key.
- **2026-11-03 (M5) Testnet instruments.** Public `exchangeInfo` on
  `demo-fapi` (2026-11-03): BTCUSDT tick 0.10, step 0.0001, minimum notional
  50; ETHUSDT tick 0.01, step 0.001, minimum notional 20; limits 6,000
  request weight per minute, 1,200 orders per minute, 300 orders per 10 s.
  The BTCUSDT step differs from the mainnet value of 0.001. Commission rates
  need the signed `commissionRate`, so `configs/instruments.toml` gets its
  testnet entries from `just instruments-testnet` once a key works; nothing
  is typed in.
- **2026-11-03 (M5) Venue error codes.** From the error-code page and the
  2023-03-22 FOK/GTX notice: a post-only order that would take is refused at
  the REST call with `-5022` (no user-stream event), a FOK that cannot fill
  with `-5021`; both map to the simulator's expiry reasons
  (`POST_ONLY_WOULD_CROSS`, `FOK_NOT_FILLABLE`). Cancels of unknown orders
  (`-2011`, `-2013`) map to `UNKNOWN_ORDER`. Expiry reason `er` 1 and 3 are
  self-trade prevention, 2 an IOC remainder, 9 a market remainder. Orders
  carry `selfTradePreventionMode=EXPIRE_TAKER` (GTC and IOC only), the
  simulator's rule. The signing test uses the documented known-answer
  vector.
- **2026-11-03 (M5) Blocking I/O on threads, rustls with ring.** The adapter
  uses blocking HTTP (ureq) and WebSocket (tungstenite) clients on threads:
  one per market connection, the user stream, one order worker per symbol
  (requests for a symbol reach the venue in send order, as in the
  simulator), a housekeeper and a recorder. TLS is rustls with the ring
  provider and webpki roots, so Windows builds need no NASM or CMake. The
  spec's tokio test server is replaced by `edbt_live::mock`, a threaded mock
  on loopback that generates a seeded synthetic market instead of replaying
  JSON fixtures. It checks API keys and HMAC signatures, refuses crossing
  post-only orders with `-5022`, fills resting orders on synthetic trades,
  and can drop connections and break the depth sequence on purpose. The
  host guard allows `http` and `ws` only for an allowlisted loopback host.
- **2026-11-03 (M5) One engine, logged live inputs.** The engine gained live
  hooks: a source may be `pending` (empty but not finished), and
  `inject_response`, `tick`, `control` and `stop` move it to a wall time at
  least one nanosecond after everything it has processed. Each market
  message gets one effective exchange time (venue time in local-clock terms,
  non-decreasing per stream, no later than receipt, after the engine's
  time), and the driver processes everything due before the next input. The
  log records these inputs (`inject`, `tick`, `control`, added after the
  existing record kinds, so backtest logs and golden hashes are unchanged),
  and `edbt log replay-strategy` feeds them back in the same order (FR-33).
  Every input is later than everything processed before it, so the replay
  reproduces the order exactly.
- **2026-11-03 (M5) Session end.** A session ends with
  `control("session_end")`: the kill switch's cancel-all and order block,
  not counted as a kill-switch event in `summary.json`. With
  `flatten_on_kill` the positions are closed too, so each daily session
  starts flat. Testnet sessions also cancel the symbols' open orders (and
  flatten, with `flatten_on_kill`) at the venue before starting, and sweep
  any order the venue still lists after stopping. `live.end_on_kill` ends a
  session after a kill once the venue confirms no open orders. `edbt live`
  then exits 3 (restart is safe), or 4 after `max_loss` or `max_drawdown`
  (do not restart).
- **2026-11-03 (M5) Unknown order outcomes.** A new-order request with a
  transport error may or may not have reached the venue (HTTP 503 "Unknown
  error" is documented as unknown). The driver cancels it by client id and
  reports it rejected (`TRANSPORT`); a later venue acceptance is ignored
  because the order is already finished.
- **2026-11-03 (M5) Venue reconciliation.** Positions from
  `/fapi/v3/positionRisk` are compared with the ledger every
  `live.reconcile_interval_s`. One mismatch can be a fill in flight, so a
  violation is two consecutive checks with the same nonzero difference.
- **2026-11-03 (M5) Clock offset and DQ-12.** The offset is the midpoint
  estimate from the shortest of three back-to-back `/fapi/v1/time` calls
  each minute (the first call can include a new TLS connection). From this
  PC the round trip to the testnet is about 300 ms, so the bound on the
  error of the offset (half the round trip) is about 150 ms, and the 5 ms
  limit of DQ-12 flags every latency row. The measured offset itself is
  about 3 ms, in line with the local clock being about 1 ms off. No
  threshold is changed: the owner decides whether the uncertainty is the
  round-trip bound (as now) or the NTP error bound of the local clock.
- **2026-11-03 (M5) One process per gap-study symbol.** BTCUSDT and ETHUSDT
  need different minimum sizes, and the `order_qty`, `max_inventory` and
  skew of `mm_simple` are in base units. Two configs and two daily session
  series, rather than a strategy change.
- **2026-11-03 (M5) Unattended runs without sudo.** Installing a systemd
  unit needs a password here, so `deploy/gap_supervisor.sh` runs from a
  cron watchdog every 5 minutes with `flock` (the pattern the vol-surface
  cron uses) and stops at a fixed end time. `deploy/edbt-gap@.service` is
  the systemd alternative.
- **2026-11-05 (M5) Supervisor retries failed starts.** During a Deribit
  testnet maintenance halt (2026-11-04 about 18:09 UTC to 2026-11-05 11:28
  UTC; instrument state `halted`, then `locked` and `settlement`) one BTC
  session start got `11094 internal_server_error`, exited 1, and the
  supervisor stopped that series for good. Exit 1 is now retried after 1,
  2, ... 10 minutes, and only the tenth failure in a row stops the series;
  exit 4 (loss limit) still stops it at once. During a halt each session
  ends after `kill_stale_feed_ms` with no market data, and the supervisor
  starts the next one (the time shows as downtime; no threshold changed).
- **2026-11-05 (M5) Venue downtime is excluded from judged uptime (owner
  decision of 2026-11-05).** A second testnet outage (feed stopped about
  12:25 UTC; `public/auth` answered HTTP 502 from 12:26 to at least 13:11;
  then `public/status` `locked: true` for every index) stopped both soak
  series after ten failed starts. The owner ruled:
  1. "Outage-tolerant, then rerun": the supervisor waits out venue outages
     and logs them as venue downtime instead of counting them as failures,
     then a fresh 24 h soak starts once both markets read open. Code change
     only, no threshold change.
  2. "Exclude it, report it": the uptime figure measures our system while
     the venue is open. Venue-reported downtime is excluded and published
     separately as its own figure. The 99% bar stays.

  Venue downtime is defined strictly: only time the venue itself reports as
  not open (an instrument `state` other than `open` in
  `public/get_order_book`, `public/status` `locked: true` or our index in
  `locked_indices`) or HTTP 5xx replies from the venue. Our process, our
  network and the owner's ISP count against us; so do Deribit JSON-RPC
  errors such as `11094 internal_server_error` that arrive without a 5xx
  status, refused credentials, and the time between the start of an outage
  and the supervisor's next check (up to `kill_stale_feed_ms` plus a
  minute). `edbt venue-status` makes the check (exit 0 open, 5 venue down,
  6 unknown). `deploy/gap_supervisor.sh` runs it before each session and
  every minute while the venue is down, writes each interval to
  `$DATA_ROOT/live/venue_downtime/<name>.csv`, and after an exit 1 counts
  the failure toward the ten-in-a-row stop only if the venue reads open; a
  venue it cannot reach (our network) is retried with the same backoff but
  never stops the series. `python -m edbt.soak` (`just soak-report`)
  reports both figures per series: uptime excluding venue downtime (judged
  against 99%) and including it, with venue and our own downtime in
  minutes. The same applies to the 21-day run.
- **2026-11-03 (M5) Recorder layout.** Hourly `part-HH-rN.parquet` files by
  receive time, `N` the first unused index (a restart in the same hour adds
  a file), so `list_partition_files` orders them like the shared capture's
  files. Gap rows use the columns of the shared capture's `gaps` dataset,
  so `scripts/compare_drops.py` can line up testnet drops with the
  capture's Binance drops. `edbt-live` depends on `edbt-data` for the
  canonical row types; it holds no strategy, order-manager or risk logic.
- **2026-11-03 (M5) Latency measurement.** `edbt latency measure` uses the
  REST channel only (the WebSocket API channel is not implemented):
  post-only buys 5% below the best bid, sized at 1.05 times the minimum
  notional, each cancelled at once, with feed latency from the depth and
  trade streams during the run.
- **2026-11-03 (M5) Venue changed to the Deribit testnet (owner ruling).**
  Binance Futures Demo Trading is not available in the owner's region (the
  Cayman Islands), which explains the `-2015` refusals of both Binance keys;
  no VPN or proxy is allowed. The live venue is the D-02 fallback, the
  Deribit testnet: host `test.deribit.com` only, for REST and WebSocket. The
  host guard refuses `www.deribit.com`, `deribit.com` and every other
  `*.deribit.com` host, as it refuses Binance mainnet. Credentials are read
  only from `DERIBIT_TESTNET_CLIENT_ID` and `DERIBIT_TESTNET_CLIENT_SECRET`.
  D-06 now names BTC and ETH perpetuals on Deribit instead of BTCUSDT and
  ETHUSDT, with mm_simple at minimum size, 7 calibration days and 14
  evaluation days. The Binance adapter stays in the code (`live.api =
  "binance_usdm"`), unused.
- **2026-11-03 (M5) Deribit facts.** From the Deribit API docs read
  2026-11-03 (JSON-RPC reference, OpenAPI and AsyncAPI specs):
  - REST is `https://test.deribit.com/api/v2/<method>`, WebSocket
    `wss://test.deribit.com/ws/api/v2`.
  - Auth is `public/auth` with `client_credentials`, giving an access token
    and a refresh token (each refresh replaces the pair); REST private calls
    carry `Authorization: Bearer`, and a WebSocket auth authenticates that
    connection.
  - `book.<i>.100ms` sends the full book first after each subscription,
    then changes `[action, price, amount]` with `change_id` and
    `prev_change_id`; on a gap the docs advise re-subscribing.
  - `trades.<i>.100ms` and `quote.<i>` are public; `user.orders.<i>.raw`
    and `user.trades.<i>.raw` are private.
  - `public/set_heartbeat` makes the server send `test_request`, which must
    be answered with `public/test` or the connection is closed.
  - A post-only order that would take is repriced by default and rejected
    with `11054` (`post_only_reject`) when `reject_post_only` is set.
  - `private/cancel_by_label` returns the number of orders cancelled.
  - `public/get_instrument` gives tick size, minimum amount, contract size,
    maker and taker commission, and `instrument_type` (`linear` or
    `reversed`).
- **2026-11-03 (M5) What carries over and what is venue-specific.** The
  session driver, live source, depth sync, recorder, replay, risk path, kill
  confirmation and reconciliation are shared. Deribit needs its own:
  - streams: JSON-RPC subscriptions over one WebSocket, heartbeats;
  - book sync: the stream's own full book instead of a REST snapshot, and a
    re-subscription after a `change_id` gap (`first = prev_change_id`, so
    the shared bridging rule applies);
  - REST auth: an OAuth token with refresh instead of HMAC signing;
  - order formatting: `private/buy` and `private/sell` with
    `post_only=true&reject_post_only=true` for GTX, so a would-be taker is
    rejected (expired as `POST_ONLY_WOULD_CROSS`) rather than repriced as
    the venue does by default; cancel by label;
  - the translator: orders by label, fills from user trades, deduplicated by
    trade id;
  - start and end sweeps, and the instrument pull.
  Units differ by instrument:
  - tick size and minimum amount (the amount step) come from
    `public/get_instrument`;
  - amounts are in USD for inverse contracts (contract size 10 USD for
    BTC-PERPETUAL) and in the base coin for linear USDC perpetuals.
  Fees are the instrument's commissions (testnet commissions cannot be set
  by the account); each fill's fee is taken from the venue's trade record.
- **2026-11-03 (M5) Linear USDC perpetuals (owner decision).**
  BTC-PERPETUAL and ETH-PERPETUAL are inverse: amounts in USD; P&L, margin
  and fees in BTC or ETH. The ledger, the simulator's fees and the risk
  notional are linear. The owner chose the linear perpetuals
  BTC_USDC-PERPETUAL and ETH_USDC-PERPETUAL ("Yes, use USDC"), so the
  accounting is unchanged; `edbt-ingest instruments` still refuses to write
  an inverse instrument.
  A public read from test.deribit.com at about 23:40 UTC on 2026-11-03
  (reported by the review session) found:
  - BTC_USDC-PERPETUAL: 100 trades in about 10 minutes, about $1.8B of
    24-hour volume.
  - ETH_USDC-PERPETUAL: 100 trades in about 2.3 hours, about $19M of 24-hour
    volume.
  ETH may give few fills. Fills are reported per symbol (`status.json`,
  `live.json`). If ETH falls short of AC-H-01's fill count, that is reported
  as a finding; symbols are not swapped mid-run.
- **2026-11-03 (M5) Deribit order events.**
  - A user-stream trade can arrive before the REST reply that tells the
    order's size; such fills are held until the size is known (from the
    reply or the order event), so leaves are never computed against an
    unknown size.
  - A confirmed cancel by label does not end the order by itself. The order
    event that follows carries the venue's filled amount, and the order ends
    only once every fill up to that amount has arrived. If the order event
    never comes (user stream down), the order ends 2 s after the confirmed
    cancel; a held end whose fills never arrive is released after 5 s.
  - Without this, a fill made just before a cancel was dropped, and the
    venue position drifted from the ledger; the mock test caught it.
- **2026-11-03 (M5) Deribit mock.** `edbt_live::mock_deribit` speaks the
  same REST methods and JSON-RPC WebSocket on loopback, with the synthetic
  book model of the Binance mock. It checks the mock client credentials and
  bearer token, sends a full book after each subscription, can skip change
  ids and drop connections, and sends `test_request` heartbeats.
  `edbt mock-venue --proto deribit` runs it for the Python tests.
