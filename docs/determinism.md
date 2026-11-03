# Determinism

The same binary, data, config and seed give a byte-identical
`hashes.json.run_hash` (NFR-04, AC-W-01). This page lists what makes that
true and how it is checked.

## Event order

Every event has a key `(ts_ns, rank, seq)` and the engine always processes
the smallest key next (FR-01).

| Rank | Event | Source |
|---|---|---|
| 0 | Exchange-side market data | the event source, already in exchange-time order |
| 1 | Order or cancel reaching the exchange | gateway-scheduled |
| 2 | Exchange response at local time | gateway-scheduled |
| 3 | Market data at local time | one FIFO per feed stream |
| 4 | Strategy timer | strategy-scheduled |

`seq` is one global insertion counter. An event from the source takes its
`seq` when the engine pulls it; everything else takes it when it is
scheduled. Equal `(ts, rank)` keys therefore pop in insertion order, and
nothing depends on how the binary heap breaks ties.

Local market data does not go through the heap: feed latency is made
non-decreasing per stream (`max(previous arrival, ts_exchange + latency)`,
FR-04), so each stream's FIFO is already sorted and only its head competes
with the heap. The property tests in `crates/edbt-core/tests/ordering.rs`
check the order (`pop_order_is_total_and_stable`), causality
(`no_lookahead_strategy_sees_only_past`) and stream order under random
latency (`local_feed_never_reorders`), each over 10,000 random cases.

## Sources of nondeterminism that are excluded

- **Wall clocks.** Simulation crates (`edbt-core`, `edbt-data`, `edbt-book`,
  `edbt-exchange`, `edbt-risk`, `edbt-ledger`, `edbt-strategies`) are linted
  with `clippy.toml` `disallowed-methods` for `SystemTime::now`,
  `Instant::now` and `thread::spawn`. Wall time appears only in `perf.json`
  and the run id, which are not hashed.
- **Hash-map order.** `std::collections::HashMap` and `HashSet` are
  disallowed types in those crates; they use `BTreeMap`, `BTreeSet` and
  vectors.
- **Randomness.** One `run.seed`; each component draws from its own ChaCha8
  stream seeded with `blake3(seed || label)` for the labels `latency.feed`,
  `latency.order`, `queue` and `strategy` (FR-17), so a new draw in one
  component never shifts another. `rand_core` and `rand_chacha` are pinned to
  the 0.9 line.
- **Floating point.** Prices and quantities are integer ticks and lots after
  ingest (DQ-05) and the ledger is `i128` nano-quote (NFR-05). The samplers
  use `libm` (`ln`, `exp`, `sqrt`, `cos`), not the platform libm.
- **Threads.** The engine is single-threaded.

## Byte-stable outputs

- Parquet files use fixed writer properties: zstd level 3, `created_by =
  "edbt"`, dictionary encoding and statistics on.
- The event log is postcard records compressed in fixed 1 MiB zstd frames
  (level 3), one frame at a time on one thread.
- `summary.json` has sorted keys and holds no wall-clock or host data;
  money is written as decimal strings with nine places beside float mirrors.
- `config.resolved.toml` is the config with every default filled in, in
  field order. Its BLAKE3 hash is the config hash.

`hashes.json` holds the BLAKE3 of `config.resolved.toml`, `events.log.zst`,
`fills.parquet`, `ledger.parquet`, `orders.parquet` and `summary.json`, and
`run_hash`, the BLAKE3 of their `name hash` lines in sorted order.

## Checks

- `just verify-ac-w-01` and the CI job `determinism` (Linux and Windows):
  generate 1M synthetic rows, run `configs/backtest_synthetic.toml` (lognormal
  feed latency) twice with one seed and once with another, and compare.
- `edbt replay-check --run DIR` re-runs a run directory from its
  `config.resolved.toml` and compares `run_hash`.
- The CI job `determinism-cross-os` reports whether the Linux and Windows run
  hashes agree (reported, not gating).
