# Live runbook

Paper and testnet sessions (FR-21 to FR-23, FR-33): setup, daily operation,
the kill switch, recovery, and the 21-day gap-study run.

The venue is the Deribit testnet (`live.api = "deribit"`, host
`test.deribit.com` only), the D-02 fallback. Binance Futures Demo Trading is
not available in the owner's region; the Binance adapter stays in the code
(`live.api = "binance_usdm"`) but no config uses it. Testnet only: the host
guard refuses mainnet hosts (any `*.deribit.com` other than
`test.deribit.com`, and the Binance mainnet hosts) at config load and on
every request. Credentials are read only from `DERIBIT_TESTNET_CLIENT_ID` /
`DERIBIT_TESTNET_CLIENT_SECRET` (Binance: `BINANCE_TESTNET_*`).

## Modes

| Mode | Market data | Orders | Keys |
|---|---|---|---|
| `paper` | testnet streams | simulated exchange (configured latency and queue models) | no |
| `testnet` | testnet streams | the testnet venue | yes |

Both run the same strategy, order manager and risk checks; only the gateway
differs. Every session writes a directory with the usual run files
(`orders.parquet`, `fills.parquet`, `ledger.parquet`, `events.log.zst`,
`summary.json`, `hashes.json`) plus:

- `status.json`: rewritten every `live.status_interval_s` (connections, books
  in sync, feed lag, open orders, positions, kill state, fills, drops, clock
  offset, disk).
- `live.json`: at the end (uptime, drops, book gaps, venue reconciliation,
  kill confirmation time, NFR-03 latency, recorder rows).
- `drops.csv`: one line per dropped connection.
- `order_latency.parquet` (testnet).
- `replay_strategy.json` after `edbt log replay-strategy`.

With `live.record = true` (or `--record`), market data goes to the canonical
datasets under `$DATA_ROOT` (`book_l2`, `trades`, `bbo`, `venue=<live.venue>`),
with gap rows (`gaps`), clock samples (`clock`) and latency rows
(`processed/latency/{order,feed}_latency`), in hourly
`part-HH-rN.parquet` files.

## First-time setup (owner)

1. Create an API key on test.deribit.com with trading permission. Put it
   in `.env`: `DERIBIT_TESTNET_CLIENT_ID=...` and
   `DERIBIT_TESTNET_CLIENT_SECRET=...`.
2. `just testnet-ping`: the unsigned `public/get_time` reaches the host.
3. `just instruments-testnet`: appends tick size, minimum amount and the
   venue's commissions (`public/get_instrument`; testnet commissions cannot
   be set by the account) to `configs/instruments.toml` as
   `venue = "deribit-testnet"`. Inverse contracts are refused: the ledger is
   linear.
4. Set `order_qty` in the session configs to the pulled minimum amount.
   Each fill's fee is taken from the venue's trade record.

## Running a session

```bash
edbt live --config configs/paper_testnet.toml --duration 10m --record
edbt live --config configs/live_testnet.toml --duration 10m --record --i-understand-testnet-only
```

From Python, `edbt.run_live(strategy, config, duration_s=600, confirm_testnet=True)`.
`just demo-modes` runs both modes for 10 minutes and replays them.

At the start of a testnet session the open orders of its symbols are
cancelled and, with `risk.flatten_on_kill`, their positions closed, so the
ledger starts from the venue's state. At the end the session cancels
everything through the order manager (`session_end`), waits up to 5 s for
the venue to confirm, then sweeps any order the venue still lists.

## Kill switch

- `edbt kill --session <dir>` writes `<dir>/control`; the session acts on it
  within a second. Ctrl-C or SIGTERM ends the session the normal way.
- Automatic triggers: max loss, max drawdown, stale feed, reject burst
  (FR-15). On a kill the order manager cancels every open order and blocks
  new ones; in testnet mode the driver polls `openOrders` until the venue
  lists none and records the time in `live.json` (`kill_confirm_ms`,
  AC-W-06 asks for 2 s or less).
- With `live.end_on_kill = true` the session then ends: exit code 3 (safe
  to restart), or 4 after `max_loss` / `max_drawdown` (do not restart).
- `echo reset > <dir>/control` re-arms the switch without restarting.

## Recovery

- Dropped connections reconnect with backoff (1 s doubling to 60 s, with
  jitter) and rotate before the venue's 24 h limit. A dropped depth stream
  invalidates the book (a gap batch) until a new snapshot is applied, so the
  strategy does not quote on a stale book. Each drop is a line in
  `drops.csv` and a row in the `gaps` dataset with the same columns as the
  shared crypto capture's, so `scripts/compare_drops.py` can line the two up.
- Venue positions are checked against the ledger every
  `live.reconcile_interval_s`; two mismatching checks in a row count as a
  violation (`venue_recon_violations`).
- A crashed session leaves its partial outputs; the next session's start
  sweep cancels its orders.
- Disk: a warning above `live.disk_warn_pct` used; below `live.min_free_gb`
  free the session kills and stops (R-10).

## Replay (FR-33)

```bash
edbt log replay-strategy --run <session dir>
```

feeds the logged market events, venue responses, ticks and operator commands
back through the strategy and compares every outbound order intent and risk
decision with the log. Paper sessions replay through the simulated exchange
with the same seed. `just verify-ac-h-06` runs it for every gap-study session.

## The 21-day gap study (D-06)

- Two series of daily sessions, BTC (`configs/gap_study_testnet.toml`) and
  ETH (`configs/gap_study_testnet_eth.toml`), mm_simple at minimum size,
  recorded (the sim side of the study replays the venue's own recorded
  book).
- Days 1-7 calibrate, days 8-21 evaluate; no fit ever sees an evaluation day
  (DQ-13).
- Start (after the 24 h soak, `just testnet-soak`):

  ```bash
  just gap-start <commit> <end-utc>
  ```

  Run it from a clone on WSL ext4 (for example `~/build/edbt-src`). It
  clones the commit to `~/deploy/event-driven-backtester`, builds a release
  virtualenv, links the credentials file (`~/.config/edbt/deribit-testnet.env`,
  mode 600), and adds two cron watchdog lines (no sudo). Nothing in the run
  touches `/mnt` paths or a Windows-side process, so a Windows sign-out
  (which once broke the `/mnt` mounts) leaves it running. `deploy/gap_supervisor.sh` runs back-to-back
  24 h sessions until the end time, restarts after exit 0 or 3, stops for
  good after exit 4 (`$DATA_ROOT/live/state/<name>/STOPPED`), and retries
  any other failure after 1, 2, ... 10 minutes, stopping after ten in a row.
  Remove `STOPPED` to let the watchdog start the series again.
  `deploy/edbt-gap@.service` is the systemd alternative (needs sudo).
- The end time must fall before 2026-12-06 (Windows Update paused until
  then; no reboot during the run).
- `just gap-status` prints the latest status files; `just gap-stop` stops
  both series and kills the running sessions.
- Logs: `$DATA_ROOT/live/logs/<name>.log`; sessions:
  `$DATA_ROOT/live/sessions/`.
