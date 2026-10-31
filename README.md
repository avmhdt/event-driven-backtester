# Event-driven backtester with realistic execution

> A simulator that models latency, queue position and partial fills, sharing its code path with live trading.

**Status:** planning

| | |
|---|---|
| Track | Quant Developer |
| Estimated effort | 6–8 weeks |
| Stack | Rust or C++ core with a Python API, or Python with Numba |
| Data cost | Free → Paid |
| Strongest for | Quant dev at any systematic shop |

## Requirements

- [ ] An event loop with nanosecond timestamps: market data → strategy → order manager → simulated exchange → fills.
- [ ] Latency models for both the feed and the order path, configurable and measured.
- [ ] A queue-position model for passive orders, partial fills, and fees and rebates.
- [ ] Risk checks (max position, max order rate, kill switch) shared between simulation and live.
- [ ] One strategy interface for backtest, paper trading and an exchange testnet.
- [ ] Deterministic, reproducible runs with a full event log.

## Data

Raw and licensed data live in `data/`, which is git-ignored. Only download scripts and small synthetic fixtures are committed.

| Dataset | Cost | What it gives you |
|---|---|---|
| Your recordings from [crypto-lob-alpha](https://github.com/avmhdt/crypto-lob-alpha), or ITCH from [itch-feed-handler](https://github.com/avmhdt/itch-feed-handler) | Free | The best input is data you already trust. |
| [Tardis.dev](https://tardis.dev) | Paid | Historical crypto L2/L3 for long backtests. |
| [LOBSTER](https://lobsterdata.com) | Free samples | Equity order-level data. |
| [Binance futures testnet](https://testnet.binancefuture.com) | Free | Live order handling with no real money. |
| [hftbacktest (open source)](https://github.com/nkaz001/hftbacktest) | Free | A reference design to compare your queue model against. |

## Success criteria

### Works (correct and reproducible)

- [ ] The same seed and data give byte-identical results.
- [ ] Accounting reconciles on every event: cash plus marked positions equals the P&L ledger.
- [ ] Unit tests cover every fill rule: crossing, partial fills, queue advance on cancels.

### Impresses hirers

- [ ] A backtest-to-live gap study: the same strategy run in simulation and on testnet for weeks, with the difference in fills and P&L quantified.
- [ ] A measurement of how much ignoring queue position or latency inflates P&L.
- [ ] Throughput in millions of events per second, through a Python API that stays fast.

## Questions this project must answer

- How do you model queue position when you only have L2 data?
- Why do vectorized backtests overstate market-making P&L?
- How do you keep the live and simulated code paths from drifting apart?
