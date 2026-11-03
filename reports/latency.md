# Latency

Not measured yet. `just verify-ac-w-05` writes this report from at least 500
order round trips on the Binance USD-M futures testnet (`edbt latency
measure`), with p50/p90/p99/p99.9 for entry, response and feed latency and
the fitted lognormal parameters. It needs a working testnet key (P-10); the
keys first placed in `.env` are refused by the venue (`-2015`, see
`docs/decisions.md`).

Open question for the owner before the fit (DQ-12): the round trip from
this PC to the testnet is about 300 ms, so the bound on the clock-offset
error from the `/fapi/v1/time` midpoint method is about 150 ms, above the
5 ms limit that keeps rows in fits.
