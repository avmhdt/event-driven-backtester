# Latency

Source: Deribit testnet (test.deribit.com), every `deribit-testnet` row under /home/ze/data/event-driven-backtester/processed/latency/order_latency/venue=deribit-testnet: `edbt latency measure` round trips and the orders of live sessions.

Rows: 0 new-order rows used, 1163 flagged rows excluded (negative after clamping, or clock-offset uncertainty above 5 ms, DQ-12), 0 outside the fit window.

| Series | n | p50 ms | p90 ms | p99 ms | p99.9 ms |
|---|---|---|---|---|---|
| Order entry (local send to venue time) | 0 | 0.000 | 0.000 | 0.000 | 0.000 |
| Order response (venue time to local receipt) | 0 | 0.000 | 0.000 | 0.000 | 0.000 |
| Order round trip | 0 | 0.000 | 0.000 | 0.000 | 0.000 |
| Feed (venue time to local receipt) | 0 | 0.000 | 0.000 | 0.000 | 0.000 |

All new-order rows, including those DQ-12 flags (shown while the owner's ruling on the clock-offset uncertainty is open; none of these are fitted):

| Series (all rows) | n | p50 ms | p90 ms | p99 ms | p99.9 ms |
|---|---|---|---|---|---|
| Order entry | 660 | 120.893 | 129.074 | 182.891 | 1174.619 |
| Order response | 660 | 112.397 | 123.334 | 128.389 | 714.480 |
| Order round trip | 660 | 233.727 | 250.602 | 272.164 | 1294.672 |
| Feed | 7777 | 173.048 | 339.424 | 435.631 | 629.222 |

Venue times have millisecond precision, so entry and response latencies below about 1 ms are not resolved; entry and response each include the remaining clock offset error.

## Fitted parameters

```toml
# Fitted by `edbt latency fit` (lognormal of ns).
[latency.order]
model = "lognormal"
```
