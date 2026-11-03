# Latency models

The simulator separates three delays (FR-04, FR-05). Code in
`crates/edbt-exchange/src/latency.rs`; random draws come from the
`latency.feed` and `latency.order` streams of the run seed (FR-17).

## Feed latency (`[latency.feed]`)

Exchange time to local arrival of market data. Sampled once per venue
message (an L2 batch, a trade, an L3 event), so all rows of a batch arrive
together. Local arrival is made non-decreasing per stream,
`max(previous arrival, ts_exchange + latency)`, so the feed never reorders.

| `model` | Latency |
|---|---|
| `recorded` | `ts_recv - ts_exchange` from the data, clamped below at `min_ns`. Reproduces the recorded receive times. On this PC it includes the host clock offset (about 165 ms fast against NTP on 2026-11-01). |
| `constant` | `constant_ns`. `0` is the zero-latency preset of the inflation study. |
| `lognormal` | `exp(mu + sigma * z)` ns, `z` standard normal (Box-Muller with `libm`). |
| `empirical` | Uniform draws from `latency_ns` in a measured `feed_latency` file. |

## Order latency (`[latency.order]`)

Entry is local send to arrival at the exchange; response is exchange event
to local arrival of the acknowledgement or fill. Requests reach the exchange
in send order and responses reach the strategy in the order produced, as on
one connection.

| `model` | Latency |
|---|---|
| `constant` | `entry_ns`, `response_ns`. |
| `lognormal` | `exp(entry_mu + entry_sigma * z)` and `exp(response_mu + response_sigma * z)` ns. |
| `empirical` | A measured round trip drawn at random from an `order_latency` file: its entry for the request and its response for the replies. |
| `interpolated` | Entry and response interpolated linearly by send time between measured round trips (in the spirit of hftbacktest's interpolated order latency); the nearest measurement outside the measured range. |

An order can never fill before `ts_send + entry`
(`order_not_fillable_before_arrival`, and the property test
`fill_not_before_arrival`).

## Checks

`crates/edbt-exchange/tests/latency_models.rs` draws 100,000 samples from the
lognormal feed and order models and from the empirical model and compares
them with their distribution by a Kolmogorov-Smirnov test (p > 0.01,
AC-W-05). Measurement on the testnet and fitting (`edbt latency measure`,
`edbt latency fit`) arrive in M5.
