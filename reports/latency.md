# Latency

Not measured yet. `just verify-ac-w-05` writes this report from at least 500
order round trips on the Deribit testnet (`edbt latency measure`), with
p50/p90/p99/p99.9 for entry, response and feed latency and the fitted
lognormal parameters. It runs once the owner confirms the Deribit testnet
key.

Open question for the owner before the fit (DQ-12): the clock-offset error
bound of the midpoint method is half the round trip to the venue (about
150 ms to the Binance testnet from this PC; to be measured for Deribit),
above the 5 ms limit that keeps rows in fits.
