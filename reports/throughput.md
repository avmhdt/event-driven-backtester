# Throughput

The reference measurement for AC-H-03 is not published yet. D-08 makes the
owner's machine under WSL2 the reference, and published benchmark runs wait
until no multi-week capture is running on it; the crypto capture's 14-day
count started on 2026-11-03.

The harness is in place: `just study-throughput` builds the release `edbt`,
runs `edbt bench` (Rust engine-only and end-to-end from Parquet) and the
Python modes (callback with the BBO-change subscription, callback on every
update, stepping API), five repetitions each, and rewrites this page,
`reports/tables/throughput.csv` and `reports/hardware.md` with the median,
the interquartile range and the hardware.
