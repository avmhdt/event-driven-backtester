# Gap study, run 2: closed as a documented failure

The run was the second 21-day testnet window for AC-H-01: `mm_simple` on Deribit testnet BTC_USDC-PERPETUAL and ETH_USDC-PERPETUAL at minimum size, deploy a15a65c.

- **Window:** 2026-11-17 19:55 UTC to 2026-11-25 00:03:30 UTC (172.1 h of the planned 504 h).
- **Bar:** 95% uptime (NFR-11, AC-H-01). Uptime is judged with venue downtime excluded, under the owner's ruling of 2026-11-05.
- **Result: failed.** Our own downtime was 43.0 h against an allowance of about 25 h for the whole 21 days. Even with no further downtime, the window could have ended at about 91.4% at best.
- **Owner decision of 2026-11-24:** close run 2 and start run 3, a fresh 21 days on the same code: 2026-11-25 00:05 UTC to 2026-12-16 00:05 UTC (`docs/decisions.md`).
- **How it was closed:** through the kill switch (STOP files and a `kill` control file), not SIGTERM. Both last sessions ended with exit 3, flattened (final positions 0) and left no open orders.

## Uptime

Numbers come from `python -m edbt.soak --bar 0.95 --from 2026-11-17T19:55:00Z --to 2026-11-25T00:03:30Z`.

| Series | Uptime excl. venue downtime (judged) | Uptime incl. venue downtime | Up | Our downtime | Venue downtime | Sessions | Stale-feed kills |
|---|---|---|---|---|---|---|---|
| BTC_USDC-PERPETUAL | 74.64% | 73.54% | 126.6 h | 43.01 h | 2.54 h | 30 | 25 |
| ETH_USDC-PERPETUAL | 74.65% | 73.55% | 126.6 h | 42.99 h | 2.54 h | 29 | 24 |

The run's day-1 (99.85% on both) and day-7 reports passed. The days 1–7 report shows the start of the miss.

## Where our downtime went

These are gaps between sessions with no venue downtime logged, plus time inside sessions when a stream was down or a book out of sync. The two series are listed as BTC / ETH.

| Cause | When (UTC) | Our downtime |
|---|---|---|
| Drive G: failed writes; WSL's virtual disk reported corrupted, down until the owner's repair | 2026-11-20 23:24 to 11-21 17:00 | **17.6 h** |
| WSL down again during the repair | 2026-11-21 17:10 to 18:55 | 1.7 h |
| WSL shut down on purpose to protect G: until it was fixed | 2026-11-21 20:21 to 11-22 19:10 | **22.8 h** |
| WSL restart after the G: fix | 2026-11-24 20:24 to 20:40 | 15.7 min |
| Network timeouts at session start, with the venue reading open | 2026-11-20 18:16 to 18:31 | 15.1 / 13.9 min |
| Short restarts after stale-feed kills, mostly the daily 08:00 settlement | 37–38 gaps per series | 15.1 / 15.7 min |
| Not up inside sessions | | 5.9 / 5.7 min |
| **Total** | | **43.01 / 42.99 h** |

Almost all of it (42.4 h) is the G: failure and its repair. The run's code would not have changed that: no process on the host could run while WSL was down.

## Venue downtime (excluded from the judged figure)

The same intervals apply to both series, within a few seconds. In total, 2.54 h.

| When (UTC) | Reported by the venue |
|---|---|
| 2026-11-24 10:37 to 12:36 | HTTP 502 on `get_order_book`, `status` and `auth` |
| 2026-11-18 12:58 to 13:11 | HTTP 502 |
| 2026-11-19 14:05 to 14:12 | HTTP 502 |
| Ten short halts or locks, 1 to 2 minutes each, on 11-18 to 11-20 | `halted` or `locked`, with the index locked in `public/status` |

## Trading

| Series | Orders (finished sessions) | Fills | Venue mismatches | Sweep closes | Carryover P&L, net of fees | Carryover fees |
|---|---|---|---|---|---|---|
| BTC_USDC-PERPETUAL | 243,720 | 1,537 | 51 | 23 | −1.391 USDC | 0.719 |
| ETH_USDC-PERPETUAL | 233,889 | 965 | 165 | 22 | −0.180 USDC | 0.018 |

- **Counts.** AC-H-01's counts (at least 2,000 orders and 200 fills) were met for both symbols.
- **Unfinished sessions.** Four sessions per series never finished, because they were cut by the outages. Their fills and P&L are in no session total.
- **Carryover after an unfinished session.** The start sweep's close is priced against the last finished session's mark, so carryover absorbs the price move across the outage. That accounts for most of BTC's −1.39 USDC.
- **Venue mismatches, not explained.** All of them came from three sessions on 2026-11-19:
  - BTC and ETH from 13:11 to 14:04, with 51 of 52 checks each;
  - ETH from 14:12 to 16:08, with 114 of 116.

  Those sessions' own ledger reconciliations were clean, and later sessions show none. The logs don't record the venue-versus-ledger detail, so the cause is not yet known.

## Findings carried into run 3

- **Pinned inventory** at the ±10-lot cap: unchanged by the owner's decision of 2026-11-07.
- **Daily 08:00 settlement:** the feed stalls and the next session starts about 30 s later.
- **Open: the 11-19 venue mismatches.** Diagnosing them needs the sessions' event logs.
- **Report job.** The job now takes the run's name from `run.env`. It had not marked windows that missed the bar as written, so it rewrote them every 15 minutes; that is fixed.
