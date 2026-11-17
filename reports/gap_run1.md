# Gap study, run 1: closed as a documented failure

The run was the first 21-day testnet window for AC-H-01: `mm_simple` on Deribit testnet BTC_USDC-PERPETUAL and ETH_USDC-PERPETUAL at minimum size, deploy c9c2597, then 8fea7b0 and a15a65c (same strategy and engine).

- **Window:** 2026-11-06 20:10 UTC to 2026-11-17 19:50:25 UTC (263.7 h of the planned 504 h).
- **Bar:** 95% uptime (NFR-11, AC-H-01). Uptime is judged with venue downtime excluded, under the owner's ruling of 2026-11-05.
- **Result: failed.** Our own downtime was 86.3 h against an allowance of about 25 h for the whole 21 days. Even with no further downtime, the window could have ended at about 82.7% at best.
- **Owner decision of 2026-11-17:** close run 1 and start a fresh 21-day window on the same code. Run 2 runs 2026-11-17 19:55 to 2026-12-08 19:55 UTC (`docs/decisions.md`).

## Uptime

Numbers come from `python -m edbt.soak --bar 0.95 --from 2026-11-06T20:10:00Z --to 2026-11-17T19:50:25Z`.

| Series | Uptime excl. venue downtime (judged) | Uptime incl. venue downtime | Up | Our downtime | Venue downtime | Sessions | Stale-feed kills |
|---|---|---|---|---|---|---|---|
| BTC_USDC-PERPETUAL | 66.77% | 65.74% | 173.3 h | 86.28 h | 4.06 h | 27 | 23 |
| ETH_USDC-PERPETUAL | 67.09% | 66.76% | 176.0 h | 86.33 h | 1.32 h | 28 | 24 |

## Where our downtime went

These are gaps between sessions with no venue downtime logged, plus time inside sessions when a stream was down or a book out of sync. The two series are listed as BTC / ETH.

| Cause | When (UTC) | Our downtime |
|---|---|---|
| Host down: the PC went down with D: drive failures; WSL was off | 2026-11-12 19:03 to 2026-11-15 17:45 | **70.70 h** |
| PC crash; Windows rebooted, WSL stayed down until the owner restarted it | 2026-11-11 07:00 to 19:40 | 12.66 h |
| The owner's ISP outage | 2026-11-07 13:32 to 16:25 | 109.0 / 109.2 min |
| Move of the project and WSL from D: to G: | 2026-11-17 18:10 to 19:00 | 46.6 min |
| Network timeouts at session start, with the venue reading open | 2026-11-10 about 14:26–14:33 (ETH also 17:09–17:14) | 4.0 / 8.3 min |
| Short restarts after stale-feed kills, mostly the daily 08:00 settlement | 22 gaps per series | 11.3 / 10.1 min |
| Not up inside sessions | | 4.1 / 4.0 min |
| **Total** | | **86.28 / 86.33 h** |

The WSL boot records give the host outages. The run's code would not have changed either of the two largest causes: no process on the host could run while it was down.

## Venue downtime (excluded from the judged figure)

| Series | When (UTC) | Reported by the venue |
|---|---|---|
| BTC | 2026-11-10 17:03 to 20:37 | `BTC_USDC-PERPETUAL halted` |
| ETH | 2026-11-10 19:48 to 20:37 | `ETH_USDC-PERPETUAL locked` |
| Both | 2026-11-12 13:12 to 13:25 | HTTP 502 |
| Both | 2026-11-17 13:44 to 14:00 | HTTP 502 |

## Trading

| Series | Orders (finished sessions) | Fills | Venue mismatches | Sweep closes | Carryover P&L, net of fees | Carryover fees |
|---|---|---|---|---|---|---|
| BTC_USDC-PERPETUAL | 226,781 | 1,741 | 0 | 14 | +6.187 USDC | 0.460 |
| ETH_USDC-PERPETUAL | 236,755 | 1,118 | 3 | 16 | +0.222 USDC | 0.015 |

- **Counts.** AC-H-01's counts (at least 2,000 orders and 200 fills) were met for both symbols.
- **Unfinished sessions.** Four sessions per series never finished:
  - two were cut by the host outages;
  - one was in progress during the move;
  - one was the last run-1 session, ended by SIGTERM at the close.

  They have no ledger, fills or summary, so their fills (counted from `status.json`) and their P&L are in no session total.
- **Carryover after an unfinished session.** The start sweep's close is priced against the last *finished* session's mark, so carryover absorbs the price move across the outage. The +6.19 USDC on BTC is mostly the session of 2026-11-15 17:45 (+7.90 USDC), priced against the mark of 2026-11-12 13:11.
- **Venue mismatches.** ETH's 3 mismatches came from the session of 2026-11-10 14:15 to 14:26, which had 24 transport errors. The next start sweep closed the leftover position.

## Findings carried into run 2

- **Inventory pinned at the cap.** With a one-tick spread, the quote that would reduce inventory crosses the touch and is rejected as post-only. The strategy is unchanged by the owner's decision (`docs/decisions.md`, 2026-11-07).
- **Daily 08:00 UTC settlement.** The feed stalls for more than 30 s, the stale-feed kill ends the session, and the next one starts about 30 s later: about 0.7 min a day counted against us.
- **SIGTERM on the Python path.** The gap configs run their strategy through the Python `edbt live` path, which installs no SIGTERM handler. So the SIGTERM that closed run 1 ended the processes without the session's flatten or output files. The venue positions it left (BTC −10, ETH +10 lots) were closed by run 2's start sweep at 19:55. Until that path handles SIGTERM, a series is stopped with `just gap-stop`, which writes STOP and a kill control file so the session ends through the kill switch.
- **Reports from cron.** The end-of-run report now runs from cron (`deploy/gap_run_report.sh`), not from an app session that a crash can stop. The day-7 report of run 1 was lost that way.
