r"""Uptime of a gap-study series over a window: the soak and 21-day reports.

Owner ruling of 2026-11-05: the judged uptime measures our system while
the venue is open. Venue downtime is only what the venue
itself reports (instrument not open, index locked) or HTTP 5xx from it, as
logged by ``deploy/gap_supervisor.sh`` in
``$DATA_ROOT/live/venue_downtime/<name>.csv``; it is excluded from that
figure and reported on its own. Everything else (our process, our network,
the ISP) counts against us. Both figures are reported:

- ``uptime_excl_venue = up / (window - venue_down)``: the judged figure.
- ``uptime_incl_venue = up / window``.

The bar (``--bar``) is 99% for the M5 24 h soak and 95% for the 21-day gap
study (NFR-11, AC-H-01). Trades closed by the start or shutdown sweep are
summed from each session's ``carryover_*`` fields (``sweep_fills.parquet``).

``up`` is each session's ``uptime_fraction`` (every stream up and every book
in sync) times its time inside the window.

    python -m edbt.soak --from 2026-11-06T00:00:00Z --to 2026-11-07T00:00:00Z \\
        --series btc=BTC_USDC-PERPETUAL --series eth=ETH_USDC-PERPETUAL
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import tomllib
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SOAK_BAR = 0.99
GAP_STUDY_BAR = 0.95


def parse_utc(text: str) -> float:
    """Epoch seconds of an RFC 3339 UTC time with up to nanosecond digits."""
    m = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?Z", text.strip())
    if m is None:
        raise ValueError(f"not a UTC time: {text!r}")
    base = datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC).timestamp()
    frac = m.group(2) or "0"
    return base + int(frac) / 10.0 ** len(frac)


def _overlap(a: float, b: float, lo: float, hi: float) -> float:
    return max(0.0, min(b, hi) - max(a, lo))


@dataclass
class SeriesReport:
    """One series (symbol) over one window."""

    name: str
    symbol: str
    start: float
    end: float
    sessions: int = 0
    up_s: float = 0.0
    venue_down_s: float = 0.0
    venue_intervals: list[tuple[str, str, str]] = field(default_factory=list)
    kills: Counter[str] = field(default_factory=Counter)
    drops: int = 0
    fills: int = 0
    recon_violations: int = 0
    bar: float = SOAK_BAR
    carryover_pnl: float = 0.0
    carryover_fees: float = 0.0
    carryover_trades: int = 0
    carryover_unpriced: int = 0

    @property
    def window_s(self) -> float:
        """Window length, seconds."""
        return max(self.end - self.start, 0.0)

    @property
    def uptime_excl_venue(self) -> float:
        """Uptime with venue downtime excluded (the judged figure)."""
        open_s = self.window_s - self.venue_down_s
        return self.up_s / open_s if open_s > 0 else 1.0

    @property
    def uptime_incl_venue(self) -> float:
        """Uptime over the whole window."""
        return self.up_s / self.window_s if self.window_s > 0 else 1.0

    @property
    def our_down_s(self) -> float:
        """Time down while the venue was open (counts against us)."""
        return max(self.window_s - self.venue_down_s - self.up_s, 0.0)

    @property
    def passes(self) -> bool:
        """Uptime excluding venue downtime meets the bar."""
        return self.uptime_excl_venue >= self.bar

    def to_dict(self) -> dict[str, Any]:
        """JSON-ready summary."""
        return {
            "name": self.name,
            "symbol": self.symbol,
            "window_s": round(self.window_s, 1),
            "sessions": self.sessions,
            "up_s": round(self.up_s, 1),
            "venue_down_s": round(self.venue_down_s, 1),
            "our_down_s": round(self.our_down_s, 1),
            "uptime_excl_venue": round(self.uptime_excl_venue, 6),
            "uptime_incl_venue": round(self.uptime_incl_venue, 6),
            "bar": self.bar,
            "passes": self.passes,
            "venue_downtime": [{"start": a, "end": b, "reason": r} for a, b, r in self.venue_intervals],
            "kills": dict(self.kills),
            "drops": self.drops,
            "fills": self.fills,
            "venue_recon_violations": self.recon_violations,
            "carryover_pnl": self.carryover_pnl,
            "carryover_fees": self.carryover_fees,
            "carryover_trades": self.carryover_trades,
            "carryover_unpriced": self.carryover_unpriced,
        }


def _session_symbols(run: Path) -> list[str]:
    try:
        cfg = tomllib.loads((run / "config.resolved.toml").read_text())
    except (OSError, tomllib.TOMLDecodeError):
        return []
    syms = cfg.get("data", {}).get("symbols", [])
    return [str(s) for s in syms]


def _session_facts(run: Path) -> dict[str, Any] | None:
    """`live.json` of a finished session, or `status.json` of a running one."""
    for name, end_key in (("live.json", "ended_utc"), ("status.json", "now_utc")):
        p = run / name
        if p.exists():
            try:
                d: dict[str, Any] = json.loads(p.read_text())
            except (OSError, json.JSONDecodeError):
                continue
            d["_end"] = d.get(end_key)
            return d
    return None


def series_report(
    name: str,
    symbol: str,
    start: float,
    end: float,
    sessions_dir: Path,
    downtime_dir: Path,
    state_dir: Path | None = None,
    now: float | None = None,
    bar: float = SOAK_BAR,
) -> SeriesReport:
    """Uptime of series `name` (trading `symbol`) over [start, end)."""
    end = min(end, now) if now is not None else end
    r = SeriesReport(name=name, symbol=symbol, start=start, end=end, bar=bar)
    runs = sorted(p for p in sessions_dir.iterdir() if p.is_dir()) if sessions_dir.exists() else []
    for run in runs:
        if symbol not in _session_symbols(run):
            continue
        d = _session_facts(run)
        if d is None or not d.get("started_utc") or not d.get("_end"):
            continue
        s, e = parse_utc(d["started_utc"]), parse_utc(d["_end"])
        inside = _overlap(s, e, start, end)
        if inside <= 0:
            continue
        r.sessions += 1
        r.up_s += float(d.get("uptime_fraction", 0.0)) * inside
        if d.get("ended_by_kill"):
            r.kills[str(d["ended_by_kill"])] += 1
        r.drops += sum(int(v) for v in (d.get("drops") or {}).values())
        r.fills += int((d.get("fills_by_symbol") or {}).get(symbol, 0))
        r.recon_violations += int(d.get("venue_recon_violations", 0))
        r.carryover_pnl += float(d.get("carryover_pnl", 0.0))
        r.carryover_fees += float(d.get("carryover_fees", 0.0))
        r.carryover_trades += int(d.get("carryover_trades", 0))
        r.carryover_unpriced += int(d.get("carryover_unpriced", 0))
    intervals: list[tuple[float, float, str]] = []
    path = downtime_dir / f"{name}.csv"
    if path.exists():
        with path.open(newline="") as f:
            for row in csv.DictReader(f):
                intervals.append((parse_utc(row["start_utc"]), parse_utc(row["end_utc"]), row["reason"]))
    # An outage still in progress (the supervisor's open interval).
    if state_dir is not None and (state_dir / name / "venue_down").exists():
        first = (state_dir / name / "venue_down").read_text().split(" ", 1)
        intervals.append((float(first[0]), end, (first[1] if len(first) > 1 else "").strip() + " (ongoing)"))
    for a, b, why in intervals:
        inside = _overlap(a, b, start, end)
        if inside > 0:
            r.venue_down_s += inside
            r.venue_intervals.append((_fmt(max(a, start)), _fmt(min(b, end)), why))
    return r


def _fmt(t: float) -> str:
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def report_md(reports: list[SeriesReport]) -> str:
    """Markdown table of the series reports."""
    bar = reports[0].bar if reports else SOAK_BAR
    lines = [
        f"| Series | Window h | Uptime excl. venue downtime (judged, bar {bar:.0%}) "
        "| Uptime incl. venue downtime | Venue downtime min | Our downtime min "
        "| Sessions | Drops | Fills | Recon violations "
        "| Sweep closes | Carryover P&L | Carryover fees |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in reports:
        lines.append(
            f"| {r.name} ({r.symbol}) | {r.window_s / 3600:.2f} | {r.uptime_excl_venue:.4%} "
            f"{'pass' if r.passes else 'FAIL'} | {r.uptime_incl_venue:.4%} | {r.venue_down_s / 60:.1f} "
            f"| {r.our_down_s / 60:.1f} | {r.sessions} | {r.drops} | {r.fills} | {r.recon_violations} "
            f"| {r.carryover_trades} ({r.carryover_unpriced} unpriced) | {r.carryover_pnl:+.6f} "
            f"| {r.carryover_fees:.6f} |"
        )
    for r in reports:
        for a, b, why in r.venue_intervals:
            lines.append(f"\n- {r.name} venue downtime {a} to {b}: {why}")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    """`python -m edbt.soak`: print the report; exit 0 if every series passes."""
    root = Path(os.environ.get("DATA_ROOT", Path.home() / "data" / "event-driven-backtester"))
    p = argparse.ArgumentParser(prog="python -m edbt.soak", description=__doc__.split("\n\n")[0])
    p.add_argument("--from", dest="start", required=True, help="window start, UTC")
    p.add_argument("--to", dest="end", required=True, help="window end, UTC (capped at now)")
    p.add_argument(
        "--series", action="append", required=True, help="name=SYMBOL, e.g. btc=BTC_USDC-PERPETUAL"
    )
    p.add_argument("--data-root", type=Path, default=root)
    p.add_argument("--bar", type=float, default=SOAK_BAR, help="judged uptime bar: 0.99 soak, 0.95 gap study")
    p.add_argument("--json", action="store_true", help="print JSON instead of Markdown")
    a = p.parse_args(argv)
    live = a.data_root / "live"
    now = datetime.now(UTC).timestamp()
    reports = []
    for s in a.series:
        name, _, symbol = s.partition("=")
        reports.append(
            series_report(
                name,
                symbol,
                parse_utc(a.start),
                parse_utc(a.end),
                live / "sessions",
                live / "venue_downtime",
                live / "state",
                now,
                a.bar,
            )
        )
    if a.json:
        print(json.dumps([r.to_dict() for r in reports], indent=2))
    else:
        print(report_md(reports), end="")
    return 0 if all(r.passes for r in reports) else 1


if __name__ == "__main__":
    raise SystemExit(main())
