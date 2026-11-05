"""Soak and gap-study uptime: venue downtime excluded and included."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from edbt import soak

T0 = soak.parse_utc("2026-11-06T00:00:00Z")


def _session(
    root: Path, name: str, symbol: str, start: str, end: str, uptime: float, **extra: object
) -> None:
    d = root / "live" / "sessions" / name
    d.mkdir(parents=True)
    (d / "config.resolved.toml").write_text(f'[data]\nsymbols = ["{symbol}"]\n')
    facts = {"started_utc": start, "ended_utc": end, "uptime_fraction": uptime, **extra}
    (d / "live.json").write_text(json.dumps(facts))


def _report(root: Path, name: str, symbol: str, hours: float, now: float | None = None) -> soak.SeriesReport:
    live = root / "live"
    return soak.series_report(
        name, symbol, T0, T0 + hours * 3600, live / "sessions", live / "venue_downtime", live / "state", now
    )


def test_parse_utc_nanoseconds() -> None:
    assert soak.parse_utc("2026-11-06T00:00:01.500000000Z") == T0 + 1.5
    with pytest.raises(ValueError, match="not a UTC time"):
        soak.parse_utc("2026-11-06 00:00:00")


def test_venue_downtime_is_excluded_and_reported(tmp_path: Path) -> None:
    sym = "BTC_USDC-PERPETUAL"
    # 10 h up at 99.9%, a 4 h venue outage, then 10 h up at 99.9%.
    _session(
        tmp_path,
        "a",
        sym,
        "2026-11-06T00:00:00Z",
        "2026-11-06T10:00:00Z",
        0.999,
        ended_by_kill="stale_feed",
        drops={"depth": 2},
        fills_by_symbol={sym: 30},
    )
    b_end = "2026-11-07T00:00:00Z"
    _session(tmp_path, "b", sym, "2026-11-06T14:00:00Z", b_end, 0.999, fills_by_symbol={sym: 20})
    # Another series' session is ignored.
    _session(tmp_path, "c", "ETH_USDC-PERPETUAL", "2026-11-06T00:00:00Z", "2026-11-07T00:00:00Z", 0.5)
    dt = tmp_path / "live" / "venue_downtime"
    dt.mkdir(parents=True)
    (dt / "btc.csv").write_text(
        "start_utc,end_utc,seconds,reason\n"
        '2026-11-06T10:00:00Z,2026-11-06T14:00:00Z,14400,"BTC_USDC-PERPETUAL locked"\n'
    )
    r = _report(tmp_path, "btc", sym, 24)
    assert r.sessions == 2 and r.fills == 50 and r.drops == 2 and dict(r.kills) == {"stale_feed": 1}
    assert r.venue_down_s == 4 * 3600
    assert r.uptime_excl_venue == pytest.approx(0.999)
    assert r.uptime_incl_venue == pytest.approx(0.999 * 20 / 24)
    assert r.passes
    assert r.our_down_s == pytest.approx(20 * 3600 * 0.001)
    md = soak.report_md([r])
    assert "99.9000% pass" in md and "venue downtime 2026-11-06T10:00:00Z to 2026-11-06T14:00:00Z" in md
    assert r.to_dict()["venue_downtime"][0]["reason"] == "BTC_USDC-PERPETUAL locked"


def test_our_downtime_counts_and_ongoing_outage(tmp_path: Path) -> None:
    sym = "ETH_USDC-PERPETUAL"
    # Up 20 h; 1 h missing on our side; then an outage still in progress.
    _session(tmp_path, "a", sym, "2026-11-06T00:00:00Z", "2026-11-06T20:00:00Z", 1.0)
    state = tmp_path / "live" / "state" / "eth"
    state.mkdir(parents=True)
    (state / "venue_down").write_text(f"{T0 + 21 * 3600:.0f} {sym} halted\n")
    r = _report(tmp_path, "eth", sym, 24, now=T0 + 22 * 3600)
    assert r.window_s == 22 * 3600
    assert r.venue_down_s == 3600
    assert r.our_down_s == pytest.approx(3600)
    assert r.uptime_excl_venue == pytest.approx(20 / 21)
    assert not r.passes
    assert r.venue_intervals[0][2] == f"{sym} halted (ongoing)"


def test_running_session_uses_status_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    sym = "BTC_USDC-PERPETUAL"
    d = tmp_path / "live" / "sessions" / "run"
    d.mkdir(parents=True)
    (d / "config.resolved.toml").write_text(f'[data]\nsymbols = ["{sym}"]\n')
    status = {
        "started_utc": "2026-11-01T00:00:00Z",
        "now_utc": "2026-11-01T12:00:00Z",
        "uptime_fraction": 0.995,
    }
    (d / "status.json").write_text(json.dumps(status))
    argv = ["--from", "2026-11-01T00:00:00Z", "--to", "2026-11-01T12:00:00Z", "--series", f"btc={sym}"]
    assert soak.main([*argv, "--data-root", str(tmp_path), "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out[0]["uptime_excl_venue"] == pytest.approx(0.995) and out[0]["passes_99"]
