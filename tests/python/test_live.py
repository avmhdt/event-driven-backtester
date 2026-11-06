"""Live helpers against the local mock venue: the instruments pull (FR-30)
and the console script's live and kill commands."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

import edbt
from conftest import FIXTURES
from edbt import _cli
from edbt.ingest import instruments as inst
from edbt.ingest.cli import app


def _mock_cfg(mock_venue: dict[str, str], tmp_path: Path, mode: str = "paper") -> dict[str, object]:
    return {
        "run": {"mode": mode, "seed": 1},
        "data": {
            "venue": "mock",
            "symbols": ["BTCUSDT", "ETHUSDT"],
            "instruments": str(FIXTURES / "instruments_mock.toml"),
        },
        "risk": {"kill_stale_feed_ms": 5000},
        "strategy": {"kind": "rust", "path": "mm_simple"},
        "live": {
            "venue": "mock",
            "rest_url": mock_venue["rest_url"],
            "ws_market_url": mock_venue["ws_url"],
            "allowed_hosts": ["127.0.0.1"],
            "record": False,
            "session_dir": str(tmp_path / "live"),
            "tick_interval_ms": 200,
            "status_interval_s": 1,
        },
    }


def test_rate_to_bps_is_exact() -> None:
    assert inst.rate_to_bps("0.0002") == 2.0
    assert inst.rate_to_bps("0.00045") == 4.5
    assert inst.rate_to_bps("-0.0001") == -1.0


def test_pull_and_append_instruments(
    mock_venue: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "mock-api-key")
    monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "mock-api-secret")
    cfg = edbt.Config.from_dict(_mock_cfg(mock_venue, tmp_path))
    rows = inst.pull(cfg)
    assert [r["symbol"] for r in rows] == ["BTCUSDT", "ETHUSDT"]
    assert rows[0]["tick_size"] == 0.1 and rows[0]["maker_rate"] == "0.0002"
    out = tmp_path / "instruments.toml"
    assert inst.append(out, "mock-pulled", rows, "2026-11-03") == ["BTCUSDT", "ETHUSDT"]
    assert inst.append(out, "mock-pulled", rows, "2026-11-03") == []
    text = out.read_text()
    assert "maker_bps = 2.0" in text and "taker_bps = 5.0" in text and "min_notional = 5.0" in text
    # The appended file loads as an instrument table.
    cfg2 = cfg.with_overrides(
        {"data.venue": "mock-pulled", "data.instruments": str(out), "run.mode": "backtest"}
    )
    assert cfg2.to_dict()["data"]["venue"] == "mock-pulled"
    # The CLI command: dry run prints, real run appends to --out.
    from typer.testing import CliRunner

    cfg_path = tmp_path / "live.toml"
    cfg_path.write_text(cfg.to_toml())
    res = CliRunner().invoke(app, ["instruments", "--venue", "m2", "--config", str(cfg_path), "--dry-run"])
    assert res.exit_code == 0, res.output
    assert 'venue = "m2"' in res.output
    res = CliRunner().invoke(
        app, ["instruments", "--venue", "m2", "--config", str(cfg_path), "--out", str(out)]
    )
    assert res.exit_code == 0 and "appended 2" in res.output


def test_pull_without_keys_is_refused(
    mock_venue: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
    monkeypatch.chdir(tmp_path)  # no .env here
    with pytest.raises(edbt.EdbtError, match="BINANCE_TESTNET"):
        inst.pull(edbt.Config.from_dict(_mock_cfg(mock_venue, tmp_path)))


def _main(monkeypatch: pytest.MonkeyPatch, *argv: str) -> int:
    monkeypatch.setattr(sys, "argv", ["edbt", *argv])
    with pytest.raises(SystemExit) as e:
        _cli.main()
    code = e.value.code
    assert isinstance(code, int)
    return code


def test_console_script_live_and_kill(
    mock_venue: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    d = _mock_cfg(mock_venue, tmp_path)
    d["strategy"] = {"kind": "python", "path": "edbt.examples.quickstart", "params": {}}
    path = tmp_path / "paper.toml"
    path.write_text(edbt.Config.from_dict(d).to_toml())
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "data"))
    assert _main(monkeypatch, "live", "--config", str(path), "--duration", "3s", "--record") == 0
    sessions = sorted((tmp_path / "live").iterdir())
    assert len(sessions) == 1 and (sessions[0] / "status.json").exists()
    assert _cli._seconds("2m") == 120.0 and _cli._seconds("5") == 5.0
    # `edbt kill` writes the control file the running session polls.
    assert _main(monkeypatch, "kill", "--session", str(sessions[0])) == 0
    assert (sessions[0] / "control").read_text() == "kill"


@pytest.mark.parametrize(
    ("ended", "code"),
    [(None, 0), ("stale_feed", 3), ("reject_burst", 3), ("max_loss", 4), ("max_drawdown", 4)],
)
def test_python_live_exit_code_matches_rust(tmp_path: Path, ended: str | None, code: int) -> None:
    # The gap-study supervisor restarts after 0 or 3 and stops after 4.
    (tmp_path / "live.json").write_text(json.dumps({"ended_by_kill": ended}))
    assert _cli._live_exit_code(tmp_path) == code
