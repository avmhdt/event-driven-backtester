"""deploy/gap_supervisor.sh with a fake `edbt` that plays a scripted venue.

Each plan line is `<command> <exit code> [output]` for the next call of the
fake (`venue-status` or `live`); when the plan runs out, the next `live`
call writes the STOP file so the supervisor exits.
"""

from __future__ import annotations

import csv
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from conftest import REPO

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("flock") is None, reason="needs bash and flock"
)

FAKE = """#!/usr/bin/env bash
line=$(head -n 1 "$FAKE_PLAN")
sed -i 1d "$FAKE_PLAN"
if [ -z "$line" ]; then
  [ "$1" = live ] && touch "$FAKE_STOP"
  [ "$1" = venue-status ] && echo open
  exit 0
fi
read -r want code rest <<<"$line"
[ "$want" = "$1" ] || echo "expected $want, got $1" >>"$FAKE_PLAN.err"
[ -n "$rest" ] && echo "$rest"
exit "$code"
"""


def _run(tmp_path: Path, plan: list[str]) -> tuple[str, Path]:
    deploy = tmp_path / "deploy"
    (deploy / ".venv-run" / "bin").mkdir(parents=True)
    fake = deploy / ".venv-run" / "bin" / "edbt"
    fake.write_text(FAKE)
    fake.chmod(0o755)
    data = tmp_path / "data"
    state = data / "live" / "state" / "t"
    (tmp_path / "plan").write_text("".join(f"{p}\n" for p in plan))
    env = {
        **os.environ,
        "EDBT_DEPLOY": str(deploy),
        "DATA_ROOT": str(data),
        "EDBT_SUPERVISOR_MINUTE_S": "0",
        "FAKE_PLAN": str(tmp_path / "plan"),
        "FAKE_STOP": str(state / "STOP"),
    }
    script = REPO / "deploy" / "gap_supervisor.sh"
    argv = ["bash", str(script), "t", "cfg.toml", "2099-01-01T00:00:00Z"]
    subprocess.run(argv, env=env, timeout=60, check=False)
    assert not (tmp_path / "plan.err").exists(), (tmp_path / "plan.err").read_text()
    return (data / "live" / "logs" / "t.log").read_text(), state


def _downtime(tmp_path: Path) -> list[dict[str, str]]:
    with (tmp_path / "data" / "live" / "venue_downtime" / "t.csv").open(newline="") as f:
        return list(csv.DictReader(f))


def test_waits_out_a_venue_outage_and_logs_it(tmp_path: Path) -> None:
    down = "venue-status 5 down: BTC_USDC-PERPETUAL halted"
    log, state = _run(tmp_path, [down, down, "venue-status 0 open", "live 3"])
    assert log.count("session start") == 2  # the scripted one and the one that stops
    assert "venue down (BTC_USDC-PERPETUAL halted)" in log and "venue open again" in log
    rows = _downtime(tmp_path)
    assert len(rows) == 1 and rows[0]["reason"] == "BTC_USDC-PERPETUAL halted"
    assert not (state / "STOPPED").exists()


def test_failed_start_during_venue_downtime_is_not_counted(tmp_path: Path) -> None:
    plan = ["venue-status 0 open", "live 1"] + ["venue-status 5 down: public/auth: HTTP 502"] * 2
    plan += ["venue-status 0 open"]
    log, state = _run(tmp_path, plan)
    assert "exit 1 while the venue is down: not counted" in log
    assert _downtime(tmp_path)[0]["reason"] == "public/auth: HTTP 502"
    assert not (state / "STOPPED").exists()


def test_ten_failures_with_the_venue_open_stop_the_series(tmp_path: Path) -> None:
    plan = ["venue-status 0 open", "live 1", "venue-status 0 open"] * 10
    log, state = _run(tmp_path, plan)
    assert "10 failures in a row with the venue open: not restarting" in log
    assert (state / "STOPPED").read_text().strip() == "1"


def test_our_network_is_retried_but_never_stops(tmp_path: Path) -> None:
    plan = ["venue-status 6 unknown: x", "live 1", "venue-status 6 unknown: x"] * 12
    log, state = _run(tmp_path, plan)
    assert log.count("our side, retrying") == 12 and "counts against us" in log
    assert not (state / "STOPPED").exists()
    assert not (tmp_path / "data" / "live" / "venue_downtime" / "t.csv").exists()


def test_loss_limit_stops_at_once(tmp_path: Path) -> None:
    log, state = _run(tmp_path, ["venue-status 0 open", "live 4"])
    assert "exit 4 (loss limit): not restarting" in log
    assert (state / "STOPPED").read_text().strip() == "4"
