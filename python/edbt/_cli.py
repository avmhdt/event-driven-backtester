"""Console-script entry point for the ``edbt`` command installed with the wheel.

Everything runs in the Rust CLI except the commands that need a Python
strategy class when ``strategy.kind = "python"``: ``edbt run``, ``edbt live``
and ``edbt log replay-strategy`` run in this process.
"""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path
from typing import Any

from edbt._edbt import cli_main


def _value(text: str) -> Any:
    """A `--set` value as TOML, or the bare string."""
    try:
        return tomllib.loads(f"v = {text}")["v"]
    except tomllib.TOMLDecodeError:
        return text


def _python_run(argv: list[str]) -> int | None:
    """Run `edbt run` here when the config names a Python strategy."""
    p = argparse.ArgumentParser(prog="edbt run", add_help=False)
    p.add_argument("--config", required=True)
    p.add_argument("--seed", type=int)
    p.add_argument("--out")
    p.add_argument("--set", action="append", default=[])
    p.add_argument("--no-log-experiment", action="store_true")
    try:
        args, _ = p.parse_known_args(argv)
    except SystemExit:
        return None
    import edbt
    from edbt.strategy import load_strategy

    cfg = edbt.Config.from_toml(args.config)
    overrides: dict[str, Any] = {}
    for kv in args.set:
        key, _, val = kv.partition("=")
        overrides[key.strip()] = _value(val.strip())
    if args.seed is not None:
        overrides["run.seed"] = args.seed
    cfg = cfg.with_overrides(overrides)
    if cfg.to_dict()["strategy"]["kind"] != "python":
        return None
    res = edbt.run_backtest(
        load_strategy(cfg), cfg, out_dir=args.out, log_experiment=not args.no_log_experiment
    )
    s = res.summary
    print(f"run_id     {res.run_id}")
    print(f"out_dir    {res.out_dir}")
    rate, calls = res.perf.events_per_s, res.perf.python_callbacks
    print(f"events     {s.n_events} ({rate:.0f} events/s, {calls} callbacks)")
    print(f"orders     {s.n_orders}  fills {s.n_fills}  rejects {s.n_rejects}")
    print(f"pnl        {s.pnl_total:.9f}  fees {s.fees_total:.9f}")
    print(f"recon      {s.reconciliation_violations} violations, book gaps {s.book_gaps}")
    print(f"run_hash   {res.run_hash}")
    return 0


def _python_live(argv: list[str]) -> int | None:
    """Run `edbt live` here when the config names a Python strategy."""
    p = argparse.ArgumentParser(prog="edbt live", add_help=False)
    p.add_argument("--config", required=True)
    p.add_argument("--duration")
    p.add_argument("--record", action="store_true")
    p.add_argument("--i-understand-testnet-only", action="store_true")
    try:
        args, _ = p.parse_known_args(argv)
    except SystemExit:
        return None
    import edbt
    from edbt.strategy import load_strategy

    cfg = edbt.Config.from_toml(args.config)
    if cfg.to_dict()["strategy"]["kind"] != "python":
        return None
    if args.record:
        cfg = cfg.with_overrides({"live.record": True})
    duration = None if args.duration is None else _seconds(args.duration)
    res = edbt.run_live(
        load_strategy(cfg), cfg, duration_s=duration, confirm_testnet=args.i_understand_testnet_only
    )
    s = res.summary
    print(f"session    {res.run_id}")
    print(f"out_dir    {res.out_dir}")
    print(f"orders     {s.n_orders}  fills {s.n_fills}  rejects {s.n_rejects}")
    print(f"pnl        {s.pnl_total:.9f}  fees {s.fees_total:.9f}")
    print(f"recon      {s.reconciliation_violations} violations")
    print(f"run_hash   {res.run_hash}")
    return 0


def _seconds(text: str) -> float:
    """`24h`, `10m`, `30s` or plain seconds."""
    units = {"h": 3600.0, "m": 60.0, "s": 1.0}
    if text and text[-1] in units:
        return float(text[:-1]) * units[text[-1]]
    return float(text)


def _python_replay(argv: list[str]) -> int | None:
    """Run `edbt log replay-strategy` here for a Python strategy."""
    p = argparse.ArgumentParser(prog="edbt log replay-strategy", add_help=False)
    p.add_argument("--run", required=True)
    try:
        args, _ = p.parse_known_args(argv)
    except SystemExit:
        return None
    import edbt
    from edbt import _edbt
    from edbt.strategy import load_strategy

    run = Path(args.run)
    cfg = edbt.Config.from_toml(run / "config.resolved.toml")
    if cfg.to_dict()["strategy"]["kind"] != "python":
        return None
    params = cfg.to_dict()["strategy"].get("params", {})
    rep = _edbt.replay_strategy(str(run), load_strategy(cfg), params)
    print(f"intents    live {rep['live']}  replayed {rep['replayed']}")
    if rep["matched"]:
        print("replay     100% of logged intents reproduced")
        return 0
    print(f"replay     first difference: {rep['first_mismatch']}")
    return 1


def main() -> None:
    """Run the CLI with this process's arguments and exit with its code."""
    argv = sys.argv[1:]
    handlers = {"run": _python_run, "live": _python_live}
    code: int | None = None
    if argv[:1] and argv[0] in handlers:
        code = handlers[argv[0]](argv[1:])
    elif argv[:2] == ["log", "replay-strategy"]:
        code = _python_replay(argv[2:])
    if code is not None:
        sys.exit(code)
    sys.exit(cli_main(["edbt", *argv]))


if __name__ == "__main__":
    main()
