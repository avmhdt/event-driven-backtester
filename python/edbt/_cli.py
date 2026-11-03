"""Console-script entry point for the ``edbt`` command installed with the wheel.

Everything runs in the Rust CLI except ``edbt run`` with
``strategy.kind = "python"``, which needs the Python strategy class and so
runs through `edbt.run_backtest` in this process.
"""

from __future__ import annotations

import argparse
import sys
import tomllib
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


def main() -> None:
    """Run the CLI with this process's arguments and exit with its code."""
    argv = sys.argv[1:]
    if argv[:1] == ["run"]:
        code = _python_run(argv[1:])
        if code is not None:
            sys.exit(code)
    sys.exit(cli_main(["edbt", *argv]))


if __name__ == "__main__":
    main()
