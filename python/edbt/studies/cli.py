"""The ``edbt-study`` command line (studies arrive in M4 to M6)."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(help="Regenerate edbt studies and reports.", no_args_is_help=True)

_LATER = {
    "gap": "M6",
    "inflation": "M6",
    "queue-validation": "M6",
    "vectorized": "M6",
    "hftbt-compare": "M6",
    "all": "M6",
}


def _make(name: str, milestone: str) -> None:
    def cmd() -> None:
        typer.echo(f"edbt-study {name}: not implemented yet (arrives in {milestone})", err=True)
        raise typer.Exit(2)

    cmd.__doc__ = f"Run the {name} study ({milestone})."
    app.command(name)(cmd)


for _name, _ms in _LATER.items():
    _make(_name, _ms)


@app.command()
def throughput(
    config: Annotated[Path | None, typer.Option(help="configs/studies/throughput.toml")] = None,
    out: Annotated[Path, typer.Option(help="Report directory.")] = Path("reports"),
) -> None:
    """Events per second through the Rust and Python paths (FR-27)."""
    from edbt.studies import throughput as study

    for r in study.run(config, out):
        typer.echo(f"{r.mode:24} {r.strategy:10} {r.median:14,.0f} events/s (IQR {r.iqr:,.0f})")
