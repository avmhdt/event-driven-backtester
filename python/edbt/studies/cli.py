"""The ``edbt-study`` command line (studies arrive in M4 to M6)."""

from __future__ import annotations

import typer

app = typer.Typer(help="Regenerate edbt studies and reports.", no_args_is_help=True)

_STUDIES = {
    "gap": "M6",
    "inflation": "M6",
    "queue-validation": "M6",
    "vectorized": "M6",
    "throughput": "M4",
    "hftbt-compare": "M6",
    "all": "M6",
}


def _make(name: str, milestone: str) -> None:
    def cmd() -> None:
        typer.echo(f"edbt-study {name}: not implemented yet (arrives in {milestone})", err=True)
        raise typer.Exit(2)

    cmd.__doc__ = f"Run the {name} study ({milestone})."
    app.command(name)(cmd)


for _name, _ms in _STUDIES.items():
    _make(_name, _ms)
