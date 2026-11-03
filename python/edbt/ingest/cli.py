"""The ``edbt-ingest`` command line (spec section 7(e))."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from edbt.config import Settings
from edbt.ingest import recordings as rec

app = typer.Typer(help="Ingest and register market data for edbt.", no_args_is_help=True)


@app.command()
def recordings(
    from_: Annotated[
        list[Path] | None,
        typer.Option("--from", help="Read-only data root(s); default EDBT_EXTERNAL_DATA_ROOTS."),
    ] = None,
    venue: Annotated[str | None, typer.Option(help="Only this venue, e.g. binance-usdm.")] = None,
    symbols: Annotated[str | None, typer.Option(help="Comma-separated symbols.")] = None,
    dates: Annotated[str | None, typer.Option(help="START:END dates, inclusive (YYYY-MM-DD).")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="List what was found; write nothing.")] = False,
    registry: Annotated[Path, typer.Option(help="Dataset registry to update.")] = Path(
        "configs/datasets.toml"
    ),
) -> None:
    """Find sibling-repo canonical data and register it (no copy)."""
    roots = from_ or Settings().external_roots
    if not roots:
        typer.echo("no roots: pass --from or set EDBT_EXTERNAL_DATA_ROOTS", err=True)
        raise typer.Exit(1)
    flt = rec.Filter.parse(venue=venue, symbols=symbols, dates=dates)
    found = [p for root in roots for p in rec.scan(root, flt)]
    for line in rec.format_table(found):
        typer.echo(line)
    if dry_run:
        return
    rec.write_registry(registry, found)
    typer.echo(f"registered {len(found)} partitions in {registry}")


def _not_yet(name: str, milestone: str) -> None:
    typer.echo(f"edbt-ingest {name}: not implemented yet (arrives in {milestone})", err=True)
    raise typer.Exit(2)


@app.command()
def tardis() -> None:
    """Download free Tardis.dev days and convert them (M6)."""
    _not_yet("tardis", "M6")


@app.command()
def lobster() -> None:
    """Convert LOBSTER sample files to canonical book_l3 and trades (M6)."""
    _not_yet("lobster", "M6")


@app.command()
def instruments(
    venue: Annotated[
        str, typer.Option(help="Venue id the entries are written under.")
    ] = "binance-usdm-testnet",
    config: Annotated[Path, typer.Option(help="Live config naming the venue and symbols.")] = Path(
        "configs/live_testnet.toml"
    ),
    out: Annotated[Path, typer.Option(help="Instrument file to append to.")] = Path(
        "configs/instruments.toml"
    ),
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Print the entries; write nothing.")] = False,
) -> None:
    """Pull tick, lot and fee metadata from the testnet (needs BINANCE_TESTNET_* keys)."""
    from edbt.config import Config
    from edbt.ingest import instruments as inst

    cfg = Config.from_toml(config)
    rows = inst.pull(cfg)
    for r in rows:
        typer.echo(inst.entry(venue, r, "today"))
    if dry_run:
        return
    added = inst.append(out, venue, rows)
    typer.echo(f"appended {len(added)} entries to {out}: {', '.join(added) or 'none (already present)'}")


@app.command("hftbt-export")
def hftbt_export() -> None:
    """Export canonical data to hftbacktest's input format (M6)."""
    _not_yet("hftbt-export", "M6")
