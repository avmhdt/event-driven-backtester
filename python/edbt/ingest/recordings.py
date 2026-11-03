"""Discovery and registration of sibling-repo canonical data.

Sibling roots are read in place and never written (spec section 16). Only
closed files are listed: a Parquet file modified within the last
``OPEN_FILE_GRACE_S`` seconds may still be open for writing by a live
recorder and is skipped.
"""

from __future__ import annotations

import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path

DATASETS = ("trades", "book_l2", "book_l3", "bbo")
OPEN_FILE_GRACE_S = 120.0


@dataclass(frozen=True, slots=True)
class Filter:
    """Which partitions to include."""

    venue: str | None = None
    symbols: frozenset[str] | None = None
    start: str | None = None
    end: str | None = None

    @classmethod
    def parse(cls, *, venue: str | None, symbols: str | None, dates: str | None) -> Filter:
        """Build a filter from CLI strings; ``dates`` is ``START:END`` or one date."""
        syms = frozenset(s.strip() for s in symbols.split(",") if s.strip()) if symbols else None
        start = end = None
        if dates:
            start, _, end = dates.partition(":")
            end = end or start
        return cls(venue=venue, symbols=syms, start=start, end=end)

    def accepts(self, venue: str, symbol: str, date: str) -> bool:
        """Whether one partition passes the filter."""
        if self.venue is not None and venue != self.venue:
            return False
        if self.symbols is not None and symbol not in self.symbols:
            return False
        if self.start is not None and date < self.start:
            return False
        return not (self.end is not None and date > self.end)


@dataclass(frozen=True, slots=True)
class Partition:
    """One date partition of a canonical dataset."""

    root: Path
    dataset: str
    venue: str
    symbol: str
    date: str
    files: int
    bytes: int
    skipped_open: int


def _kv(path: Path, key: str) -> str | None:
    name = path.name
    return name[len(key) + 1 :] if name.startswith(key + "=") else None


def scan(root: Path, flt: Filter, *, now: float | None = None) -> list[Partition]:
    """List date partitions under ``root`` that pass ``flt``, sorted."""
    now = time.time() if now is None else now
    out: list[Partition] = []
    for dataset in DATASETS:
        ds = root / dataset
        if not ds.is_dir():
            continue
        for vdir in sorted(ds.iterdir()):
            venue = _kv(vdir, "venue")
            if venue is None or not vdir.is_dir():
                continue
            for sdir in sorted(vdir.iterdir()):
                enc = _kv(sdir, "symbol")
                if enc is None or not sdir.is_dir():
                    continue
                symbol = urllib.parse.unquote(enc)
                for ddir in sorted(sdir.iterdir()):
                    date = _kv(ddir, "date")
                    if date is None or not flt.accepts(venue, symbol, date):
                        continue
                    files = sorted(ddir.glob("*.parquet"))
                    closed = [f for f in files if now - f.stat().st_mtime >= OPEN_FILE_GRACE_S]
                    out.append(
                        Partition(
                            root=root,
                            dataset=dataset,
                            venue=venue,
                            symbol=symbol,
                            date=date,
                            files=len(closed),
                            bytes=sum(f.stat().st_size for f in closed),
                            skipped_open=len(files) - len(closed),
                        )
                    )
    return out


def format_table(parts: list[Partition]) -> list[str]:
    """Human-readable listing, one line per partition."""
    lines = [f"{'dataset':8} {'venue':14} {'symbol':10} {'date':10} {'files':>5} {'MB':>9}  root"]
    for p in parts:
        note = f"  ({p.skipped_open} open file(s) skipped)" if p.skipped_open else ""
        lines.append(
            f"{p.dataset:8} {p.venue:14} {p.symbol:10} {p.date:10} {p.files:5d} "
            f"{p.bytes / 1e6:9.1f}  {p.root}{note}"
        )
    lines.append(f"{len(parts)} partitions")
    return lines


def write_registry(path: Path, parts: list[Partition]) -> None:
    """Write ``configs/datasets.toml`` listing registered partitions (no copy)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = ["# Registered canonical datasets (edbt-ingest recordings). Roots are read-only.", ""]
    for p in parts:
        rows += [
            "[[dataset]]",
            f'root = "{p.root}"',
            f'dataset = "{p.dataset}"',
            f'venue = "{p.venue}"',
            f'symbol = "{p.symbol}"',
            f'date = "{p.date}"',
            f"files = {p.files}",
            "",
        ]
    path.write_text("\n".join(rows), encoding="utf-8")
