"""Instrument metadata pull from the testnet (FR-30).

Tick and lot sizes and the minimum notional come from
``GET /fapi/v1/exchangeInfo``; maker and taker rates from the signed
``GET /fapi/v1/commissionRate`` of the account whose testnet keys are in
``BINANCE_TESTNET_*``. Requests run in Rust behind the testnet host guard.
Fees are never typed in: each entry records the endpoints and the date read.
"""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from edbt import _edbt
from edbt.config import Config


def rate_to_bps(rate: str) -> float:
    """A commission rate (``"0.0002"``) in basis points (``2.0``), exactly."""
    return float(Decimal(rate) * 10_000)


def entry(venue: str, row: dict[str, Any], effective_from: str) -> str:
    """One ``[[instrument]]`` table for ``configs/instruments.toml``."""
    lines = [
        "[[instrument]]",
        f'venue = "{venue}"',
        f'symbol = "{row["symbol"]}"',
        f"tick_size = {row['tick_size']!r}",
        f"lot_size = {row['lot_size']!r}",
        'contract_type = "linear_perp"',
        f"maker_bps = {rate_to_bps(row['maker_rate'])!r}",
        f"taker_bps = {rate_to_bps(row['taker_rate'])!r}",
        f'effective_from = "{effective_from}"',
    ]
    if row.get("min_notional") is not None:
        lines.append(f"min_notional = {row['min_notional']!r}")
    lines.append(f'source = "{row["source"]}"')
    return "\n".join(lines) + "\n"


def pull(config: Config) -> list[dict[str, Any]]:
    """Pull metadata for ``data.symbols`` from the venue in ``[live]``."""
    rows: list[dict[str, Any]] = _edbt.pull_instruments(json.dumps(config.to_dict()))
    return rows


def append(
    path: Path, venue: str, rows: list[dict[str, Any]], effective_from: str | None = None
) -> list[str]:
    """Append entries not already present for the same venue, symbol and date."""
    day = effective_from or dt.datetime.now(dt.UTC).date().isoformat()
    text = path.read_text() if path.exists() else ""
    added = []
    blocks = []
    for r in rows:
        key = f'venue = "{venue}"\nsymbol = "{r["symbol"]}"'
        if key in text and f'effective_from = "{day}"' in text.split(key, 1)[1].split("[[instrument]]", 1)[0]:
            continue
        blocks.append(entry(venue, r, day))
        added.append(r["symbol"])
    if blocks:
        header = f"\n# Pulled by `edbt-ingest instruments --venue {venue}` on {day}.\n"
        with path.open("a") as f:
            f.write(header + "\n".join(blocks))
    return added
