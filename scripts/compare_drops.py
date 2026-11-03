"""Line up testnet connection drops with the shared capture's Binance drops.

Both write gap rows with the same columns (gap_id, venue, symbol, stream,
start_ns, end_ns, duration_ms, reason, ...). This reads the gap rows of the
testnet sessions under ``$DATA_ROOT/gaps`` and the crypto capture's under
``<capture-root>/gaps`` (read-only, closed hourly files only: the current
hour's file is still being written and is skipped), and writes one CSV with
both, sorted by start, plus per-day drop counts on stdout.

    python scripts/compare_drops.py --capture ~/data/crypto-lob-alpha --out results/drops_compare.csv
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path

import polars as pl

NS_PER_HOUR = 3_600_000_000_000


def closed_files(root: Path) -> list[Path]:
    """Gap Parquet files whose hour has ended (``part-HH*`` in ``date=D``)."""
    now = dt.datetime.now(dt.UTC)
    current = (now.date().isoformat(), now.hour)
    out = []
    for p in sorted((root / "gaps").rglob("*.parquet")):
        date = next((s.removeprefix("date=") for s in p.parts if s.startswith("date=")), None)
        hour = p.name.removeprefix("part-")[:2]
        if date is not None and hour.isdigit() and (date, int(hour)) >= current:
            continue
        out.append(p)
    return out


def load(root: Path, source: str) -> pl.DataFrame:
    """Gap rows under ``root`` with a ``source`` column."""
    frames = []
    for p in closed_files(root):
        try:
            frames.append(pl.read_parquet(p, hive_partitioning=False))
        except (OSError, pl.exceptions.ComputeError):
            continue
    if not frames:
        return pl.DataFrame()
    df = pl.concat(frames, how="diagonal_relaxed").unique(subset="gap_id", keep="first")
    keep = ["venue", "symbol", "stream", "start_ns", "end_ns", "duration_ms", "reason"]
    return df.select([c for c in keep if c in df.columns]).with_columns(pl.lit(source).alias("source"))


def main() -> None:
    """Write the merged drop table and print daily counts."""
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--capture", type=Path, required=True, help="crypto-lob-alpha data root (read-only)")
    p.add_argument(
        "--testnet", type=Path, default=Path(os.environ.get("DATA_ROOT", "~/data/event-driven-backtester"))
    )
    p.add_argument("--venue", default="binance-usdm", help="capture venue to compare against")
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    cap = load(a.capture.expanduser(), "capture")
    if cap.height:
        cap = cap.filter(pl.col("venue") == a.venue)
    tn = load(a.testnet.expanduser(), "testnet")
    both = pl.concat([f for f in (cap, tn) if f.height], how="diagonal_relaxed")
    if not both.height:
        print("no closed gap files yet")
        return
    both = both.sort("start_ns").with_columns(
        pl.from_epoch("start_ns", time_unit="ns").dt.strftime("%Y-%m-%dT%H:%M:%S%.3fZ").alias("start_utc")
    )
    a.out.parent.mkdir(parents=True, exist_ok=True)
    both.write_csv(a.out)
    daily = (
        both.with_columns(pl.col("start_utc").str.slice(0, 10).alias("day"))
        .group_by("day", "source")
        .agg(pl.len().alias("drops"), pl.col("duration_ms").sum().alias("down_ms"))
        .sort("day", "source")
    )
    print(daily)
    print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
