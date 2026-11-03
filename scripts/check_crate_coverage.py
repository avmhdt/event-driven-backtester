"""NFR-06 per-crate line coverage from `cargo llvm-cov report --summary-only`.

Usage: check_crate_coverage.py REPORT.txt edbt-exchange=90 edbt-ledger=90
Sums covered and total lines of every file under crates/<crate>/src and
fails if a crate is below its threshold.
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path


def main(argv: list[str]) -> int:
    """Parse the report and compare each crate with its threshold."""
    report = Path(argv[1]).read_text(encoding="utf-8")
    wanted = dict(a.split("=") for a in argv[2:])
    lines: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for row in report.splitlines():
        # Paths are relative to the common prefix, with or without `crates/`.
        m = re.search(r"(?:^|/)(edbt-[a-z0-9-]+)/src/\S*", row)
        cols = row.split()
        if not m or len(cols) < 10:
            continue
        # Columns: file, regions, missed, cover, functions, missed, cover,
        # lines, missed lines, cover, ...
        total, missed = int(cols[7]), int(cols[8])
        lines[m.group(1)][0] += total - missed
        lines[m.group(1)][1] += total
    status = 0
    for crate, threshold in wanted.items():
        covered, total = lines.get(crate, [0, 0])
        pct = 100.0 * covered / total if total else 0.0
        ok = pct >= float(threshold)
        status |= not ok
        verdict = "ok" if ok else "FAIL"
        print(f"{crate}: {pct:.2f}% lines ({covered}/{total}), threshold {threshold}% {verdict}")
    return status


if __name__ == "__main__":
    sys.exit(main(sys.argv))
