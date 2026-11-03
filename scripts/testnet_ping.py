"""Unsigned reachability check of the Binance USD-M futures testnet (P-09, D-01).

Sends ``GET <rest_url>/fapi/v1/time`` (no keys, no signature) to each REST
base URL listed in ``configs/live_testnet.toml`` and appends one row per URL
to ``results/testnet_ping.csv``. URLs whose host is not on
``live.allowed_hosts``, or that name a mainnet host, are refused before any
connection is made.
"""

from __future__ import annotations

import csv
import json
import sys
import time
import tomllib
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

MAINNET = {
    "fapi.binance.com",
    "fstream.binance.com",
    "ws-fapi.binance.com",
    "api.binance.com",
    "dapi.binance.com",
    "dstream.binance.com",
    "stream.binance.com",
    "ws-api.binance.com",
}


def is_mainnet(host: str) -> bool:
    """Mirror of the Rust guard's mainnet rule."""
    h = host.lower().rstrip(".")
    if h in MAINNET or h == "binance.com":
        return True
    return h.endswith(".binance.com") and not h.removesuffix(".binance.com").startswith("demo-")


def main(config: str = "configs/live_testnet.toml", out: str = "results/testnet_ping.csv") -> int:
    """Ping every configured REST base URL; exit 0 if at least one answered."""
    live = tomllib.loads(Path(config).read_text(encoding="utf-8"))["live"]
    allowed = {h.lower() for h in live["allowed_hosts"]}
    if any(is_mainnet(h) for h in allowed):
        print("refused: allowed_hosts names a mainnet host", file=sys.stderr)
        return 2
    urls = list(dict.fromkeys([live["rest_url"], *live.get("ping_urls", [])]))
    rows = []
    for base in urls:
        host = (urlparse(base).hostname or "").lower()
        if is_mainnet(host) or host not in allowed or urlparse(base).scheme != "https":
            print(f"refused: {base}", file=sys.stderr)
            return 2
        url = base.rstrip("/") + "/fapi/v1/time"
        t0 = time.perf_counter()
        status, server_ms, note = "", "", ""
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                status = str(resp.status)
                server_ms = str(json.loads(resp.read())["serverTime"])
        except urllib.error.HTTPError as e:
            status, note = str(e.code), e.reason
        except (urllib.error.URLError, TimeoutError, OSError, ValueError, KeyError) as e:
            status, note = "error", type(e).__name__ + ": " + str(e)
        rtt_ms = (time.perf_counter() - t0) * 1000
        utc = datetime.now(UTC).isoformat(timespec="seconds")
        rows.append([utc, url, status, server_ms, f"{rtt_ms:.1f}", note])
        print(f"{url} status={status} serverTime={server_ms} rtt_ms={rtt_ms:.1f} {note}".rstrip())
    path = Path(out)
    new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        if new:
            w.writerow(["utc", "url", "http_status", "server_time_ms", "rtt_ms", "note"])
        w.writerows(rows)
    return 0 if any(r[2] == "200" for r in rows) else 1


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
