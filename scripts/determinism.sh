#!/usr/bin/env bash
# AC-W-01 on synthetic data: the same seed and data give the same run_hash
# and byte-identical hashed files; a different seed gives a different hash.
# Usage: EDBT=path/to/edbt scripts/determinism.sh [WORKDIR]   (WORKDIR is relative)
set -euo pipefail
edbt=${EDBT:-edbt}
work=${1:-_determinism}
events=${EVENTS:-1000000}
rm -rf "$work"
mkdir -p "$work"
"$edbt" data synth --kind l2 --events "$events" --seed 7 --out "$work/data" >/dev/null
run() {
  "$edbt" run --config configs/backtest_synthetic.toml --set "data.roots=[\"$work/data\"]" \
    --out "$work/$1" --seed "$2" --no-log-experiment | awk '/^run_hash/ {print $2}'
}
a=$(run a 20261030)
b=$(run b 20261030)
c=$(run c 7)
echo "seed 20261030 run A: $a"
echo "seed 20261030 run B: $b"
echo "seed 7:              $c"
status=0
for f in config.resolved.toml events.log.zst fills.parquet ledger.parquet orders.parquet summary.json; do
  if ! cmp -s "$work/a/$f" "$work/b/$f"; then
    echo "differs: $f"
    status=1
  fi
done
[ "$a" = "$b" ] || status=1
[ "$a" != "$c" ] || status=1
if [ "$status" = 0 ]; then echo "AC-W-01: PASS"; else echo "AC-W-01: FAIL"; fi
exit "$status"
