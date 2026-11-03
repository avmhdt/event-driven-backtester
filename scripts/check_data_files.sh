#!/usr/bin/env bash
# NFR-09: no raw or licensed data in git. Fails on tracked *.parquet, *.zst,
# *.npz or *.csv.gz outside tests/fixtures/, and on any tracked file over
# 500 KB outside reports/figures/. With file arguments (pre-commit), checks
# only those files.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
limit=$((500 * 1024))
status=0
check() {
  local path=$1
  case "$path" in
    tests/fixtures/*) ;;
    *.parquet | *.zst | *.npz | *.csv.gz)
      echo "data file not allowed in git: $path" >&2
      status=1
      ;;
  esac
  [ -f "$path" ] || return 0
  case "$path" in reports/figures/*) return 0 ;; esac
  local size
  size=$(wc -c <"$path")
  if [ "$size" -gt "$limit" ]; then
    echo "file over 500 KB: $path ($size bytes)" >&2
    status=1
  fi
}
if [ "$#" -gt 0 ]; then
  for p in "$@"; do check "$p"; done
else
  while IFS= read -r -d '' p; do check "$p"; done < <(git ls-files -z)
fi
exit "$status"
