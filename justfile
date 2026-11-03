# edbt task runner. Run from WSL (or any Linux shell); CI calls the same tools.
set shell := ["bash", "-euo", "pipefail", "-c"]
set dotenv-load := true
# Shebang recipes run from /tmp: /run/user/<uid> is not always writable under WSL.
set tempdir := "/tmp"

home := env_var_or_default("HOME", ".")
# Build output and the dev venv live on WSL ext4, never on /mnt/* (spec 7d).
export CARGO_TARGET_DIR := env_var_or_default("CARGO_TARGET_DIR", home + "/build/event-driven-backtester")
export UV_PROJECT_ENVIRONMENT := env_var_or_default("UV_PROJECT_ENVIRONMENT", home + "/build/event-driven-backtester/venv")
# At most 8 build jobs: live recorders share this machine.
export CARGO_BUILD_JOBS := env_var_or_default("CARGO_BUILD_JOBS", "8")
data_root := env_var_or_default("DATA_ROOT", home + "/data/event-driven-backtester")
py := "uv run --frozen"

# List recipes.
default:
    @just --list

# One-time local setup: data root, data/ and results/runs symlinks, venv.
setup:
    mkdir -p "{{data_root}}/runs" "{{data_root}}/synthetic"
    [ -e data ] || ln -s "{{data_root}}" data
    [ -e results/runs ] || ln -s "{{data_root}}/runs" results/runs
    just dev

# Build the extension and install the package with dev tools.
dev:
    PYO3_PYTHON="$(uv python find 3.12)" uv sync --python 3.12

# Format Rust and Python sources.
fmt:
    cargo fmt --all
    {{py}} ruff format python tests/python scripts strategies
    {{py}} ruff check --fix python tests/python scripts strategies

# All lint gates of the CI lint job.
lint: lint-rust lint-py guard deny

lint-rust:
    cargo fmt --all --check
    PYO3_PYTHON="$(uv python find 3.12)" cargo clippy --workspace --all-targets --locked -- -D warnings

lint-py:
    {{py}} ruff check python tests/python scripts strategies
    {{py}} ruff format --check python tests/python scripts strategies
    {{py}} mypy
    {{py}} python -m mypy.stubtest edbt._edbt

# NFR-09 data-file guard over tracked files.
guard:
    bash scripts/check_data_files.sh

deny:
    cargo deny check

# Rust and Python tests (no real data, no network).
test: test-rust test-py

test-rust:
    cargo nextest run --workspace --exclude edbt-py --locked

test-py:
    {{py}} pytest -m "not data and not testnet"

# Tests that need real data (local only).
test-data:
    cargo nextest run --workspace --exclude edbt-py --run-ignored only -E 'test(/data/)'
    {{py}} pytest -m data

# Tests that need testnet keys and network (local only).
test-testnet:
    {{py}} pytest -m testnet

# Coverage with the NFR-06 thresholds.
cov rust_min="85" py_min="85":
    cargo llvm-cov nextest --workspace --exclude edbt-py --locked --ignore-filename-regex "edbt-cli/src/main.rs" --fail-under-lines {{rust_min}}
    cargo llvm-cov report --ignore-filename-regex "edbt-cli/src/main.rs" --summary-only > /tmp/edbt-coverage-summary.txt
    python3 scripts/check_crate_coverage.py /tmp/edbt-coverage-summary.txt edbt-exchange=90 edbt-ledger=90
    {{py}} pytest -m "not data and not testnet" --cov=edbt --cov-report=term-missing --cov-fail-under={{py_min}}

# Everything CI gates on.
ci: lint test cov

# M1 smoke test: synth -> canonical partition -> validated read -> sibling listing.
smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    cargo run -q -p edbt-cli --bin edbt -- data synth --kind l2 --events 20000 --seed 7 --out "$tmp/data"
    cargo run -q -p edbt-cli --bin edbt -- data synth --kind l3 --events 20000 --seed 7 --out "$tmp/data"
    for ds in book_l2 trades book_l3; do
      cargo run -q -p edbt-cli --bin edbt -- data validate --dataset "$ds" --root "$tmp/data" --instruments configs/instruments.toml | tail -1
    done
    {{py}} python -c "import sys, pyarrow.dataset as d; t = d.dataset(sys.argv[1], partitioning='hive').to_table(); print('pyarrow read', t.num_rows, 'book_l2 rows')" "$tmp/data/book_l2"
    if [ -n "${EDBT_EXTERNAL_DATA_ROOTS:-}" ]; then
      {{py}} edbt-ingest recordings --dry-run | tail -5
    else
      echo "EDBT_EXTERNAL_DATA_ROOTS unset: sibling listing skipped"
    fi
    echo "smoke: PASS"

# AC-W-01: same seed twice -> identical run hash; another seed -> different.
verify-ac-w-01:
    cargo build -q --release -p edbt-cli
    EDBT="$CARGO_TARGET_DIR/release/edbt" bash scripts/determinism.sh _determinism
    cargo nextest run -p edbt-core --test determinism

# AC-W-02: reconciliation proptest (10,000 cases) and 0 violations on synthetic runs.
verify-ac-w-02:
    cargo nextest run -p edbt-ledger --test recon_prop
    cargo nextest run -p edbt-core --test determinism same_seed_identical_hash
    @echo "AC-W-02: PASS (engine aborts on any violation; see summary.json reconciliation_violations)"

# AC-W-03: every fill rule has its named test; scenarios and fill rules pass.
verify-ac-w-03:
    python3 scripts/check_fill_rule_coverage.py
    cargo nextest run -p edbt-exchange

# AC-W-06: every risk rule accepts and rejects; kill switch; the same risk
# path through the simulated exchange and the venue gateway (mock); the kill
# switch clears the venue's open orders within 2 s.
verify-ac-w-06:
    cargo nextest run -p edbt-risk
    cargo nextest run -p edbt-core --test oms_state_machine
    cargo nextest run -p edbt-cli --test live_mock -E 'test(shared_risk_path) | test(kill_switch)'

# AC-W-05: latency models (KS tests) and the measured report. `measure` needs
# testnet keys; it writes rows under $DATA_ROOT/processed/latency and the report.
verify-ac-w-05 n="500":
    cargo nextest run -p edbt-exchange --test latency_models
    cargo run -q --release -p edbt-cli --bin edbt -- latency measure --config configs/live_testnet.toml --n {{n}} --report reports/latency.md

# AC-W-07 (CI version): the same strategy in backtest, paper and testnet
# against the local mock venue.
verify-ac-w-07:
    cargo nextest run -p edbt-cli --test live_mock
    {{py}} pytest tests/python/test_strategy_parity.py

# AC-W-07 with the real testnet (keys in .env): 10 minutes each of
# strategies/mm_simple.py in paper and testnet mode, then the replay check.
demo-modes minutes="10":
    #!/usr/bin/env bash
    set -euo pipefail
    {{py}} edbt live --config configs/paper_testnet.toml --duration {{minutes}}m --record
    {{py}} edbt live --config configs/live_testnet.toml --duration {{minutes}}m --record --i-understand-testnet-only
    for run in $(ls -d results/live/*/ | tail -2); do {{py}} edbt log replay-strategy --run "$run"; done

# AC-W-12: guard and secret hygiene.
verify-ac-w-12:
    cargo nextest run -p edbt-live --test guard

# AC-H-06: replay every recorded testnet session through the strategy.
verify-ac-h-06 sessions=(data_root + "/live/sessions"):
    #!/usr/bin/env bash
    set -euo pipefail
    for run in "{{sessions}}"/*/; do {{py}} edbt log replay-strategy --run "$run"; done
    cargo tree -p edbt-live -e normal --depth 1

# 24 h testnet soak before the gap study (M5 exit): mm_simple on BTCUSDT.
testnet-soak:
    {{py}} edbt live --config configs/gap_study_testnet.toml --duration 24h --record --i-understand-testnet-only

# Pull testnet tick, lot, min notional and commission rates into
# configs/instruments.toml (keys in .env).
instruments-testnet:
    {{py}} edbt-ingest instruments --venue binance-usdm-testnet --config configs/live_testnet.toml
    {{py}} edbt-ingest instruments --venue binance-usdm-testnet --config configs/gap_study_testnet_ethusdt.toml

# Deploy clone + cron watchdog for the 21-day gap study (D-06):
#   just gap-start <commit> <end-utc>
gap-start commit end:
    bash deploy/install_gap_study.sh {{commit}} --cron {{end}}

# Stop the gap study at the end of the current sessions' next check.
gap-stop:
    mkdir -p "{{data_root}}/live/state/btcusdt" "{{data_root}}/live/state/ethusdt"
    touch "{{data_root}}/live/state/btcusdt/STOP" "{{data_root}}/live/state/ethusdt/STOP"
    for d in "{{data_root}}"/live/sessions/*/; do [ -f "$d/status.json" ] && echo kill > "$d/control" || true; done

# Status of the running gap-study sessions.
gap-status:
    @for f in $(ls -t "{{data_root}}"/live/sessions/*/status.json 2>/dev/null | head -2); do echo "$f"; cat "$f"; done

# AC-W-04: ordering proptests (10,000 cases each).
verify-ac-w-04:
    cargo nextest run -p edbt-core --test ordering

# AC-W-08: event log rebuilds the ledger; replay-check reproduces the run hash.
verify-ac-w-08:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build -q --release -p edbt-cli
    e="$CARGO_TARGET_DIR/release/edbt"
    rm -rf _ac_w_08 && "$e" data synth --kind l2 --events 300000 --seed 7 --out _ac_w_08/data >/dev/null
    "$e" run --config configs/backtest_synthetic.toml --set 'data.roots=["_ac_w_08/data"]' --out _ac_w_08/run --no-log-experiment | tail -1
    "$e" log verify --run _ac_w_08/run
    "$e" replay-check --run _ac_w_08/run

# AC-W-01/02/08/09 on one real L2 day (local; needs the shared crypto capture).
verify-real-day date="2026-11-02":
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build -q --release -p edbt-cli
    e="$CARGO_TARGET_DIR/release/edbt"
    x="${EDBT_EXTERNAL_DATA_ROOTS:?set EDBT_EXTERNAL_DATA_ROOTS to the crypto capture root}"
    r="{{data_root}}/runs/real-{{date}}"
    rm -rf "$r"
    for run in a b; do "$e" run --config configs/backtest_l2_btcusdt.toml --out "$r/$run" --no-log-experiment | tail -2; done
    for f in config.resolved.toml events.log.zst fills.parquet ledger.parquet orders.parquet summary.json; do cmp "$r/a/$f" "$r/b/$f"; done
    echo "AC-W-01 real day: PASS (identical run files)"
    "$e" log verify --run "$r/a"
    "$e" replay-check --run "$r/a"
    for ds in book_l2 trades; do "$e" data validate --dataset "$ds" --root "${x%%:*}" --venue binance-usdm --symbol BTCUSDT --date {{date}} | tail -1; done
    "$e" data bbo --root "${x%%:*}" --venue binance-usdm --symbol BTCUSDT --date {{date}} --compare "${x%%:*}"

# AC-W-09: fixtures validate with 0 hard failures; L3 aggregation equals the L2 book.
verify-ac-w-09:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
    for f in tests/fixtures/*_small.csv; do cargo run -q -p edbt-cli --bin edbt -- data synth --fixture "$f" --out "$tmp" >/dev/null; done
    for ds in book_l2 trades book_l3; do
      cargo run -q -p edbt-cli --bin edbt -- data validate --dataset "$ds" --root "$tmp" --instruments tests/fixtures/instruments.toml | tail -2
    done
    cargo nextest run -p edbt-data --test fixtures_l3_l2
    cargo nextest run -p edbt-book --test l3_l2_prop

# AC-W-11: the Python API contract, stubs and strict typing.
verify-ac-w-11:
    {{py}} pytest tests/python/test_api_contract.py
    {{py}} python -m mypy.stubtest edbt._edbt
    {{py}} mypy
    {{py}} mypy --strict strategies
    @grep -q '^API_VERSION = "1\.' python/edbt/__init__.py && echo "API_VERSION 1.x"

# Criterion benchmarks (local numbers only; see reports/throughput.md).
bench:
    cargo bench -p edbt-core --bench engine

# AC-H-03 / FR-27: throughput of every Rust and Python path. Publish only on
# the reference machine with no multi-week capture running (D-08).
study-throughput:
    cargo build -q --release -p edbt-cli
    sed 's|^edbt = .*|edbt = "'"$CARGO_TARGET_DIR"'/release/edbt"|' configs/studies/throughput.toml > /tmp/edbt-throughput.toml
    {{py}} edbt-study throughput --config /tmp/edbt-throughput.toml --out reports
    cat reports/hardware.md

verify-ac-h-03: study-throughput

# AC-H-07: build the wheel, install it into a clean venv, run the quickstart.
verify-ac-h-07:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
    uvx maturin build --release --out "$tmp/dist"
    uv venv -q --python 3.12 "$tmp/venv"
    VIRTUAL_ENV="$tmp/venv" uv pip install -q "$tmp"/dist/*.whl
    cd "$tmp" && start=$(date +%s) && "$tmp/venv/bin/python" -m edbt.examples.quickstart
    echo "quickstart took $(( $(date +%s) - start ))s"

# Synthetic backtest with summary in under a minute.
quickstart:
    {{py}} python -m edbt.examples.quickstart

# Unsigned GET /fapi/v1/time against the configured testnet REST hosts (no keys).
testnet-ping:
    {{py}} python scripts/testnet_ping.py configs/live_testnet.toml results/testnet_ping.csv

# Reference-hardware facts for reports/hardware.md (P-13, D-08).
sysinfo:
    @lscpu | grep -E 'Model name|^CPU\(s\)|Thread|Core|Socket|MHz|L1d|L2|L3' || true
    @free -h | head -2
    @uname -srvmo
    @(lsb_release -ds 2>/dev/null || cat /etc/os-release | head -1)
    @rustc --version
    @uv run --frozen python --version

# Write fixture CSVs from the seeded generator (the committed fixtures).
fixtures:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    cargo run -q -p edbt-cli --bin edbt -- data synth --kind l2 --events 3000 --seed 7 --csv tests/fixtures
    cargo run -q -p edbt-cli --bin edbt -- data synth --kind l3 --events 3000 --seed 7 --symbol SYN3 --csv "$tmp"
    cp "$tmp/l3_small.csv" tests/fixtures/l3_small.csv
    cp "$tmp/trades_small.csv" tests/fixtures/l3_trades_small.csv

# Remove run directories not referenced by a report (asks first).
clean-runs:
    @echo "clean-runs arrives with the studies (M6)"
