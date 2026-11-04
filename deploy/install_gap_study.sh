#!/usr/bin/env bash
# Prepare the gap-study deploy clone on WSL ext4 (spec 7(d)): a clone of this
# repository at one commit, its own virtualenv with a release build of the
# extension, and a link to the repository's .env (never a copy of it).
#
#   deploy/install_gap_study.sh <commit> [--cron <end-utc>]
#
# With --cron it also adds the watchdog lines to the user's crontab (no sudo
# needed); without it, it prints them. deploy/edbt-gap@.service is the
# systemd alternative for an owner with sudo.
set -euo pipefail
commit="$1"
repo="$(cd "$(dirname "$0")/.." && pwd)"
deploy="${EDBT_DEPLOY:-$HOME/deploy/event-driven-backtester}"
mkdir -p "$(dirname "$deploy")"
if [ ! -d "$deploy/.git" ]; then
  git clone --quiet "$repo" "$deploy"
fi
git -C "$deploy" fetch --quiet "$repo" "$commit"
git -C "$deploy" checkout --quiet --detach "$commit"
ln -sfn "$repo/.env" "$deploy/.env"
cd "$deploy"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/build/event-driven-backtester}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-8}"
UV_PROJECT_ENVIRONMENT="$deploy/.venv-run" uv sync --frozen --no-dev --python 3.12
echo "deployed $(git -C "$deploy" rev-parse --short HEAD) to $deploy"
lines="*/5 * * * * $deploy/deploy/gap_supervisor.sh btc $deploy/configs/gap_study_testnet.toml END
*/5 * * * * $deploy/deploy/gap_supervisor.sh eth $deploy/configs/gap_study_testnet_eth.toml END"
if [ "${2:-}" = "--cron" ]; then
  end="$3"
  lines="${lines//END/$end}"
  ( crontab -l 2>/dev/null | grep -v gap_supervisor.sh; echo "$lines" ) | crontab -
  echo "crontab updated; the watchdog starts the sessions within 5 minutes"
else
  echo "watchdog lines (add with --cron <end-utc>):"
  echo "$lines"
fi
