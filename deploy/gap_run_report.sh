#!/usr/bin/env bash
# Gap-study run 2 reports, run from cron (not from an app session).
# Reads START and END from $DATA_ROOT/live/state/run2.env and, once each
# milestone has passed, writes its report once (a marker file per
# milestone): day 1, day 7, day 14 and the final 21-day window, judged at
# the 95% bar (NFR-11) with the owner's venue-downtime ruling. Read-only
# apart from the report files.
set -u
data="${DATA_ROOT:-$HOME/data/event-driven-backtester}"
deploy="${EDBT_DEPLOY:-$HOME/deploy/event-driven-backtester}"
env_file="$data/live/state/run2.env"
[ -r "$env_file" ] || exit 0
# shellcheck disable=SC1090
. "$env_file"
out="$data/live/reports/run2"
mkdir -p "$out"
exec 9>"$out/.lock"
flock -n 9 || exit 0
start_s=$(date -u -d "$START" +%s)
end_s=$(date -u -d "$END" +%s)
now=$(date -u +%s)
export DATA_ROOT="$data"
report() { # <tag> <from-epoch> <to-epoch>
  local tag="$1" from to
  from=$(date -u -d "@$2" +%FT%TZ)
  to=$(date -u -d "@$3" +%FT%TZ)
  [ -e "$out/$tag.done" ] && return
  [ "$now" -ge $(($3 + 300)) ] || return
  (
    cd "$deploy" || exit 1
    for fmt in md json; do
      flag=""
      [ "$fmt" = json ] && flag="--json"
      .venv-run/bin/python -m edbt.soak --bar 0.95 --from "$from" --to "$to" \
        --series btc=BTC_USDC-PERPETUAL --series eth=ETH_USDC-PERPETUAL $flag >"$out/$tag.$fmt"
    done
  ) && echo "$(date -u +%FT%TZ) $from $to" >"$out/$tag.done"
}
report day1 "$((start_s + 0 * 86400))" "$((start_s + 1 * 86400))"
report days1-7 "$start_s" "$((start_s + 7 * 86400))"
report day7 "$((start_s + 6 * 86400))" "$((start_s + 7 * 86400))"
report days1-14 "$start_s" "$((start_s + 14 * 86400))"
report days1-21 "$start_s" "$end_s"
