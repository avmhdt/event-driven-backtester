#!/usr/bin/env bash
# Keep one gap-study session series running (AC-H-01, R-09).
#
#   gap_supervisor.sh <name> <config> <end-utc>
#
# Runs `edbt live ... --duration 24h` back to back from the deploy clone
# until <end-utc> (e.g. 2026-11-28T00:00:00Z) or until <state>/STOP exists.
# A normal end (exit 0) or a kill that is safe to restart from (exit 3)
# starts the next session after 30 s; a loss-limit kill (exit 4) or any
# other failure writes <state>/STOPPED and stops. A cron watchdog starts this
# script every 5 minutes; flock makes the extra starts no-ops, so after a
# WSL restart the series resumes on its own and the missed time shows up as
# a gap between sessions (logged, never filled).
set -u
name="$1"
config="$2"
end_utc="$3"
deploy="${EDBT_DEPLOY:-$HOME/deploy/event-driven-backtester}"
data_root="${DATA_ROOT:-$HOME/data/event-driven-backtester}"
state="$data_root/live/state/$name"
mkdir -p "$state" "$data_root/live/logs"
exec 9>"$state/lock"
flock -n 9 || exit 0

end_s=$(date -u -d "$end_utc" +%s)
log="$data_root/live/logs/$name.log"
export DATA_ROOT="$data_root"
cd "$deploy" || exit 1
while :; do
  now=$(date -u +%s)
  if [ -e "$state/STOP" ] || [ -e "$state/STOPPED" ] || [ "$now" -ge "$end_s" ]; then
    echo "$(date -u +%FT%TZ) supervisor exiting (stop file or end $end_utc)" >>"$log"
    exit 0
  fi
  left=$((end_s - now))
  dur=$((left < 86400 ? left : 86400))
  echo "$(date -u +%FT%TZ) session start, ${dur}s" >>"$log"
  "$deploy/.venv-run/bin/edbt" live --config "$config" --duration "${dur}s" --record \
    --i-understand-testnet-only >>"$log" 2>&1
  code=$?
  echo "$(date -u +%FT%TZ) session end, exit $code" >>"$log"
  case "$code" in
    0 | 3) sleep 30 ;;
    *)
      echo "$(date -u +%FT%TZ) exit $code: not restarting" >>"$log"
      echo "$code" >"$state/STOPPED"
      exit "$code"
      ;;
  esac
done
