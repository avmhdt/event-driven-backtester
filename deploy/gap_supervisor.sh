#!/usr/bin/env bash
# Keep one gap-study session series running (AC-H-01, R-09).
#
#   gap_supervisor.sh <name> <config> <end-utc>
#
# Runs `edbt live ... --duration 24h` back to back from the deploy clone
# until <end-utc> (e.g. 2026-11-28T00:00:00Z) or until <state>/STOP exists.
#
# Before each session it runs `edbt venue-status`. While the venue itself
# reports not open (halted, locked, settlement) or answers with HTTP 5xx
# (exit 5), it waits, checking every minute, and logs the interval as
# venue downtime in $DATA_ROOT/live/venue_downtime/<name>.csv (owner ruling
# of 2026-11-05: excluded from the uptime judged against 99%, reported on
# its own). Anything else is our side and counts against us.
#
# A normal end (exit 0) or a kill that is safe to restart from (exit 3)
# starts the next session after 30 s. A loss-limit kill (exit 4) writes
# <state>/STOPPED and stops. Any other failure (exit 1) is checked against
# the venue: during venue downtime it is not counted; when the venue cannot
# be reached (our network) it is retried after 1, 2, ... 10 minutes without
# counting; while the venue is open it is retried the same way, and the
# tenth such failure in a row writes <state>/STOPPED and stops.
#
# A cron watchdog starts this script every 5 minutes; flock makes the extra
# starts no-ops, so after a WSL restart the series resumes on its own and
# the missed time shows up as a gap between sessions (logged, never filled).
set -u
name="$1"
config="$2"
end_utc="$3"
deploy="${EDBT_DEPLOY:-$HOME/deploy/event-driven-backtester}"
data_root="${DATA_ROOT:-$HOME/data/event-driven-backtester}"
state="$data_root/live/state/$name"
downtime="$data_root/live/venue_downtime/$name.csv"
mkdir -p "$state" "$data_root/live/logs" "$(dirname "$downtime")"
exec 9>"$state/lock"
flock -n 9 || exit 0

end_s=$(date -u -d "$end_utc" +%s)
log="$data_root/live/logs/$name.log"
edbt="$deploy/.venv-run/bin/edbt"
export DATA_ROOT="$data_root"
cd "$deploy" || exit 1
max_fails=10
fails=0
ours=0
# Seconds in a "minute" of waiting (tests shorten it).
minute="${EDBT_SUPERVISOR_MINUTE_S:-60}"

say() { echo "$(date -u +%FT%TZ) $*" >>"$log"; }
utc() { date -u -d "@$1" +%FT%TZ; }

# `edbt venue-status`: sets $venue (its line) and $vrc (0 open, 5 down, 6 unknown).
check_venue() {
  local out
  out=$("$edbt" venue-status --config "$config" 2>>"$log")
  vrc=$?
  venue=${out##*$'\n'}
}

# Start a venue-downtime interval (no-op while one is open).
open_down() {
  [ -e "$state/venue_down" ] && return
  echo "$(date -u +%s) $1" >"$state/venue_down"
  say "venue down ($1): waiting, logged as venue downtime"
}

# Close the open interval at epoch $1 and append it to the downtime CSV.
close_down() {
  [ -e "$state/venue_down" ] || return
  local start reason
  read -r start reason <"$state/venue_down"
  [ -e "$downtime" ] || echo "start_utc,end_utc,seconds,reason" >"$downtime"
  echo "$(utc "$start"),$(utc "$1"),$(($1 - start)),\"${reason//\"/}\"" >>"$downtime"
  rm -f "$state/venue_down"
  say "venue open again after $(($1 - start))s of venue downtime"
}

while :; do
  now=$(date -u +%s)
  if [ -e "$state/STOP" ] || [ -e "$state/STOPPED" ] || [ "$now" -ge "$end_s" ]; then
    close_down "$((now < end_s ? now : end_s))"
    say "supervisor exiting (stop file or end $end_utc)"
    exit 0
  fi
  check_venue
  if [ "$vrc" -eq 5 ]; then
    open_down "${venue#down: }"
    sleep "$minute"
    continue
  fi
  close_down "$(date -u +%s)"
  [ "$vrc" -eq 0 ] || say "venue status $venue (exit $vrc): counts against us; starting anyway"
  left=$((end_s - $(date -u +%s)))
  dur=$((left < 86400 ? left : 86400))
  say "session start, ${dur}s"
  "$edbt" live --config "$config" --duration "${dur}s" --record \
    --i-understand-testnet-only >>"$log" 2>&1
  code=$?
  say "session end, exit $code"
  case "$code" in
    0 | 3)
      fails=0
      ours=0
      sleep $((minute / 2))
      ;;
    4)
      say "exit 4 (loss limit): not restarting"
      echo "$code" >"$state/STOPPED"
      exit "$code"
      ;;
    *)
      check_venue
      if [ "$vrc" -eq 5 ]; then
        say "exit $code while the venue is down: not counted"
        open_down "${venue#down: }"
      elif [ "$vrc" -ne 0 ]; then
        ours=$((ours < max_fails ? ours + 1 : max_fails))
        say "exit $code, venue status $venue: our side, retrying in ${ours} min (not counted toward the stop)"
        sleep $((minute * ours))
      else
        fails=$((fails + 1))
        if [ "$fails" -ge "$max_fails" ]; then
          say "exit $code, $fails failures in a row with the venue open: not restarting"
          echo "$code" >"$state/STOPPED"
          exit "$code"
        fi
        say "exit $code (failure $fails of $max_fails with the venue open): retrying in ${fails} min"
        sleep $((minute * fails))
      fi
      ;;
  esac
done
