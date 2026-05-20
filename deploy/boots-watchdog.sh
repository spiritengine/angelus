#!/bin/sh
set -eu

# Advisory only: run this from system cron/systemd to watch the belfry
# liveness sentinel that belfry touches on each tick.
#
# Example crontab entry:
# */10 * * * * ANGELUS_ROOT=/opt/angelus ANGELUS_BOOTS_NOTIFY='mail -s "angelus boots alert" you@example.com' /opt/angelus/deploy/boots-watchdog.sh
#
# Expected configuration:
# - ANGELUS_ROOT: angelus checkout/deploy root. Defaults to the parent of
#   this script's deploy/ directory.
# - ANGELUS_BELFRY_SENTINEL_PATH: sentinel file to check. Defaults to
#   "$ANGELUS_ROOT/state/belfry-pinged-at", matching belfry's default.
# - ANGELUS_BOOTS_STALE_MINUTES: alert when the sentinel is older than
#   this many minutes. Defaults to 30.
# - ANGELUS_BOOTS_NOTIFY: shell command that reads the alert message on
#   stdin. Defaults to 'logger -t angelus-boots'.

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ANGELUS_ROOT=${ANGELUS_ROOT:-$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)}
ANGELUS_BELFRY_SENTINEL_PATH=${ANGELUS_BELFRY_SENTINEL_PATH:-"$ANGELUS_ROOT/state/belfry-pinged-at"}
ANGELUS_BOOTS_STALE_MINUTES=${ANGELUS_BOOTS_STALE_MINUTES:-30}
ANGELUS_BOOTS_NOTIFY=${ANGELUS_BOOTS_NOTIFY:-'logger -t angelus-boots'}

case "$ANGELUS_BOOTS_STALE_MINUTES" in
    ''|*[!0-9]*)
        echo "ANGELUS_BOOTS_STALE_MINUTES must be an integer number of minutes" >&2
        exit 2
        ;;
esac

message=
if [ ! -e "$ANGELUS_BELFRY_SENTINEL_PATH" ]; then
    message="angelus boots: belfry sentinel missing at $ANGELUS_BELFRY_SENTINEL_PATH"
elif find "$ANGELUS_BELFRY_SENTINEL_PATH" -mmin +"$ANGELUS_BOOTS_STALE_MINUTES" -print -quit | grep -q .; then
    message="angelus boots: belfry sentinel stale (> ${ANGELUS_BOOTS_STALE_MINUTES}m) at $ANGELUS_BELFRY_SENTINEL_PATH"
fi

if [ -n "$message" ]; then
    printf '%s\n' "$message" | sh -c "$ANGELUS_BOOTS_NOTIFY"
fi
