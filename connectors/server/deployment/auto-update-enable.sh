#!/usr/bin/env bash
set -euo pipefail

# The installed copy is also the timer's worker.
if [[ "${1:-}" == '--run' ]]; then
    source "${2:-/etc/skvoz-auto-update/settings.sh}"
    for variable in ${!SKVOZ_@} ${!COMPOSE_@}; do unset "$variable"; done
    export COMPOSE_DISABLE_ENV_FILE=1
    compose=("$docker_bin" compose -p "$project_name" -f "$compose_file")
    log=$(mktemp)
    trap 'rm -f "$log"' EXIT
    trap 'exit 143' TERM
    trap 'exit 130' INT
    update() {
        local running
        running=$("${compose[@]}" ps --status running -q server) || return
        [[ -n "$running" ]] || return 0
        "${compose[@]}" pull --quiet server || return
        running=$("${compose[@]}" ps --status running -q server) || return
        [[ -n "$running" ]] || return 0
        "${compose[@]}" up -d --no-deps --no-build --pull never server
    }
    if update >"$log" 2>&1; then
        exit 0
    else
        status=$?
        printf 'SKVOZ auto-update failed (exit %s).\n' "$status" >&2
        cat "$log" >&2
        exit "$status"
    fi
fi

if (( EUID != 0 )); then
    echo 'Run this script as root.' >&2
    exit 1
fi
if (( $# > 2 )); then
    echo 'Usage: auto-update-enable.sh [COMPOSE_FILE [PROJECT_NAME]]' >&2
    exit 1
fi

directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
compose_file=$(realpath -e -- "${1:-$directory/../compose.yaml}")
docker_bin=$(command -v docker)
docker_bin=$(realpath -e -- "$docker_bin")
[[ -f "$compose_file" ]]
# Use the same self-contained YAML from the shell and systemd.
for variable in ${!SKVOZ_@} ${!COMPOSE_@}; do unset "$variable"; done
export COMPOSE_DISABLE_ENV_FILE=1
compose=("$docker_bin" compose -f "$compose_file")
if (( $# == 2 )); then compose+=(-p "$2"); fi
"${compose[@]}" config --quiet
image=$("${compose[@]}" config --images server)
if [[ "$image" == *@* ]]; then
    echo 'Auto-update requires a mutable registry tag, not a pinned digest.' >&2
    exit 1
fi
container=$("${compose[@]}" ps --status running -q server)
if [[ -z "$container" || "$container" == *$'\n'* ]]; then
    echo 'Start exactly one server with this Compose configuration before enabling auto-update.' >&2
    exit 1
fi
project_name=$("$docker_bin" inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}' "$container")
if [[ -z "$project_name" || "$project_name" == '<no value>' ]]; then
    echo 'The server container has no Compose project label.' >&2
    exit 1
fi

umask 077
install -d -m 0700 /etc/skvoz-auto-update
install -d -m 0755 /usr/local/lib/skvoz-auto-update
# Stop the old job before replacing its settings on a repeated enable.
if [[ -f /etc/systemd/system/skvoz-auto-update.timer ]]; then
    systemctl stop skvoz-auto-update.timer skvoz-auto-update.service
fi
install -m 0755 "${BASH_SOURCE[0]}" /usr/local/lib/skvoz-auto-update/auto-update.sh
printf 'docker_bin=%q\ncompose_file=%q\nproject_name=%q\n' \
    "$docker_bin" "$compose_file" "$project_name" > /etc/skvoz-auto-update/settings.sh
chmod 0600 /etc/skvoz-auto-update/settings.sh

cat > /etc/systemd/system/skvoz-auto-update.service <<'UNIT'
[Unit]
Description=Update the SKVOZ server image
Wants=network-online.target
After=network-online.target docker.service

[Service]
Type=oneshot
ExecStart=/usr/local/lib/skvoz-auto-update/auto-update.sh --run
TimeoutStartSec=10min
StandardOutput=null
StandardError=journal
SyslogLevel=err
LogLevelMax=err
UNIT

cat > /etc/systemd/system/skvoz-auto-update.timer <<'UNIT'
[Unit]
Description=Check the SKVOZ server image every two minutes

[Timer]
OnActiveSec=2min
OnUnitInactiveSec=2min
AccuracySec=1s

[Install]
WantedBy=timers.target
UNIT
chmod 0644 /etc/systemd/system/skvoz-auto-update.{service,timer}
systemctl daemon-reload
systemctl enable --now skvoz-auto-update.timer
echo 'SKVOZ auto-update enabled: two minutes between checks; errors only.'
