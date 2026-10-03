#!/usr/bin/env bash
set -euo pipefail

if (( EUID != 0 )); then
    echo 'Run this script as root.' >&2
    exit 1
fi
if [[ -f /etc/systemd/system/skvoz-auto-update.timer ]]; then
    systemctl disable --now skvoz-auto-update.timer
    systemctl stop skvoz-auto-update.service
fi
echo 'SKVOZ auto-update disabled.'
