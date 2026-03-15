#!/usr/bin/env bash
# Deploy pi-agent to a Raspberry Pi connected over USB (gadget mode) or LAN IP.
#
# Prerequisites:
# - Pi connected via USB cable (OTG port) with gadget mode enabled, OR
# - Pi reachable on the local network (e.g. BLE-reported IP like 192.168.x.x)
#
# Auto-detection order (when no IP given):
#   1. 169.254.75.1  — HashWatcher usb0-gadget.service
#   2. 10.12.194.1   — rpi-usb-gadget SHARED mode
#   3. 169.254.234.123 — older dtoverlay/dwc2 setups
#   4. HashWatcherHub.local — mDNS
#   5. raspberrypi.local — mDNS fallback
#
# Usage:
#   ./install_to_pi_usb.sh                          # auto-detect
#   ./install_to_pi_usb.sh 192.168.0.53             # BLE-reported LAN IP
#   ./install_to_pi_usb.sh 169.254.75.1             # explicit USB gadget IP
#   ./install_to_pi_usb.sh 192.168.0.53 hashwatcherhub  # IP and username

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PI_HOST="${1:-}"
PI_USER="${2:-hashwatcherhub}"

SSH_OPTS=(-o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)

CANDIDATE_IPS=(
  169.254.75.1
  10.12.194.1
  169.254.234.123
)
CANDIDATE_MDNS=(
  HashWatcherHub.local
  raspberrypi.local
)

can_reach() {
  ping -c 1 -W 2 "$1" &>/dev/null
}

find_pi() {
  if [[ -n "$PI_HOST" ]]; then
    if can_reach "$PI_HOST"; then
      echo "$PI_HOST"
      return
    fi
    echo "Specified host $PI_HOST not reachable." >&2
    return 1
  fi

  echo "Auto-detecting Pi..." >&2
  for ip in "${CANDIDATE_IPS[@]}"; do
    if can_reach "$ip"; then
      echo "$ip"
      return
    fi
    echo "  $ip — not reachable" >&2
  done
  for name in "${CANDIDATE_MDNS[@]}"; do
    if can_reach "$name"; then
      echo "$name"
      return
    fi
    echo "  $name — not reachable" >&2
  done

  echo "No Pi found. Pass the IP explicitly (e.g. from BLE: ./install_to_pi_usb.sh 192.168.0.53)" >&2
  return 1
}

RESOLVED="$(find_pi)" || exit 1

echo "=== HashWatcher Hub — USB/LAN Deploy ==="
echo "Target: ${PI_USER}@${RESOLVED}"
echo ""

exec "${SCRIPT_DIR}/install_to_pi.sh" "${RESOLVED}" "${PI_USER}"
