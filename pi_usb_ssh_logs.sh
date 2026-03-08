#!/usr/bin/env bash
# Detect a Raspberry Pi over USB gadget or LAN and SSH in to read Tailscale / WiFi logs.
#
# Detection order (when no IP given):
#   1. USB gadget IPs: 169.254.75.1, 10.12.194.1, 169.254.234.123
#   2. mDNS: HashWatcherHub.local, raspberrypi.local
#   3. LAN scan: ARP table entries on 192.168.x.x / 10.x.x.x that respond to SSH on port 22
#
# You can also pass the BLE-reported IP directly (e.g. 192.168.0.53):
#   ./pi_usb_ssh_logs.sh 192.168.0.53
#   ./pi_usb_ssh_logs.sh 192.168.0.53 pi
#   ./pi_usb_ssh_logs.sh                     # auto-detect (USB gadget → mDNS → ARP)

set -euo pipefail

PI_HOST="${1:-}"
PI_USER="${2:-pi}"

USB_GADGET_IPS=(
  169.254.75.1
  10.12.194.1
  169.254.234.123
)

MDNS_NAMES=(
  HashWatcherHub.local
  raspberrypi.local
)

SSH_OPTS=(-o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new -o BatchMode=yes)

# ── helpers ───────────────────────────────────────────────────────────

show_usb_interfaces() {
  echo "=== Mac network interfaces (USB gadget often appears as enX) ==="
  if command -v networksetup &>/dev/null; then
    networksetup -listallhardwareports 2>/dev/null | grep -A2 -i "Ethernet\|USB\|Thunderbolt" || true
  fi
  ifconfig 2>/dev/null | grep -E '^en[0-9]|inet 169\.254|inet 10\.12\.' || true
  echo ""
}

can_reach() {
  local host="$1"
  ping -c 1 -W 2 "$host" &>/dev/null && return 0
  ssh "${SSH_OPTS[@]}" "$PI_USER@$host" "exit" 2>/dev/null && return 0
  return 1
}

# Try to find Pi IPs from the ARP table (picks up any device the Mac has talked to recently)
arp_candidates() {
  arp -a 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | grep -v '255\|\.0$' | sort -u
}

find_pi() {
  # 1. Explicit host
  if [[ -n "$PI_HOST" ]]; then
    if can_reach "$PI_HOST"; then
      echo "$PI_HOST"
      return
    fi
    echo "Specified host $PI_HOST not reachable." >&2
    return 1
  fi

  # 2. USB gadget IPs
  echo "Trying USB gadget IPs..." >&2
  for ip in "${USB_GADGET_IPS[@]}"; do
    if can_reach "$ip"; then
      echo "$ip"
      return
    fi
    echo "  $ip — not reachable" >&2
  done

  # 3. mDNS names
  echo "Trying mDNS..." >&2
  for name in "${MDNS_NAMES[@]}"; do
    if can_reach "$name"; then
      echo "$name"
      return
    fi
    echo "  $name — not reachable" >&2
  done

  # 4. ARP table scan — look for a host running SSH that responds like a Pi
  echo "Scanning ARP table for SSH-capable hosts..." >&2
  for ip in $(arp_candidates); do
    if ssh "${SSH_OPTS[@]}" -o ConnectTimeout=2 "$PI_USER@$ip" "test -f /etc/rpi-issue 2>/dev/null || hostname | grep -qi 'pi\|hashwatcher'" 2>/dev/null; then
      echo "$ip"
      return
    fi
  done

  cat >&2 <<'MSG'
No Pi found. Options:
  • Plug the Pi in via USB (OTG) and ensure gadget mode is enabled
  • Pass the BLE-reported IP:  ./pi_usb_ssh_logs.sh 192.168.0.53
  • Pass any reachable IP:     ./pi_usb_ssh_logs.sh <ip> [user]
MSG
  return 1
}

# ── remote log collection ─────────────────────────────────────────────

fetch_logs() {
  local host="$1"
  echo "=== SSH ${PI_USER}@${host} — Tailscale & WiFi error logs ==="
  echo ""

  # Drop BatchMode for the actual session (may need password prompt)
  ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new "$PI_USER@$host" bash -s << 'REMOTE'
set -uo pipefail

echo "────────────────────────────────────────────────"
echo "HOSTNAME: $(hostname 2>/dev/null || echo unknown)"
echo "UPTIME:   $(uptime 2>/dev/null || echo unknown)"
echo "IP ADDRS:"
ip -4 addr show 2>/dev/null | grep 'inet ' || ifconfig 2>/dev/null | grep 'inet '
echo "────────────────────────────────────────────────"
echo ""

echo "=== tailscaled service status ==="
systemctl status tailscaled --no-pager 2>/dev/null || echo "(tailscaled unit not found)"
echo ""

echo "=== tailscale status ==="
tailscale status 2>/dev/null || echo "(tailscale CLI not found or not running)"
echo ""

echo "=== tailscaled errors (last 200 lines) ==="
journalctl -u tailscaled --no-pager -n 200 -p err..emerg 2>/dev/null || true
journalctl -u tailscaled --no-pager -n 200 2>/dev/null | grep -i -E 'error|fail|fatal|denied|timeout|refused|panic' || echo "(no error lines)"
echo ""

echo "=== tailscaled full log (last 100 lines) ==="
journalctl -u tailscaled --no-pager -n 100 2>/dev/null || echo "(no tailscaled logs)"
echo ""

echo "=== WiFi / wlan status ==="
iwconfig 2>/dev/null || true
wpa_cli status 2>/dev/null || true
echo ""

echo "=== wpa_supplicant log (last 80 lines) ==="
journalctl -u wpa_supplicant --no-pager -n 80 2>/dev/null || true
echo ""

echo "=== dhcpcd / dhclient log (last 50 lines) ==="
journalctl -u dhcpcd --no-pager -n 50 2>/dev/null || true
journalctl -u dhclient --no-pager -n 50 2>/dev/null || true
echo ""

echo "=== NetworkManager log (last 50 lines) ==="
journalctl -u NetworkManager --no-pager -n 50 2>/dev/null || true
echo ""

echo "=== System journal wifi/wlan/network errors (last 200 lines) ==="
journalctl --no-pager -n 500 -p err..emerg 2>/dev/null | grep -i -E 'wifi|wlan|network|dhcp|wpa|tailscale|dns' || echo "(no matching error lines)"
echo ""

echo "=== dmesg: wifi / USB gadget / tailscale ==="
dmesg 2>/dev/null | grep -i -E 'wlan|wifi|tailscale|rndis|g_ether|usb0|dwc2|gadget|firmware' | tail -100 || echo "(no matching dmesg)"
echo ""

echo "=== USB gadget service ==="
systemctl status usb0-gadget --no-pager 2>/dev/null || echo "(usb0-gadget unit not found)"
echo ""

echo "=== HashWatcher hub agent log (last 50 lines) ==="
journalctl -u hashwatcher-hub-pi --no-pager -n 50 2>/dev/null || true
echo ""

echo "=== /etc/wpa_supplicant/wpa_supplicant.conf (redacted) ==="
if [[ -f /etc/wpa_supplicant/wpa_supplicant.conf ]]; then
  grep -v 'psk=' /etc/wpa_supplicant/wpa_supplicant.conf 2>/dev/null || true
else
  echo "(not found)"
fi
echo ""

echo "=== Done ==="
REMOTE
}

# ── main ──────────────────────────────────────────────────────────────

show_usb_interfaces

echo "=== Looking for Pi ==="
HOST="$(find_pi)" || exit 1
echo "Found Pi at: $HOST"
echo ""

fetch_logs "$HOST"
