#!/bin/bash
set -euo pipefail

LAST_WIFI_PATH="${LAST_WIFI_PATH:-/opt/hashwatcher-hub-pi/last_wifi_credentials.json}"

rm -f "${LAST_WIFI_PATH}" 2>/dev/null || true

# Remove all known NetworkManager wireless profiles.
if command -v nmcli >/dev/null 2>&1; then
    nmcli -t -f NAME,TYPE connection show 2>/dev/null | while IFS=: read -r name type; do
        if echo "${type}" | grep -q wireless; then
            nmcli connection delete "${name}" 2>/dev/null || true
        fi
    done
    nmcli device disconnect wlan0 2>/dev/null || true
fi

# Clear runtime NM connection files as belt-and-suspenders.
rm -f /run/NetworkManager/system-connections/*wlan*.nmconnection 2>/dev/null || true
rm -f /run/NetworkManager/system-connections/*wireless*.nmconnection 2>/dev/null || true

# Remove netplan files containing Wi-Fi config.
for yf in /etc/netplan/*.yaml; do
    [ -f "${yf}" ] || continue
    if grep -qE 'wifis:|access-points:' "${yf}" 2>/dev/null; then
        rm -f "${yf}"
    fi
done

# Strip the wifi stanza from /boot/firmware/network-config if present.
BOOT_CFG="/boot/firmware/network-config"
if [ -f "${BOOT_CFG}" ]; then
    python3 - "${BOOT_CFG}" <<'PY'
import pathlib
import sys

p = pathlib.Path(sys.argv[1])
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines(keepends=True)
out = []
skip = False
for line in lines:
    stripped = line.lstrip()
    if stripped.startswith("wifis:"):
        skip = True
        continue
    if skip:
        if line[:1] in (" ", "\t") or stripped == "":
            continue
        skip = False
    out.append(line)
p.write_text("".join(out), encoding="utf-8")
PY
fi

# Prevent cloud-init from re-writing network config on next boot.
mkdir -p /etc/cloud/cloud.cfg.d
printf '%s\n' 'network: {config: disabled}' > /etc/cloud/cloud.cfg.d/99-disable-network-config.cfg

if command -v netplan >/dev/null 2>&1; then
    netplan apply 2>/dev/null || true
fi
