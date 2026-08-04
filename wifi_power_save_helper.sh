#!/usr/bin/env bash
# Keep the Raspberry Pi Wi-Fi radio awake. The BCM43430 used by the Pi Zero 2 W
# can remain associated while power saving prevents timely traffic, which makes
# an always-on Tailscale subnet router appear offline.
set -euo pipefail

INTERFACE="${1:-wlan0}"
NM_CONF_DIR="/etc/NetworkManager/conf.d"
NM_CONF_PATH="${NM_CONF_DIR}/99-hashwatcher-wifi-powersave.conf"

mkdir -p "${NM_CONF_DIR}"
cat > "${NM_CONF_PATH}" <<'EOF'
# Installed by HashWatcher Hub Pi. Keep Wi-Fi responsive for always-on remote
# access. NetworkManager value 2 means power saving is disabled.
[connection]
wifi.powersave = 2
EOF
chmod 0644 "${NM_CONF_PATH}"

# Existing profiles can override the global default, so update the active one.
if command -v nmcli >/dev/null 2>&1; then
    connection="$(nmcli -g GENERAL.CONNECTION device show "${INTERFACE}" 2>/dev/null | head -n1 || true)"
    if [[ -n "${connection}" && "${connection}" != "--" ]]; then
        nmcli connection modify "${connection}" 802-11-wireless.powersave 2 || true
        nmcli device reapply "${INTERFACE}" || true
    fi
fi

# Apply immediately; the NetworkManager configuration keeps this after a
# reconnect or reboot.
if command -v iw >/dev/null 2>&1; then
    iw dev "${INTERFACE}" set power_save off || true
fi
