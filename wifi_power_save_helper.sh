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
# This persists the setting, but NetworkManager cannot apply it to an already
# associated device. Do not use `nmcli device reapply` here: it only logs an
# expected error and does not change the radio's current state.
if command -v nmcli >/dev/null 2>&1; then
    connection="$(nmcli -g GENERAL.CONNECTION device show "${INTERFACE}" 2>/dev/null | head -n1 || true)"
    if [[ -n "${connection}" && "${connection}" != "--" ]]; then
        nmcli connection modify "${connection}" 802-11-wireless.powersave 2 || true
        nmcli connection reload || true
    fi
fi

# Disable it in the Wi-Fi driver immediately. NetworkManager's global setting
# and the saved profile above ensure the setting survives a reconnect or reboot.
if ! command -v iw >/dev/null 2>&1; then
    echo "[HashWatcher] ERROR: 'iw' is required to disable Wi-Fi power saving immediately." >&2
    exit 1
fi

iw dev "${INTERFACE}" set power_save off
power_save_state="$(iw dev "${INTERFACE}" get power_save)"
printf '%s\n' "${power_save_state}"
if ! grep -qx 'Power save: off' <<<"${power_save_state}"; then
    echo "[HashWatcher] ERROR: Wi-Fi power saving is still enabled on ${INTERFACE}." >&2
    exit 1
fi
