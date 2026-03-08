#!/usr/bin/env bash
# Run this on your Mac when the Pi SD card boot partition is mounted (e.g. /Volumes/bootfs).
# It copies the USB gadget setup script and adds cloud-init so the Pi gets 169.254.75.1 on first boot.
#
# Usage: ./prepare-sd-usb-firstboot.sh [/Volumes/bootfs]

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOOT="${1:-}"

if [[ -z "${BOOT}" ]]; then
    if [[ -d /Volumes/bootfs ]]; then
        BOOT=/Volumes/bootfs
    else
        echo "Usage: $0 /Volumes/bootfs"
        echo "Or mount the SD card and run: $0 /Volumes/<boot-volume-name>"
        exit 1
    fi
fi

# Accept config.txt in either /boot or /boot/firmware layout
CONFIG="${BOOT}/config.txt"
CMDLINE="${BOOT}/cmdline.txt"
[[ -f "${CONFIG}" ]] || { echo "Not a Pi boot partition (no config.txt): ${BOOT}"; exit 1; }

echo "Preparing USB first-boot on ${BOOT}..."

# ── Enable USB gadget kernel support (dwc2 + g_ether) ────────────────
if grep -q 'dtoverlay=dwc2' "${CONFIG}"; then
    echo "dtoverlay=dwc2 already in config.txt."
else
    echo 'dtoverlay=dwc2' >> "${CONFIG}"
    echo "Added dtoverlay=dwc2 to config.txt."
fi

if [[ -f "${CMDLINE}" ]]; then
    if grep -q 'modules-load=dwc2,g_ether' "${CMDLINE}"; then
        echo "modules-load=dwc2,g_ether already in cmdline.txt."
    else
        sed -i.bak 's/$/ modules-load=dwc2,g_ether/' "${CMDLINE}"
        rm -f "${CMDLINE}.bak"
        echo "Added modules-load=dwc2,g_ether to cmdline.txt."
    fi
else
    echo "WARNING: cmdline.txt not found at ${CMDLINE}. You may need to add 'modules-load=dwc2,g_ether' manually."
fi

# ── Copy setup script to boot partition ───────────────────────────────
cp "${SCRIPT_DIR}/setup-usb0-on-boot.sh" "${BOOT}/"
chmod 755 "${BOOT}/setup-usb0-on-boot.sh"

# ── Ensure cloud-init runs our script on first boot ──────────────────
USER_DATA="${BOOT}/user-data"
USB_RUN="# HashWatcher: enable USB gadget ethernet and assign 169.254.75.1 to usb0
runcmd:
  - /bin/sh /boot/firmware/setup-usb0-on-boot.sh
"

if [[ ! -f "${USER_DATA}" ]]; then
    echo "#cloud-config
${USB_RUN}" > "${USER_DATA}"
    echo "Created ${USER_DATA} with USB gadget first-boot."
else
    if grep -q "setup-usb0-on-boot" "${USER_DATA}"; then
        echo "user-data already has USB gadget runcmd."
    else
        echo ""
        echo "user-data already exists (e.g. from Pi Imager). Add this under runcmd:"
        echo "  - /bin/sh /boot/firmware/setup-usb0-on-boot.sh"
        echo "Or add a new line to the runcmd list if one exists."
    fi
fi

echo "Done. Eject the SD card and boot the Pi with it connected via USB to your Mac."
echo "The Pi will be at 169.254.75.1 after first boot."
