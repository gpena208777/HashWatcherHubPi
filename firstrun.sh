#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# HashWatcher Gateway — First-Boot Auto-Installer
#
# This script runs once on first boot when using a pre-built image.
# It downloads and runs the full installer, then disables itself.
#
# To embed in a Pi image:
#   1. Flash Pi OS to an SD card
#   2. Mount the boot partition
#   3. Copy this file to /boot/firmware/firstrun.sh
#   4. Add to /boot/firmware/cmdline.txt (at the end of the line):
#        systemd.run=/boot/firmware/firstrun.sh
#
# Or use the systemd service method (more reliable):
#   1. Copy this to /opt/hashwatcher-firstrun.sh on the image
#   2. Create /etc/systemd/system/hashwatcher-firstrun.service
#   3. Enable it: systemctl enable hashwatcher-firstrun
# ─────────────────────────────────────────────────────────────────────
set -euo pipefail

MARKER="/opt/.hashwatcher-installed"
LOG="/var/log/hashwatcher-firstrun.log"

# Only run once
if [[ -f "${MARKER}" ]]; then
    exit 0
fi

exec > >(tee -a "${LOG}") 2>&1
echo "$(date): HashWatcher first-run installer starting..."

# Wait for network
for i in $(seq 1 30); do
    if ping -c1 -W2 github.com &>/dev/null; then
        break
    fi
    echo "Waiting for network... (${i}/30)"
    sleep 2
done

# Run the main installer
curl -fsSL https://raw.githubusercontent.com/gpena208777/HashWatcherHubPi/main/install.sh | bash

# Mark as done so it never runs again
touch "${MARKER}"
echo "$(date): HashWatcher first-run complete."

# Disable this service so it doesn't run on next boot
systemctl disable hashwatcher-firstrun.service 2>/dev/null || true
