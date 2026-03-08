#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# Build a pre-configured HashWatcher Gateway Pi image
#
# This takes a stock Raspberry Pi OS image and injects the first-run
# installer so HashWatcher Gateway is set up automatically on first boot.
#
# Prerequisites:
#   - macOS with Homebrew, or Linux
#   - A stock Raspberry Pi OS Lite (64-bit) .img file
#
# Usage:
#   ./build-image.sh /path/to/2024-xx-xx-raspios-bookworm-arm64-lite.img
#
# The user still needs to configure Wi-Fi via Raspberry Pi Imager
# (or by editing wpa_supplicant.conf on the boot partition).
# ─────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_IMG="${1:-}"

if [[ -z "${INPUT_IMG}" || ! -f "${INPUT_IMG}" ]]; then
    echo "Usage: $0 <path-to-raspios.img>"
    echo ""
    echo "Download Pi OS Lite (64-bit) from:"
    echo "  https://www.raspberrypi.com/software/operating-systems/"
    exit 1
fi

OUTPUT_IMG="${INPUT_IMG%.img}-hashwatcher.img"
cp "${INPUT_IMG}" "${OUTPUT_IMG}"

echo "Image copied to: ${OUTPUT_IMG}"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "MANUAL STEPS (image modification requires mounting the partitions):"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "On Linux (or a Pi):"
echo ""
echo "  # Mount the rootfs partition (partition 2)"
echo "  sudo losetup -fP ${OUTPUT_IMG}"
echo "  LOOP=\$(losetup -j ${OUTPUT_IMG} | cut -d: -f1)"
echo "  sudo mkdir -p /mnt/pi-rootfs"
echo "  sudo mount \${LOOP}p2 /mnt/pi-rootfs"
echo ""
echo "  # Copy the first-run script and service"
echo "  sudo cp ${SCRIPT_DIR}/firstrun.sh /mnt/pi-rootfs/opt/hashwatcher-firstrun.sh"
echo "  sudo chmod +x /mnt/pi-rootfs/opt/hashwatcher-firstrun.sh"
echo "  sudo cp ${SCRIPT_DIR}/hashwatcher-firstrun.service /mnt/pi-rootfs/etc/systemd/system/"
echo "  sudo ln -sf /etc/systemd/system/hashwatcher-firstrun.service /mnt/pi-rootfs/etc/systemd/system/multi-user.target.wants/"
echo ""
echo "  # Enable SSH by default"
echo "  sudo mkdir -p /mnt/pi-rootfs/boot/firmware"
echo "  sudo touch /mnt/pi-rootfs/boot/firmware/ssh"
echo ""
echo "  # Set default hostname"
echo "  echo 'HashWatcherHub' | sudo tee /mnt/pi-rootfs/etc/hostname"
echo ""
echo "  # Enable USB gadget mode (dwc2 + g_ether) for Pi Zero 2 W / Pi 4 / Pi 5"
echo "  CONFIG=/mnt/pi-rootfs/boot/firmware/config.txt"
echo "  [ ! -f \"\$CONFIG\" ] && CONFIG=/mnt/pi-rootfs/boot/config.txt"
echo "  CMDLINE=/mnt/pi-rootfs/boot/firmware/cmdline.txt"
echo "  [ ! -f \"\$CMDLINE\" ] && CMDLINE=/mnt/pi-rootfs/boot/cmdline.txt"
echo "  grep -q 'dtoverlay=dwc2' \"\$CONFIG\" || echo 'dtoverlay=dwc2' | sudo tee -a \"\$CONFIG\" >/dev/null"
echo "  sudo sed -i 's/\$/ modules-load=dwc2,g_ether/' \"\$CMDLINE\""
echo ""
echo "  # Unmount"
echo "  sudo umount /mnt/pi-rootfs"
echo "  sudo losetup -d \${LOOP}"
echo ""
echo "Then flash ${OUTPUT_IMG} with Raspberry Pi Imager."
echo "The user only needs to configure Wi-Fi in the Imager settings."
echo "HashWatcher Gateway installs automatically on first boot (~3 min)."
