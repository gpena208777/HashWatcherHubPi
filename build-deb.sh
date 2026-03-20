#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# Build a .deb package for the HashWatcher Hub Pi agent.
#
# Usage:
#   cd pi-agent && ./build-deb.sh [version]
#
# Example:
#   ./build-deb.sh 1.2.0
#
# Output:
#   hashwatcher-hub-pi_<version>_all.deb
#
# The .deb is architecture: all (pure Python) and installs to:
#   /opt/hashwatcher-hub-pi/    — Python scripts + requirements.txt
#   /etc/hashwatcher-hub-pi/    — hub.env config
#   /etc/systemd/system/        — service unit files
# ─────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION="${1:-1.0.1}"
PKG_NAME="hashwatcher-hub-pi"
PKG_DIR="${SCRIPT_DIR}/_build/${PKG_NAME}_${VERSION}_all"

echo "Building ${PKG_NAME} v${VERSION}..."

# Clean previous build
rm -rf "${SCRIPT_DIR}/_build"
mkdir -p "${PKG_DIR}"

# ── DEBIAN metadata ──────────────────────────────────────────────────

mkdir -p "${PKG_DIR}/DEBIAN"

sed "s/__VERSION__/${VERSION}/" "${SCRIPT_DIR}/debian/control" \
    > "${PKG_DIR}/DEBIAN/control"

cp "${SCRIPT_DIR}/debian/conffiles" "${PKG_DIR}/DEBIAN/conffiles"
cp "${SCRIPT_DIR}/debian/postinst"  "${PKG_DIR}/DEBIAN/postinst"
cp "${SCRIPT_DIR}/debian/prerm"     "${PKG_DIR}/DEBIAN/prerm"
cp "${SCRIPT_DIR}/debian/postrm"    "${PKG_DIR}/DEBIAN/postrm"
chmod 0755 "${PKG_DIR}/DEBIAN/postinst" "${PKG_DIR}/DEBIAN/prerm" "${PKG_DIR}/DEBIAN/postrm"

# ── Application files ────────────────────────────────────────────────

INSTALL_DIR="${PKG_DIR}/opt/hashwatcher-hub-pi"
CONFIG_DIR="${PKG_DIR}/etc/hashwatcher-hub-pi"
SYSTEMD_DIR="${PKG_DIR}/etc/systemd/system"

mkdir -p "${INSTALL_DIR}" "${CONFIG_DIR}" "${SYSTEMD_DIR}"

PYTHON_FILES=(
    hashwatcher_hub_agent.py
    hub_ble_provisioner.py
    tailscale_setup.py
    requirements.txt
    icon.png
)

for f in "${PYTHON_FILES[@]}"; do
    if [[ -f "${SCRIPT_DIR}/${f}" ]]; then
        cp "${SCRIPT_DIR}/${f}" "${INSTALL_DIR}/"
    else
        echo "WARNING: ${f} not found, skipping"
    fi
done

# Stamp the version so the update agent knows what's installed
echo "${VERSION}" > "${INSTALL_DIR}/VERSION"

# ── Default config (won't overwrite existing on upgrade) ─────────────

cp "${SCRIPT_DIR}/hub.env.prepared" "${CONFIG_DIR}/hub.env"

# ── Systemd unit files ───────────────────────────────────────────────

cat > "${SYSTEMD_DIR}/hashwatcher-hub-pi.service" <<'UNIT'
[Unit]
Description=HashWatcher Hub Pi Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=hashwatcher-hub-pi
Group=hashwatcher-hub-pi
WorkingDirectory=/opt/hashwatcher-hub-pi
EnvironmentFile=/etc/hashwatcher-hub-pi/hub.env
ExecStart=/opt/hashwatcher-hub-pi/.venv/bin/python /opt/hashwatcher-hub-pi/hashwatcher_hub_agent.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

cat > "${SYSTEMD_DIR}/hashwatcher-ble-provisioner.service" <<'UNIT'
[Unit]
Description=HashWatcher BLE Wi-Fi Provisioner
After=bluetooth.service network-pre.target
Wants=bluetooth.service

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=/opt/hashwatcher-hub-pi
EnvironmentFile=-/etc/hashwatcher-hub-pi/hub.env
ExecStartPre=/bin/sleep 2
ExecStartPre=/usr/sbin/rfkill unblock bluetooth
ExecStartPre=/bin/sleep 1
ExecStartPre=/usr/bin/hciconfig hci0 up
ExecStartPre=/bin/sleep 1
ExecStart=/opt/hashwatcher-hub-pi/.venv/bin/python /opt/hashwatcher-hub-pi/hub_ble_provisioner.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

# ── Build the .deb ───────────────────────────────────────────────────

DEB_OUT="${SCRIPT_DIR}/${PKG_NAME}_${VERSION}_all.deb"
if command -v dpkg-deb &>/dev/null; then
    dpkg-deb --build "${PKG_DIR}" "${DEB_OUT}" 2>/dev/null || {
        echo ""
        echo "Local dpkg-deb failed. To build with Docker, run:"
        echo "  docker run --rm -v \"${SCRIPT_DIR}:/work\" -w /work debian:bookworm-slim dpkg-deb --build /work/_build/${PKG_NAME}_${VERSION}_all /work/${PKG_NAME}_${VERSION}_all.deb"
        exit 1
    }
else
    echo "Building .deb via Docker..."
    docker run --rm -v "${SCRIPT_DIR}:/work" -w /work debian:bookworm-slim \
        dpkg-deb --build "/work/_build/${PKG_NAME}_${VERSION}_all" "/work/${PKG_NAME}_${VERSION}_all.deb" || {
        echo "Docker build failed. Try:"
        echo "  docker run --rm -v \"${SCRIPT_DIR}:/work\" -w /work debian:bookworm-slim dpkg-deb --build /work/_build/${PKG_NAME}_${VERSION}_all /work/${PKG_NAME}_${VERSION}_all.deb"
        exit 1
    }
fi

rm -rf "${SCRIPT_DIR}/_build"

echo ""
echo "Built: ${DEB_OUT}"
echo "Size:  $(du -h "${DEB_OUT}" | cut -f1)"
echo ""
echo "To install on a Pi:"
echo "  sudo dpkg -i ${PKG_NAME}_${VERSION}_all.deb"
echo ""
echo "To upload as a GitHub Release:"
echo "  gh release create v${VERSION} ${DEB_OUT} --repo gpena208777/HashWatcherHubPi --title \"v${VERSION}\" --notes \"HashWatcher Hub Pi v${VERSION}\""
