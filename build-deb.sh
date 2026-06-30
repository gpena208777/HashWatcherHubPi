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
VERSION="${1:-1.0.11}"
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
    VERSION
    hashwatcher_hub_agent.py
    hub_ble_provisioner.py
    tailscale_setup.py
    ota_update_helper.sh
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

# Stamp the package version so the update agent knows what's installed.
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
elif command -v bsdtar &>/dev/null && command -v python3 &>/dev/null; then
    echo "Building .deb with local bsdtar/python..."
    rm -f "${DEB_OUT}"
    printf '2.0\n' > "${SCRIPT_DIR}/_build/debian-binary"
    COPYFILE_DISABLE=1 bsdtar --format ustar --uid 0 --gid 0 --uname root --gname root \
        -C "${PKG_DIR}/DEBIAN" -cJf "${SCRIPT_DIR}/_build/control.tar.xz" .
    COPYFILE_DISABLE=1 bsdtar --format ustar --uid 0 --gid 0 --uname root --gname root \
        --exclude './DEBIAN' -C "${PKG_DIR}" -cJf "${SCRIPT_DIR}/_build/data.tar.xz" .
    python3 - "${DEB_OUT}" \
        "${SCRIPT_DIR}/_build/debian-binary" \
        "${SCRIPT_DIR}/_build/control.tar.xz" \
        "${SCRIPT_DIR}/_build/data.tar.xz" <<'PY'
import os
import sys

out_path = sys.argv[1]
members = sys.argv[2:]

with open(out_path, "wb") as out:
    out.write(b"!<arch>\n")
    for path in members:
        name = os.path.basename(path)
        data = open(path, "rb").read()
        header = (
            f"{name:<16}"
            f"{0:<12}"
            f"{0:<6}"
            f"{0:<6}"
            f"{'100644':<8}"
            f"{len(data):<10}"
            "`\n"
        )
        out.write(header.encode("ascii"))
        out.write(data)
        if len(data) % 2:
            out.write(b"\n")
PY
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
