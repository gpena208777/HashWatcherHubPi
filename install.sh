#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# HashWatcher Hub Pi — One-Command Installer for Raspberry Pi
#
# SETUP GUIDE FOR CUSTOMERS:
#
#   1. Download Raspberry Pi Imager from https://www.raspberrypi.com/software/
#   2. Insert your microSD card into your computer
#   3. Open Raspberry Pi Imager and choose:
#        - Device:  your Pi model (Pi 4 / Pi 5)
#        - OS:      Raspberry Pi OS Lite (64-bit)
#        - Storage: your microSD card
#   4. Click the GEAR icon (⚙) before writing and configure:
#        ✅ Set hostname:     HashWatcherHub
#        ✅ Enable SSH:       Use password authentication
#        ✅ Set username:     pi
#        ✅ Set password:     (choose your own)
#        ✅ Configure Wi-Fi:  enter your SSID and password
#        ✅ Set locale:       your timezone
#   5. Click WRITE and wait for it to finish
#   6. Insert the SD card into your Pi and power it on
#   7. Wait ~60 seconds for it to boot and connect to Wi-Fi
#   8. From your computer (on the same network), run:
#
#        ssh pi@HashWatcherHub.local
#        curl -fsSL https://raw.githubusercontent.com/gpena208777/HashWatcherHubPi/main/install.sh | sudo bash
#
#     Or if you've set up install.hashwatcher.app:
#        curl -fsSL https://install.hashwatcher.app | sudo bash
#
# ─────────────────────────────────────────────────────────────────────
set -euo pipefail

REPO="gpena208777/HashWatcherHubPi"
BRANCH="main"
RELEASE_TAG="latest"
INSTALL_DIR="/opt/hashwatcher-hub-pi"
CONFIG_DIR="/etc/hashwatcher-hub-pi"
SERVICE_USER="hashwatcher-hub-pi"
HOSTNAME_TARGET="HashWatcherHub"
VENV_DIR="${INSTALL_DIR}/.venv"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${CYAN}[HashWatcher]${NC} $*"; }
ok()    { echo -e "${GREEN}[HashWatcher]${NC} $*"; }
fail()  { echo -e "${RED}[HashWatcher]${NC} $*" >&2; exit 1; }

# ── Pre-flight checks ───────────────────────────────────────────────

[[ "$(uname -s)" == "Linux" ]] || fail "This installer is for Linux (Raspberry Pi OS)."
[[ "$(id -u)" -eq 0 ]] || fail "Please run as root:  curl -fsSL https://install.hashwatcher.app | sudo bash"

info "${BOLD}HashWatcher Hub Pi Installer${NC}"
info "This will install the HashWatcher Hub Pi agent on this Raspberry Pi."
echo ""

# ── System packages ──────────────────────────────────────────────────

info "Updating system packages..."
apt-get update -qq
apt-get install -y -qq \
    python3 python3-venv python3-pip \
    bluetooth bluez libbluetooth-dev \
    wireless-tools wpasupplicant network-manager \
    curl wget git jq >/dev/null 2>&1

# ── Install Tailscale ────────────────────────────────────────────────

if ! command -v tailscale &>/dev/null; then
    info "Installing Tailscale..."
    curl -fsSL https://tailscale.com/install.sh | sh
else
    ok "Tailscale already installed."
fi
systemctl enable tailscaled --now 2>/dev/null || true

# ── Enable IP forwarding ────────────────────────────────────────────

info "Enabling IP forwarding for subnet routing..."
cat > /etc/sysctl.d/99-tailscale.conf <<'EOF'
net.ipv4.ip_forward = 1
net.ipv6.conf.all.forwarding = 1
EOF
sysctl -p /etc/sysctl.d/99-tailscale.conf >/dev/null 2>&1 || true

# ── Set hostname ─────────────────────────────────────────────────────

info "Setting hostname to ${HOSTNAME_TARGET}..."
hostnamectl set-hostname "${HOSTNAME_TARGET}" 2>/dev/null || true
echo "${HOSTNAME_TARGET}" > /etc/hostname

# ── Create service user ──────────────────────────────────────────────

if ! id "${SERVICE_USER}" &>/dev/null; then
    info "Creating service user: ${SERVICE_USER}"
    useradd -r -m -d "${INSTALL_DIR}" -s /usr/sbin/nologin "${SERVICE_USER}"
fi

mkdir -p "${INSTALL_DIR}" "${CONFIG_DIR}"

# ── Download gateway files ────────────────────────────────────────────

info "Downloading HashWatcher Gateway files..."
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

# Try .deb from GitHub Release first (cleanest install), fall back to tarball
# Resolve latest release .deb via API (GitHub redirects don't work for parameterized asset names)
DEB_URL=""
if [[ "${RELEASE_TAG}" == "latest" ]]; then
    DEB_INFO="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null | jq -r '.assets[] | select(.name | endswith(".deb")) | .browser_download_url' 2>/dev/null | head -1)"
    [[ -n "${DEB_INFO}" ]] && DEB_URL="${DEB_INFO}"
else
    DEB_URL="https://github.com/${REPO}/releases/download/${RELEASE_TAG}/hashwatcher-hub-pi_${RELEASE_TAG#v}_all.deb"
fi
RELEASE_URL="https://github.com/${REPO}/releases/${RELEASE_TAG}/download/hashwatcher-hub-pi.tar.gz"
ARCHIVE_URL="https://github.com/${REPO}/archive/refs/heads/${BRANCH}.tar.gz"

if [[ -n "${DEB_URL}" ]] && curl -fsSL --head "${DEB_URL}" >/dev/null 2>&1; then
    info "Installing from .deb package..."
    DEB_PATH="${TMP_DIR}/hashwatcher-hub-pi.deb"
    curl -fsSL -o "${DEB_PATH}" "${DEB_URL}"
    dpkg -i "${DEB_PATH}"
    ok "Installed via .deb package."
    echo ""
    ok "Dashboard:  http://$(hostname).local:8787"
    ok "Next: open the HashWatcher app to pair your miners."
    exit 0
fi

if curl -fsSL --head "${RELEASE_URL}" >/dev/null 2>&1; then
    info "Downloading from release tarball..."
    curl -fsSL "${RELEASE_URL}" | tar -xz -C "${TMP_DIR}"
    PI_SRC="${TMP_DIR}"
else
    info "Downloading from repository..."
    curl -fsSL "${ARCHIVE_URL}" | tar -xz -C "${TMP_DIR}" --strip-components=1
    PI_SRC="${TMP_DIR}"
fi

if [[ ! -f "${PI_SRC}/bitaxe_firebase_uploader.py" ]]; then
    fail "Hub Pi agent files not found in the downloaded archive. Check the repo or release."
fi

# ── Install Python files ─────────────────────────────────────────────

info "Installing Hub Pi agent..."
cp "${PI_SRC}/bitaxe_firebase_uploader.py" "${INSTALL_DIR}/"
cp "${PI_SRC}/hub_ble_provisioner.py"      "${INSTALL_DIR}/"
cp "${PI_SRC}/tailscale_setup.py"          "${INSTALL_DIR}/"
cp "${PI_SRC}/requirements.txt"            "${INSTALL_DIR}/"
cp "${PI_SRC}/icon.png"                    "${INSTALL_DIR}/" 2>/dev/null || true

# ── Python virtual environment ───────────────────────────────────────

info "Setting up Python environment..."
python3 -m venv "${VENV_DIR}"
"${VENV_DIR}/bin/pip" install --upgrade pip -q
"${VENV_DIR}/bin/pip" install -r "${INSTALL_DIR}/requirements.txt" -q

# ── Configuration ────────────────────────────────────────────────────

if [[ ! -f "${CONFIG_DIR}/hub.env" ]]; then
    info "Writing default configuration..."
    cat > "${CONFIG_DIR}/hub.env" <<EOF
PI_HOSTNAME=${HOSTNAME_TARGET}
BITAXE_HOST=
BITAXE_SCHEME=http
BITAXE_ENDPOINTS=/system/info,/api/system/info
POLL_SECONDS=10
HTTP_TIMEOUT_SECONDS=5
STATUS_HTTP_BIND=0.0.0.0
STATUS_HTTP_PORT=8787
RUNTIME_CONFIG_PATH=${INSTALL_DIR}/runtime_config.json
LAST_WIFI_PATH=${INSTALL_DIR}/last_wifi_credentials.json
AGENT_ID=${HOSTNAME_TARGET}
EOF
else
    ok "Existing configuration preserved at ${CONFIG_DIR}/hub.env"
fi

# ── Systemd services ────────────────────────────────────────────────

info "Installing systemd services..."

cat > /etc/systemd/system/hashwatcher-hub-pi.service <<EOF
[Unit]
Description=HashWatcher Hub Pi Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${INSTALL_DIR}
EnvironmentFile=${CONFIG_DIR}/hub.env
ExecStart=${VENV_DIR}/bin/python ${INSTALL_DIR}/bitaxe_firebase_uploader.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/hashwatcher-ble-provisioner.service <<EOF
[Unit]
Description=HashWatcher BLE Wi-Fi Provisioner
After=bluetooth.service network-pre.target
Wants=bluetooth.service

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=${INSTALL_DIR}
EnvironmentFile=-${CONFIG_DIR}/hub.env
ExecStartPre=/bin/sleep 2
ExecStartPre=/usr/sbin/rfkill unblock bluetooth
ExecStartPre=/bin/sleep 1
ExecStartPre=/usr/bin/hciconfig hci0 up
ExecStartPre=/bin/sleep 1
ExecStart=${VENV_DIR}/bin/python ${INSTALL_DIR}/hub_ble_provisioner.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# ── Sudoers for service user ─────────────────────────────────────────

cat > /etc/sudoers.d/hashwatcher-hub-pi <<'EOF'
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/tailscale, /usr/bin/tailscale up *, /usr/bin/tailscale down, /usr/bin/tailscale logout, /usr/bin/tailscale status *, /usr/bin/tailscale debug *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/systemctl start tailscaled, /usr/bin/systemctl restart tailscaled, /usr/bin/systemctl restart hashwatcher-hub-pi, /usr/bin/systemctl restart hashwatcher-ble-provisioner
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/sbin/sysctl -w *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/nmcli *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/sbin/wpa_cli *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/dpkg -i /opt/hashwatcher-hub-pi/updates/*
EOF
chmod 0440 /etc/sudoers.d/hashwatcher-hub-pi

# ── Set ownership & start ────────────────────────────────────────────

chown -R "${SERVICE_USER}:${SERVICE_USER}" "${INSTALL_DIR}" "${CONFIG_DIR}"

systemctl daemon-reload
systemctl enable --now hashwatcher-hub-pi
systemctl enable --now hashwatcher-ble-provisioner

# ── Done ─────────────────────────────────────────────────────────────

echo ""
ok "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
ok "${BOLD}HashWatcher Hub Pi installed successfully!${NC}"
ok "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
ok "Dashboard:  http://${HOSTNAME_TARGET}.local:8787"
ok "API:        http://${HOSTNAME_TARGET}.local:8787/api/status"
echo ""
ok "Next steps:"
ok "  1. Download the HashWatcher app at https://www.HashWatcher.app"
ok "  2. Open the dashboard above to set up Tailscale"
ok "  3. Pair your miners from the app"
echo ""
ok "Logs:  journalctl -u hashwatcher-hub-pi -f"
ok "BLE:   journalctl -u hashwatcher-ble-provisioner -f"
echo ""
