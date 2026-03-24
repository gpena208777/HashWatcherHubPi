#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# HashWatcher Hub Pi — canonical installer for stock Raspberry Pi OS.
#
# Default mode:
#   Downloads the latest app payload from GitHub and installs it.
#
# Local mode:
#   Installs from a local source directory. Used by SSH/manual deploys.
#   Example: sudo bash install.sh --source-dir /tmp/hashwatcher
# ─────────────────────────────────────────────────────────────────────
set -euo pipefail

REPO="gpena208777/HashWatcherHubPi"
LATEST_RELEASE_API_URL="https://api.github.com/repos/${REPO}/releases/latest"
LATEST_RELEASE_TAG=""
LATEST_RELEASE_DEB_URL=""
LATEST_RELEASE_ARCHIVE_URL=""

INSTALL_DIR="/opt/hashwatcher-hub-pi"
CONFIG_DIR="/etc/hashwatcher-hub-pi"
SYSTEMD_DIR="/etc/systemd/system"
SERVICE_USER="hashwatcher-hub-pi"
VENV_DIR="${INSTALL_DIR}/.venv"
REQ_HASH_FILE="${INSTALL_DIR}/.requirements.sha256"
SOURCE_DIR=""

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${CYAN}[HashWatcher]${NC} $*"; }
ok()    { echo -e "${GREEN}[HashWatcher]${NC} $*"; }
fail()  { echo -e "${RED}[HashWatcher]${NC} $*" >&2; exit 1; }

is_rootfs_read_only() {
    local mount_opts
    mount_opts="$(findmnt -no OPTIONS / 2>/dev/null || true)"
    [[ "${mount_opts}" == *"ro"* ]] && [[ "${mount_opts}" != *"rw"* ]]
}

require_writable_rootfs() {
    if is_rootfs_read_only; then
        fail "$(cat <<'EOF'
Root filesystem is mounted read-only.
This installer needs write access to /etc, /var, and /opt.

On the Pi, run:
  sudo mount -o remount,rw /

If remount fails, schedule a filesystem check and reboot:
  sudo touch /forcefsck
  sudo reboot
EOF
)"
    fi
}

required_files=(
    VERSION
    hashwatcher_hub_agent.py
    hub_ble_provisioner.py
    tailscale_setup.py
    ota_update_helper.sh
    requirements.txt
    hashwatcher-hub.service
    hashwatcher-ble-provisioner.service
    hub.env.prepared
)

required_packages=(
    python3
    python3-venv
    python3-pip
    python3-dev
    python3-dbus
    bluetooth
    bluez
    libbluetooth-dev
    libglib2.0-dev
    libdbus-1-dev
    libcairo2-dev
    libgirepository1.0-dev
    gobject-introspection
    pkg-config
    meson
    wireless-tools
    wpasupplicant
    network-manager
    curl
)

usage() {
    cat <<'EOF'
Usage:
  curl -fsSL https://raw.githubusercontent.com/gpena208777/HashWatcherHubPi/main/install.sh | sudo bash
  sudo bash install.sh --source-dir /path/to/hashwatcher-bundle
EOF
}

resolve_latest_release() {
    local release_json parsed release_tag release_deb_url

    release_json="$(curl -fsSL "${LATEST_RELEASE_API_URL}")" || return 1

    parsed="$(python3 -c '
import json, sys
d = json.load(sys.stdin)
tag = (d.get("tag_name") or "").strip()
deb_url = ""
for asset in d.get("assets", []):
    name = (asset.get("name") or "").strip()
    url = (asset.get("browser_download_url") or "").strip()
    if name.startswith("hashwatcher-hub-pi_") and name.endswith("_all.deb") and url:
        deb_url = url
        break
print(tag)
print(deb_url)
' <<<"${release_json}")" || return 1

    release_tag="$(printf '%s\n' "${parsed}" | sed -n '1p')"
    release_deb_url="$(printf '%s\n' "${parsed}" | sed -n '2p')"

    [[ -n "${release_tag}" ]] || return 1

    LATEST_RELEASE_TAG="${release_tag}"
    LATEST_RELEASE_DEB_URL="${release_deb_url}"
    LATEST_RELEASE_ARCHIVE_URL="https://github.com/${REPO}/archive/refs/tags/${LATEST_RELEASE_TAG}.tar.gz"
}

install_latest_release_deb() {
    local deb_url="$1"
    local deb_path="$2"
    local installed_version
    local installed_status

    info "Downloading latest release package..."
    curl -fsSL "${deb_url}" -o "${deb_path}"

    info "Installing latest release package..."
    apt-get update -qq
    if ! DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "${deb_path}"; then
        fail "Package install failed during apt/dpkg configure step."
    fi

    installed_version="$(dpkg-query -W -f='${Version}' hashwatcher-hub-pi 2>/dev/null || true)"
    installed_status="$(dpkg-query -W -f='${Status}' hashwatcher-hub-pi 2>/dev/null || true)"
    [[ "${installed_status}" == "install ok installed" ]] || return 1
    [[ -n "${installed_version}" ]] || return 1

    ok "Installed hashwatcher-hub-pi version ${installed_version}"
}

package_installed() {
    dpkg-query -W -f='${db:Status-Status}' "$1" 2>/dev/null | grep -qx "installed"
}

append_config_if_missing() {
    local key="$1"
    local value="$2"
    local config_path="${CONFIG_DIR}/hub.env"

    if ! grep -q "^${key}=" "${config_path}"; then
        printf '\n%s=%s\n' "${key}" "${value}" >> "${config_path}"
    fi
}

install_from_source() {
    local src="$1"
    local current_hostname
    local current_req_hash
    local saved_req_hash
    local venv_created=0
    local missing_packages=()

    for f in "${required_files[@]}"; do
        [[ -f "${src}/${f}" ]] || fail "Missing required installer asset: ${src}/${f}"
    done

    info "${BOLD}HashWatcher Hub Pi Installer${NC}"
    info "Installing from source: ${src}"
    echo ""

    for pkg in "${required_packages[@]}"; do
        if ! package_installed "${pkg}"; then
            missing_packages+=("${pkg}")
        fi
    done

    if ((${#missing_packages[@]} > 0)); then
        info "Installing missing system packages: ${missing_packages[*]}"
        apt-get update -qq
        DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "${missing_packages[@]}"
    else
        ok "Required system packages already installed."
    fi

    if ! command -v tailscale >/dev/null 2>&1; then
        info "Installing Tailscale..."
        curl -fsSL https://tailscale.com/install.sh | sh
    else
        ok "Tailscale already installed."
    fi

    if systemctl list-unit-files tailscaled.service >/dev/null 2>&1; then
        systemctl enable tailscaled --now 2>/dev/null || true
    fi

    cat > /etc/sysctl.d/99-tailscale.conf <<'EOF'
net.ipv4.ip_forward = 1
net.ipv6.conf.all.forwarding = 1
EOF
    sysctl -p /etc/sysctl.d/99-tailscale.conf >/dev/null 2>&1 || true

    if ! id "${SERVICE_USER}" &>/dev/null; then
        info "Creating service user: ${SERVICE_USER}"
        useradd -r -d "${INSTALL_DIR}" -s /usr/sbin/nologin "${SERVICE_USER}"
    fi

    install -d -m 0755 "${INSTALL_DIR}" "${CONFIG_DIR}" "${INSTALL_DIR}/updates"

    for f in hashwatcher_hub_agent.py hub_ble_provisioner.py tailscale_setup.py requirements.txt; do
        install -m 0644 "${src}/${f}" "${INSTALL_DIR}/${f}"
    done
    install -m 0755 "${src}/ota_update_helper.sh" "${INSTALL_DIR}/ota_update_helper.sh"

    install -m 0644 "${src}/VERSION" "${INSTALL_DIR}/VERSION"

    if [[ -f "${src}/icon.png" ]]; then
        install -m 0644 "${src}/icon.png" "${INSTALL_DIR}/icon.png"
    fi

    install -m 0644 "${src}/hashwatcher-hub.service" "${SYSTEMD_DIR}/hashwatcher-hub-pi.service"
    install -m 0644 "${src}/hashwatcher-ble-provisioner.service" "${SYSTEMD_DIR}/hashwatcher-ble-provisioner.service"

    if [[ ! -f "${CONFIG_DIR}/hub.env" ]]; then
        info "Writing default configuration..."
        install -m 0644 "${src}/hub.env.prepared" "${CONFIG_DIR}/hub.env"
        current_hostname="$(hostname)"
        sed -i "s/^PI_HOSTNAME=.*/PI_HOSTNAME=${current_hostname}/" "${CONFIG_DIR}/hub.env"
        sed -i "s/^AGENT_ID=.*/AGENT_ID=${current_hostname}/" "${CONFIG_DIR}/hub.env"
    else
        ok "Existing configuration preserved at ${CONFIG_DIR}/hub.env"
    fi

    append_config_if_missing "RUNTIME_CONFIG_PATH" "${INSTALL_DIR}/runtime_config.json"
    append_config_if_missing "LAST_WIFI_PATH" "${INSTALL_DIR}/last_wifi_credentials.json"

    current_req_hash="$(sha256sum "${INSTALL_DIR}/requirements.txt" | awk '{print $1}')"
    saved_req_hash="$(cat "${REQ_HASH_FILE}" 2>/dev/null || true)"

    if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
        info "Creating Python virtual environment..."
        python3 -m venv "${VENV_DIR}"
        venv_created=1
    fi

    if [[ "${current_req_hash}" != "${saved_req_hash}" || "${venv_created}" -eq 1 ]]; then
        info "Installing Python dependencies..."
        "${VENV_DIR}/bin/pip" install --upgrade pip -q
        "${VENV_DIR}/bin/pip" install -r "${INSTALL_DIR}/requirements.txt" -q
        printf '%s\n' "${current_req_hash}" > "${REQ_HASH_FILE}"
    else
        ok "Python dependencies unchanged; reusing existing virtualenv."
    fi

    cat > /etc/sudoers.d/hashwatcher-hub-pi <<'EOF'
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/tailscale, /usr/bin/tailscale up *, /usr/bin/tailscale down, /usr/bin/tailscale logout, /usr/bin/tailscale status *, /usr/bin/tailscale debug *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/systemctl start tailscaled, /usr/bin/systemctl restart tailscaled, /usr/bin/systemctl restart hashwatcher-hub-pi, /usr/bin/systemctl restart hashwatcher-ble-provisioner
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/sbin/reboot
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/sbin/sysctl -w *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/nmcli *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/sbin/wpa_cli *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/vcgencmd *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/tee /sys/class/leds/*/brightness, /usr/bin/tee /sys/class/leds/*/trigger, /usr/bin/tee /sys/class/leds/*/delay_on, /usr/bin/tee /sys/class/leds/*/delay_off
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/dpkg -i /opt/hashwatcher-hub-pi/updates/*
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/systemd-run --unit hashwatcher-hub-update --collect --service-type=oneshot /opt/hashwatcher-hub-pi/ota_update_helper.sh *
EOF
    chmod 0440 /etc/sudoers.d/hashwatcher-hub-pi

    chown -R "${SERVICE_USER}:${SERVICE_USER}" "${INSTALL_DIR}" "${CONFIG_DIR}"

    systemctl daemon-reload
    systemctl enable hashwatcher-hub-pi >/dev/null 2>&1 || true
    systemctl enable hashwatcher-ble-provisioner >/dev/null 2>&1 || true
    systemctl restart hashwatcher-hub-pi
    systemctl restart hashwatcher-ble-provisioner

    echo ""
    ok "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    ok "${BOLD}HashWatcher Hub Pi installed successfully!${NC}"
    ok "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    ok "Dashboard:  http://$(hostname).local:8787"
    ok "API:        http://$(hostname).local:8787/api/status"
    echo ""
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source-dir)
            [[ $# -ge 2 ]] || fail "--source-dir requires a path"
            SOURCE_DIR="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "Unknown argument: $1"
            ;;
    esac
done

[[ "$(uname -s)" == "Linux" ]] || fail "This installer is for Linux (Raspberry Pi OS)."
[[ "$(id -u)" -eq 0 ]] || fail "Please run as root."
require_writable_rootfs

if [[ -n "${SOURCE_DIR}" ]]; then
    install_from_source "${SOURCE_DIR}"
    exit 0
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

info "${BOLD}HashWatcher Hub Pi Bootstrap Installer${NC}"
info "This will download the latest HashWatcher Hub Pi payload and install it on this Raspberry Pi."
echo ""
info "Resolving latest release metadata..."

if resolve_latest_release; then
    info "Latest release tag: ${LATEST_RELEASE_TAG}"
else
    fail "Could not resolve latest release metadata from GitHub."
fi

if [[ -n "${LATEST_RELEASE_DEB_URL}" ]] && install_latest_release_deb "${LATEST_RELEASE_DEB_URL}" "${TMP_DIR}/hashwatcher-hub-pi.deb"; then
    exit 0
fi

info "Falling back to latest release source archive."
if curl -fsSL "${LATEST_RELEASE_ARCHIVE_URL}" | tar -xz -C "${TMP_DIR}" --strip-components=1; then
    info "Downloaded latest release archive (${LATEST_RELEASE_TAG})."
else
    fail "Failed to download latest release archive (${LATEST_RELEASE_TAG})."
fi

install_from_source "${TMP_DIR}"
