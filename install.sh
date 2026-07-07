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
PREFERRED_RELEASE_MODE="${HASHWATCHER_RELEASE_MODE:-source}"

INSTALL_DIR="/opt/hashwatcher-hub-pi"
CONFIG_DIR="/etc/hashwatcher-hub-pi"
SYSTEMD_DIR="/etc/systemd/system"
SERVICE_USER="hashwatcher-hub-pi"
VENV_DIR="${INSTALL_DIR}/.venv"
REQ_HASH_FILE="${INSTALL_DIR}/.requirements.sha256"
VENDORED_BLUEZERO_WHEEL_REL="vendor/bluezero-0.9.1-py2.py3-none-any.whl"
SOURCE_DIR=""

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${CYAN}[HashWatcher]${NC} $*"; }
ok()    { echo -e "${GREEN}[HashWatcher]${NC} $*"; }
fail()  { echo -e "${RED}[HashWatcher]${NC} $*" >&2; exit 1; }

clock_now_epoch() {
    date -u +%s 2>/dev/null || echo 0
}

fetch_https_date_epoch() {
    local url="$1"
    local date_header

    date_header="$(
        curl -fsSI --max-time 15 "${url}" 2>/dev/null |
        awk 'BEGIN{IGNORECASE=1} /^date:/ {sub(/\r$/, "", $0); print substr($0, 7); exit}'
    )"
    [[ -n "${date_header}" ]] || return 1

    HTTPS_DATE_HEADER="${date_header}" python3 - <<'PY'
import email.utils
import os
import sys

value = os.environ.get("HTTPS_DATE_HEADER", "").strip()
if not value:
    raise SystemExit(1)
dt = email.utils.parsedate_to_datetime(value)
print(int(dt.timestamp()))
PY
}

wait_for_ntp_sync() {
    local attempts="${1:-20}"
    local delay_seconds="${2:-2}"
    local synced=""
    local i

    if ! command -v timedatectl >/dev/null 2>&1; then
        return 1
    fi

    for i in $(seq 1 "${attempts}"); do
        synced="$(timedatectl show -p NTPSynchronized --value 2>/dev/null || true)"
        if [[ "${synced}" == "yes" ]]; then
            return 0
        fi
        sleep "${delay_seconds}"
    done

    return 1
}

try_enable_time_sync() {
    if ! command -v timedatectl >/dev/null 2>&1; then
        return 1
    fi

    timedatectl set-ntp true >/dev/null 2>&1 || true

    if systemctl list-unit-files systemd-timesyncd.service >/dev/null 2>&1; then
        systemctl restart systemd-timesyncd.service >/dev/null 2>&1 || true
    fi

    wait_for_ntp_sync 15 2
}

set_clock_from_https_date() {
    local current_epoch remote_epoch source_url delta

    current_epoch="$(clock_now_epoch)"

    for source_url in \
        "https://api.github.com" \
        "https://github.com" \
        "https://pkgs.tailscale.com" \
        "https://deb.debian.org"
    do
        remote_epoch="$(fetch_https_date_epoch "${source_url}" || true)"
        [[ -n "${remote_epoch}" ]] || continue
        delta=$(( remote_epoch - current_epoch ))
        if (( delta > 60 )); then
            info "Pi clock is behind by about ${delta}s; setting time from ${source_url}."
            date -u -s "@${remote_epoch}" >/dev/null 2>&1 || return 1
            return 0
        fi
        if (( delta >= -60 )); then
            return 0
        fi
    done

    return 1
}

ensure_clock_is_sane() {
    info "Checking Pi clock before package operations..."
    if try_enable_time_sync; then
        ok "NTP time sync is active."
        return 0
    fi

    if set_clock_from_https_date; then
        ok "Adjusted Pi clock from HTTPS response headers."
        return 0
    fi

    info "Clock sync could not be confirmed yet; continuing and watching for apt signature timing errors."
    return 1
}

apt_update_with_clock_recovery() {
    local apt_output status

    require_apt_archive_cache_healthy

    set +e
    apt_output="$(apt-get update -qq 2>&1)"
    status=$?
    set -e

    if [[ ${status} -eq 0 ]] && ! grep -q "Not live until" <<<"${apt_output}"; then
        return 0
    fi

    if grep -q "Not live until" <<<"${apt_output}"; then
        info "apt repository signatures are not valid yet for the Pi clock; retrying after a clock sync."
        ensure_clock_is_sane || true

        require_apt_archive_cache_healthy

        set +e
        apt_output="$(apt-get update -qq 2>&1)"
        status=$?
        set -e
    fi

    if [[ ${status} -ne 0 ]]; then
        printf '%s\n' "${apt_output}" >&2
        if apt_output_indicates_filesystem_damage "${apt_output}"; then
            print_filesystem_repair_guidance
        fi
        return "${status}"
    fi

    if grep -q "Not live until" <<<"${apt_output}"; then
        printf '%s\n' "${apt_output}" >&2
        return 1
    fi

    return 0
}

recover_interrupted_dpkg() {
    info "Recovering interrupted dpkg state..."
    DEBIAN_FRONTEND=noninteractive dpkg --configure -a
}

apt_output_indicates_filesystem_damage() {
    grep -Eq "Structure needs cleaning|Input/output error" <<<"${1:-}"
}

print_filesystem_repair_guidance() {
    cat >&2 <<'EOF'

The Pi filesystem is reporting metadata damage while apt is using /var/cache/apt.
This is below apt/dpkg; retrying the installer will keep failing until the filesystem
is repaired or the microSD/image is replaced.

Fast factory path:
  1. Power the Pi down cleanly.
  2. Reflash or replace the microSD card.
  3. Re-run provisioning.

Field recovery path:
  sudo touch /forcefsck
  sudo reboot

If the error returns after fsck, treat the card or power path as bad.
EOF
}

repair_apt_archive_cache() {
    local repair_output status

    set +e
    repair_output="$(
        mkdir -p /var/cache/apt/archives 2>&1
        rm -rf /var/cache/apt/archives/partial 2>&1
        if id _apt >/dev/null 2>&1; then
            install -d -m 0700 -o _apt -g root /var/cache/apt/archives/partial 2>&1
        else
            install -d -m 0755 /var/cache/apt/archives/partial 2>&1
        fi
        touch /var/cache/apt/archives/.hashwatcher-write-test 2>&1
        rm -f /var/cache/apt/archives/.hashwatcher-write-test 2>&1
    )"
    status=$?
    set -e

    if [[ ${status} -eq 0 ]]; then
        return 0
    fi

    printf '%s\n' "${repair_output}" >&2
    if apt_output_indicates_filesystem_damage "${repair_output}"; then
        print_filesystem_repair_guidance
    fi
    return "${status}"
}

require_apt_archive_cache_healthy() {
    if [[ -d /var/cache/apt/archives/partial ]]; then
        return 0
    fi

    info "Repairing missing apt archive cache directory..."
    repair_apt_archive_cache || fail "apt archive cache is not writable."
}

apt_install_with_dpkg_recovery() {
    local apt_output status

    require_apt_archive_cache_healthy

    set +e
    apt_output="$(DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "$@" 2>&1)"
    status=$?
    set -e

    if [[ ${status} -eq 0 ]]; then
        return 0
    fi

    if grep -q "dpkg was interrupted, you must manually run 'sudo dpkg --configure -a'" <<<"${apt_output}"; then
        printf '%s\n' "${apt_output}" >&2
        recover_interrupted_dpkg || return 1

        require_apt_archive_cache_healthy

        set +e
        apt_output="$(DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "$@" 2>&1)"
        status=$?
        set -e
    fi

    if [[ ${status} -ne 0 ]]; then
        printf '%s\n' "${apt_output}" >&2
        if apt_output_indicates_filesystem_damage "${apt_output}"; then
            print_filesystem_repair_guidance
        fi
        print_dpkg_diagnostics
        return "${status}"
    fi

    return 0
}

print_dpkg_diagnostics() {
    info "dpkg diagnostics:"
    dpkg --audit 2>&1 || true
    if [[ -f /var/log/dpkg.log ]]; then
        info "Recent dpkg log:"
        tail -n 40 /var/log/dpkg.log 2>&1 || true
    fi
}

print_python_dependency_diagnostics() {
    info "Python dependency diagnostics:"
    "${VENV_DIR}/bin/python" -m pip --version 2>&1 || true
    "${VENV_DIR}/bin/python" -m pip list 2>&1 || true
    "${VENV_DIR}/bin/python" - <<'PY' 2>&1 || true
import importlib.util

required = ("requests", "dotenv", "bluezero", "dbus")
for name in required:
    print(f"{name}: {'present' if importlib.util.find_spec(name) else 'missing'}")
PY
}

wait_for_service_healthy() {
    local unit="$1"
    local attempts="${2:-6}"
    local delay_seconds="${3:-2}"
    local i

    for i in $(seq 1 "${attempts}"); do
        if systemctl is-active --quiet "${unit}"; then
            return 0
        fi
        sleep "${delay_seconds}"
    done

    systemctl --no-pager --full status "${unit}" 2>&1 || true
    journalctl -u "${unit}" -n 80 --no-pager 2>&1 || true
    return 1
}

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

require_no_kernel_storage_errors() {
    local kernel_log

    if [[ "${HASHWATCHER_ALLOW_FS_ERRORS:-0}" == "1" ]]; then
        info "Skipping kernel storage-error preflight because HASHWATCHER_ALLOW_FS_ERRORS=1."
        return 0
    fi

    kernel_log="$(journalctl -k -n 300 --no-pager 2>/dev/null || dmesg -T 2>/dev/null || true)"
    if [[ -z "${kernel_log}" ]]; then
        return 0
    fi

    if grep -Eiq 'EXT4-fs error|I/O error|Buffer I/O|structure needs cleaning|mmcblk[^[:space:]]*.*error|end_request: I/O error' <<<"${kernel_log}"; then
        {
            echo
            echo "Recent kernel storage/filesystem errors were detected:"
            grep -Ei 'EXT4-fs error|I/O error|Buffer I/O|structure needs cleaning|mmcblk[^[:space:]]*.*error|end_request: I/O error' <<<"${kernel_log}" | tail -20
            echo
            echo "The Pi storage is not healthy enough for package installation."
            echo "Reflash or replace the microSD card, then run provisioning again."
            echo
        } >&2
        fail "Filesystem preflight failed before install."
    fi
}

configure_sd_longevity() {
    # Reduce microSD wear on an always-on appliance:
    # - cap the persistent journal so journald stops growing/rotating large files
    # - stop daily apt metadata churn (hub updates come from the HashWatcher installer/OTA)
    # - keep the kernel from swapping to SD unless memory pressure is real
    info "Applying SD-card longevity settings..."

    install -d -m 0755 /etc/systemd/journald.conf.d
    cat > /etc/systemd/journald.conf.d/hashwatcher-sd-care.conf <<'EOF'
# Installed by HashWatcher Hub Pi. Caps journal writes to protect the microSD.
[Journal]
Storage=persistent
SystemMaxUse=48M
SystemMaxFileSize=8M
MaxRetentionSec=14day
EOF
    systemctl restart systemd-journald 2>/dev/null || true

    local timer
    for timer in apt-daily.timer apt-daily-upgrade.timer man-db.timer; do
        if systemctl list-unit-files "${timer}" >/dev/null 2>&1; then
            systemctl disable --now "${timer}" >/dev/null 2>&1 || true
        fi
    done

    cat > /etc/sysctl.d/98-hashwatcher-sd-care.conf <<'EOF'
# Installed by HashWatcher Hub Pi. Prefer RAM over swapping to the microSD.
vm.swappiness = 10
EOF
    sysctl -p /etc/sysctl.d/98-hashwatcher-sd-care.conf >/dev/null 2>&1 || true

    ok "SD-card longevity settings applied."
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
    "${VENDORED_BLUEZERO_WHEEL_REL}"
)

required_packages=(
    python3
    python3-venv
    python3-pip
    python3-dev
    python3-dbus
    python3-requests
    python3-dotenv
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

Optional:
  HASHWATCHER_RELEASE_MODE=deb    # prefer the GitHub release .deb asset first
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

    ensure_clock_is_sane || true

    info "Downloading latest release package..."
    curl -fsSL "${deb_url}" -o "${deb_path}"

    if ! command -v tailscale >/dev/null 2>&1; then
        info "Installing Tailscale before package install..."
        curl -fsSL https://tailscale.com/install.sh | sh
    else
        ok "Tailscale already installed."
    fi
    if systemctl list-unit-files tailscaled.service >/dev/null 2>&1; then
        systemctl enable tailscaled --now 2>/dev/null || true
    fi

    info "Installing latest release package..."
    apt_update_with_clock_recovery || fail "Package index update failed, likely because the Pi clock is still incorrect."
    if ! apt_install_with_dpkg_recovery "${deb_path}"; then
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

venv_has_requirements() {
    "${VENV_DIR}/bin/python" - <<'PY' >/dev/null 2>&1
import importlib.util
import sys

required = ("requests", "dotenv", "bluezero", "dbus")
missing = [name for name in required if importlib.util.find_spec(name) is None]
sys.exit(0 if not missing else 1)
PY
}

runtime_dependency_hash() {
    local src_root="$1"
    sha256sum \
        "${src_root}/requirements.txt" \
        "${src_root}/${VENDORED_BLUEZERO_WHEEL_REL}" | sha256sum | awk '{print $1}'
}

ensure_venv_uses_system_site_packages() {
    local cfg="${VENV_DIR}/pyvenv.cfg"

    [[ -f "${cfg}" ]] || return 0
    if grep -q '^include-system-site-packages = true$' "${cfg}"; then
        return 0
    fi

    info "Reconfiguring virtualenv to use system site-packages."
    sed -i 's/^include-system-site-packages = false/include-system-site-packages = true/' "${cfg}"
}

ensure_venv_pip() {
    if "${VENV_DIR}/bin/python" -m pip --version >/dev/null 2>&1; then
        return 0
    fi

    info "Bootstrapping pip inside the virtual environment..."
    "${VENV_DIR}/bin/python" -m ensurepip --upgrade >/dev/null 2>&1 || return 1
    "${VENV_DIR}/bin/python" -m pip --version >/dev/null 2>&1
}

install_python_runtime_dependencies() {
    local vendor_wheel="${INSTALL_DIR}/${VENDORED_BLUEZERO_WHEEL_REL}"

    if [[ ! -f "${vendor_wheel}" ]]; then
        fail "Missing bundled dependency wheel: ${vendor_wheel}"
    fi

    if ! ensure_venv_pip; then
        fail "pip bootstrap failed in ${VENV_DIR}."
    fi

    if ! "${VENV_DIR}/bin/python" -m pip install --no-deps --quiet --force-reinstall "${vendor_wheel}"; then
        info "Bundled bluezero install failed; re-running verbosely."
        "${VENV_DIR}/bin/python" -m pip install --no-deps --force-reinstall "${vendor_wheel}" || {
            print_python_dependency_diagnostics
            fail "Installing bundled bluezero wheel failed."
        }
    fi

    if venv_has_requirements; then
        return 0
    fi

    info "System packages plus bundled wheel were incomplete; falling back to pip requirements install."
    if ! "${VENV_DIR}/bin/python" -m pip install --prefer-binary --timeout 30 --retries 5 -r "${INSTALL_DIR}/requirements.txt" -q; then
        info "Python dependency fallback failed; re-running pip verbosely for diagnostics."
        "${VENV_DIR}/bin/python" -m pip install --prefer-binary --timeout 30 --retries 5 -r "${INSTALL_DIR}/requirements.txt" || true
        print_python_dependency_diagnostics
        fail "Python dependency install failed."
    fi

    if ! venv_has_requirements; then
        print_python_dependency_diagnostics
        fail "Python dependencies are still incomplete after installation."
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

    ensure_clock_is_sane || true

    for pkg in "${required_packages[@]}"; do
        if ! package_installed "${pkg}"; then
            missing_packages+=("${pkg}")
        fi
    done

    if ((${#missing_packages[@]} > 0)); then
        info "Installing missing system packages: ${missing_packages[*]}"
        apt_update_with_clock_recovery || fail "Package index update failed, likely because the Pi clock is still incorrect."
        apt_install_with_dpkg_recovery "${missing_packages[@]}" || fail "Installing required system packages failed."
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

    configure_sd_longevity

    if ! id "${SERVICE_USER}" &>/dev/null; then
        info "Creating service user: ${SERVICE_USER}"
        useradd -r -d "${INSTALL_DIR}" -s /usr/sbin/nologin "${SERVICE_USER}"
    fi

    install -d -m 0755 "${INSTALL_DIR}" "${CONFIG_DIR}" "${INSTALL_DIR}/updates"
    install -d -m 0755 "${INSTALL_DIR}/vendor"

    for f in hashwatcher_hub_agent.py hub_ble_provisioner.py tailscale_setup.py requirements.txt; do
        install -m 0644 "${src}/${f}" "${INSTALL_DIR}/${f}"
    done
    install -m 0644 "${src}/${VENDORED_BLUEZERO_WHEEL_REL}" "${INSTALL_DIR}/${VENDORED_BLUEZERO_WHEEL_REL}"
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

    current_req_hash="$(runtime_dependency_hash "${src}")"
    saved_req_hash="$(cat "${REQ_HASH_FILE}" 2>/dev/null || true)"

    if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
        info "Creating Python virtual environment..."
        python3 -m venv --system-site-packages "${VENV_DIR}"
        venv_created=1
    fi
    ensure_venv_uses_system_site_packages

    if [[ "${current_req_hash}" != "${saved_req_hash}" || "${venv_created}" -eq 1 ]] || ! venv_has_requirements; then
        info "Installing Python dependencies..."
        install_python_runtime_dependencies
        printf '%s\n' "${current_req_hash}" > "${REQ_HASH_FILE}"
    else
        ok "Python dependencies unchanged; reusing existing virtualenv."
    fi

    cat > /etc/sudoers.d/hashwatcher-hub-pi <<'EOF'
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/tailscale, /usr/bin/tailscale up *, /usr/bin/tailscale down, /usr/bin/tailscale logout, /usr/bin/tailscale status *, /usr/bin/tailscale debug *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/systemctl start tailscaled, /usr/bin/systemctl stop tailscaled, /usr/bin/systemctl restart tailscaled, /usr/bin/systemctl restart hashwatcher-hub-pi, /usr/bin/systemctl restart hashwatcher-ble-provisioner
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/sbin/reboot
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/sbin/sysctl -w *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/nmcli *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/sbin/wpa_cli *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/vcgencmd *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/journalctl *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/rm -f /var/lib/tailscale/tailscaled.state, /usr/bin/rm -f /var/lib/tailscale/tailscaled.state.tmp, /usr/bin/rm -f /var/lib/tailscale/tailscaled.state.bak
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /bin/rm -f /var/lib/tailscale/tailscaled.state, /bin/rm -f /var/lib/tailscale/tailscaled.state.tmp, /bin/rm -f /var/lib/tailscale/tailscaled.state.bak
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

    wait_for_service_healthy hashwatcher-hub-pi 8 2 || fail "hashwatcher-hub-pi failed to become healthy after install."
    wait_for_service_healthy hashwatcher-ble-provisioner 8 2 || fail "hashwatcher-ble-provisioner failed to become healthy after install."

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
require_no_kernel_storage_errors
require_apt_archive_cache_healthy

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

if [[ "${PREFERRED_RELEASE_MODE}" == "deb" && -n "${LATEST_RELEASE_DEB_URL}" ]]; then
    info "Release mode: deb-first"
    if install_latest_release_deb "${LATEST_RELEASE_DEB_URL}" "${TMP_DIR}/hashwatcher-hub-pi.deb"; then
        exit 0
    fi
    info "Falling back to latest release source archive."
else
    info "Release mode: source-first"
fi

if curl -fsSL "${LATEST_RELEASE_ARCHIVE_URL}" | tar -xz -C "${TMP_DIR}" --strip-components=1; then
    info "Downloaded latest release archive (${LATEST_RELEASE_TAG})."
    install_from_source "${TMP_DIR}"
    exit 0
fi

if [[ -n "${LATEST_RELEASE_DEB_URL}" ]]; then
    info "Source archive download failed; falling back to the GitHub release .deb asset."
    if install_latest_release_deb "${LATEST_RELEASE_DEB_URL}" "${TMP_DIR}/hashwatcher-hub-pi.deb"; then
        exit 0
    fi
fi

fail "Failed to install HashWatcher Hub Pi from both the source archive and the .deb asset."
