#!/usr/bin/env bash
set -euo pipefail

PI_HOST="${1:-raspberrypi.local}"
PI_USER="${2:-hashwatcherhub}"
PI_TARGET="${PI_USER}@${PI_HOST}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUDO_PASS="${PI_SUDO_PASS:-${SSHPASS:-}}"
SSH_OPTS=(-o ConnectTimeout=8 -o StrictHostKeyChecking=accept-new)

# Use sshpass when SSHPASS is set (password auth)
if [[ -n "${SSHPASS:-}" ]]; then
  SSH_CMD=(sshpass -e ssh)
  SCP_CMD=(sshpass -e scp)
else
  SSH_CMD=(ssh)
  SCP_CMD=(scp)
fi

TMP_ARCHIVE="$(mktemp /tmp/hashwatcher-hub-pi-bundle.XXXXXX.tgz)"
cleanup() {
  rm -f "${TMP_ARCHIVE}" || true
}
trap cleanup EXIT

echo "Bundling files"
tar -C "${ROOT_DIR}" -czf "${TMP_ARCHIVE}" \
  install.sh \
  hashwatcher_hub_agent.py \
  hub_ble_provisioner.py \
  tailscale_setup.py \
  requirements.txt \
  hashwatcher-hub.service \
  hashwatcher-ble-provisioner.service \
  hub.env.prepared \
  icon.png

echo "Uploading bundle"
"${SCP_CMD[@]}" "${SSH_OPTS[@]}" "${TMP_ARCHIVE}" "${PI_TARGET}:~/hashwatcher-hub-pi-bundle.tgz"

echo "Installing service on Pi"
SUDO_PASS_QUOTED="$(printf "%q" "${SUDO_PASS}")"
"${SSH_CMD[@]}" "${SSH_OPTS[@]}" "${PI_TARGET}" "SUDO_PASS=${SUDO_PASS_QUOTED} bash -s" <<'EOSSH'
set -euo pipefail

sudo_cmd() {
  sudo "$@"
}

TARGET_HOSTNAME="HashWatcherHub"

if [[ -n "${SUDO_PASS:-}" ]]; then
  printf '%s\n' "${SUDO_PASS}" | sudo -S -p '' -v
fi

rm -rf ~/hw-install
mkdir -p ~/hw-install
tar -xzf ~/hashwatcher-hub-pi-bundle.tgz -C ~/hw-install
chmod +x ~/hw-install/install.sh
sudo_cmd bash ~/hw-install/install.sh --source-dir ~/hw-install
sudo_cmd systemctl --no-pager --full status hashwatcher-hub-pi | sed -n '1,60p'
sudo_cmd systemctl --no-pager --full status hashwatcher-ble-provisioner | sed -n '1,60p'

rm -f ~/hashwatcher-hub-pi-bundle.tgz
rm -rf ~/hw-install
EOSSH

echo "Install complete."
echo "Status URL: http://${PI_HOST}:8787/"
echo "Status API: http://${PI_HOST}:8787/api/status"
echo "Live logs: ssh ${PI_TARGET} 'journalctl -u hashwatcher-hub-pi -f'"
