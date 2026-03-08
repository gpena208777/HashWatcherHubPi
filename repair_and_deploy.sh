#!/usr/bin/env bash
set -euo pipefail

PI_IP="${1:-192.168.0.53}"
PI_USER="${2:-hashwatcherhub}"
PI_PASS="${3:-}"

if [[ -z "${PI_PASS}" ]]; then
  echo "Usage: $0 <pi_ip> <pi_user> <pi_password>"
  echo "Example: $0 192.168.0.53 hashwatcherhub 90218"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "${TMP_DIR}" || true; }
trap cleanup EXIT

cat > "${TMP_DIR}/99-tailscale.conf" <<'EOF'
net.ipv4.ip_forward = 1
net.ipv6.conf.all.forwarding = 1
EOF

cat > "${TMP_DIR}/hashwatcher-hub-pi.sudoers" <<'EOF'
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/tailscale, /usr/bin/tailscale up *, /usr/bin/tailscale down, /usr/bin/tailscale logout, /usr/bin/tailscale status *, /usr/bin/tailscale debug *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/systemctl start tailscaled, /usr/bin/systemctl restart tailscaled, /usr/bin/systemctl restart hashwatcher-hub-pi, /usr/bin/systemctl restart hashwatcher-ble-provisioner
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/sbin/reboot
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/sbin/sysctl -w *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/nmcli *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/sbin/wpa_cli *
hashwatcher-hub-pi ALL=(ALL) NOPASSWD: /usr/bin/dpkg -i /opt/hashwatcher-hub-pi/updates/*
EOF

echo "Cleaning stale SSH keys..."
ssh-keygen -R "${PI_IP}" >/dev/null 2>&1 || true
ssh-keygen -R "${PI_USER}.local" >/dev/null 2>&1 || true
ssh-keygen -R "HashWatcherHub.local" >/dev/null 2>&1 || true
rm -f "/tmp/hashwatcher-installer-${PI_IP//[^a-zA-Z0-9]/_}.sock" || true

echo "Checking SSH access..."
SSHPASS="${PI_PASS}" sshpass -e ssh \
  -o PreferredAuthentications=password \
  -o BatchMode=no \
  -o ConnectTimeout=8 \
  -o StrictHostKeyChecking=accept-new \
  "${PI_USER}@${PI_IP}" "echo ssh-ok" >/dev/null

echo "Uploading repair files..."
SSHPASS="${PI_PASS}" sshpass -e scp \
  -o PreferredAuthentications=password \
  -o BatchMode=no \
  -o StrictHostKeyChecking=accept-new \
  "${TMP_DIR}/99-tailscale.conf" \
  "${TMP_DIR}/hashwatcher-hub-pi.sudoers" \
  "${PI_USER}@${PI_IP}:~/"

echo "Repairing sudoers/sysctl on Pi..."
SSHPASS="${PI_PASS}" sshpass -e ssh \
  -o PreferredAuthentications=password \
  -o BatchMode=no \
  -o StrictHostKeyChecking=accept-new \
  "${PI_USER}@${PI_IP}" "bash -s" <<EOF
set -euo pipefail
printf '%s\n' '${PI_PASS}' | sudo -S cp ~/99-tailscale.conf /etc/sysctl.d/99-tailscale.conf
printf '%s\n' '${PI_PASS}' | sudo -S chmod 0644 /etc/sysctl.d/99-tailscale.conf
printf '%s\n' '${PI_PASS}' | sudo -S sysctl -p /etc/sysctl.d/99-tailscale.conf
printf '%s\n' '${PI_PASS}' | sudo -S cp ~/hashwatcher-hub-pi.sudoers /etc/sudoers.d/hashwatcher-hub-pi
printf '%s\n' '${PI_PASS}' | sudo -S chmod 0440 /etc/sudoers.d/hashwatcher-hub-pi
printf '%s\n' '${PI_PASS}' | sudo -S visudo -cf /etc/sudoers.d/hashwatcher-hub-pi
rm -f ~/99-tailscale.conf ~/hashwatcher-hub-pi.sudoers
EOF

echo "Deploying pi-agent..."
cd "${SCRIPT_DIR}"
PI_SUDO_PASS="${PI_PASS}" SSHPASS="${PI_PASS}" ./install_to_pi.sh "${PI_IP}" "${PI_USER}"

echo "Verifying services..."
SSHPASS="${PI_PASS}" sshpass -e ssh \
  -o PreferredAuthentications=password \
  -o BatchMode=no \
  -o StrictHostKeyChecking=accept-new \
  "${PI_USER}@${PI_IP}" \
  "printf '%s\n' '${PI_PASS}' | sudo -S systemctl restart hashwatcher-hub-pi hashwatcher-ble-provisioner && tailscale version && curl -s http://localhost:8787/api/tailscale/status"

echo "Done."
