#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# Publish HashWatcher Gateway files to the hashwatcherpi public repo.
#
# This copies the customer-facing files from this dev repo into a
# local clone of hashwatcherpi, ready to commit and push.
#
# Usage:
#   ./publish-to-hashwatcherpi.sh /path/to/hashwatcherpi [version]
#
# Example:
#   ./publish-to-hashwatcherpi.sh ~/repos/hashwatcherpi 1.2.0
#
# What goes into hashwatcherpi (public):
#   install.sh          — one-liner installer (curl | sudo bash)
#   firstrun.sh         — first-boot auto-installer for pre-flashed images
#   hashwatcher_hub_agent.py
#   hub_ble_provisioner.py
#   tailscale_setup.py
#   requirements.txt
#   README.md
#
# What stays in RigMonitor (private):
#   build-deb.sh, build-release.sh, debian/, install_to_pi.sh, etc.
# ─────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${1:?Usage: $0 /path/to/hashwatcherpi [version]}"
VERSION="${2:-}"

if [[ ! -d "${TARGET_DIR}/.git" ]]; then
    echo "ERROR: ${TARGET_DIR} is not a git repo. Clone hashwatcherpi first:"
    echo "  gh repo create gpena208777/HashWatcherHubPi --public --clone"
    exit 1
fi

echo "Publishing to: ${TARGET_DIR}"

# Customer-facing files
cp "${SCRIPT_DIR}/install.sh"                   "${TARGET_DIR}/"
cp "${SCRIPT_DIR}/firstrun.sh"                  "${TARGET_DIR}/"
cp "${SCRIPT_DIR}/hashwatcher_hub_agent.py"  "${TARGET_DIR}/"
cp "${SCRIPT_DIR}/hub_ble_provisioner.py"       "${TARGET_DIR}/"
cp "${SCRIPT_DIR}/tailscale_setup.py"           "${TARGET_DIR}/"
cp "${SCRIPT_DIR}/requirements.txt"             "${TARGET_DIR}/"
cp "${SCRIPT_DIR}/icon.png"                    "${TARGET_DIR}/"
cp "${SCRIPT_DIR}/hub.env.prepared"             "${TARGET_DIR}/"
cp "${SCRIPT_DIR}/hashwatcher-hub.service"      "${TARGET_DIR}/"
cp "${SCRIPT_DIR}/hashwatcher-ble-provisioner.service" "${TARGET_DIR}/" 2>/dev/null || true
cp "${SCRIPT_DIR}/hashwatcher-firstrun.service" "${TARGET_DIR}/"

# README for the public repo
cp "${SCRIPT_DIR}/README-hashwatcherpi.md"      "${TARGET_DIR}/README.md"

if [[ -n "${VERSION}" ]]; then
    echo "${VERSION}" > "${TARGET_DIR}/VERSION"
fi

echo ""
echo "Done. Files copied to ${TARGET_DIR}"
echo ""
echo "Next steps:"
echo "  cd ${TARGET_DIR}"
echo "  git add -A && git commit -m 'Update gateway agent'"
echo "  git push"
if [[ -n "${VERSION}" ]]; then
    echo ""
    echo "To create a release with the .deb:"
    echo "  cd ${SCRIPT_DIR}"
    echo "  ./build-deb.sh ${VERSION}"
    echo "  gh release create v${VERSION} hashwatcher-hub-pi_${VERSION}_all.deb --repo gpena208777/HashWatcherHubPi --title \"v${VERSION}\""
fi
