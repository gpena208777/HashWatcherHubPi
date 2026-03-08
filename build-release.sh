#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# Build a release tarball for the HashWatcher Gateway Pi agent.
#
# Run this locally, then upload hashwatcher-hub-pi.tar.gz as a
# GitHub Release asset at gpena208777/HashWatcherHubPi (HashWatcher Hub Pi).
#
# Usage:
#   cd pi-agent && ./build-release.sh
# ─────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${SCRIPT_DIR}/hashwatcher-hub-pi.tar.gz"

FILES=(
    install.sh
    hashwatcher_hub_agent.py
    hub_ble_provisioner.py
    tailscale_setup.py
    requirements.txt
    hashwatcher-hub.service
    hashwatcher-ble-provisioner.service
    hub.env.prepared
    icon.png
)

for f in "${FILES[@]}"; do
    [[ -f "${SCRIPT_DIR}/${f}" ]] || { echo "Missing: ${f}"; exit 1; }
done

tar -C "${SCRIPT_DIR}" -czf "${OUT}" "${FILES[@]}"

echo "Built: ${OUT}"
echo "Size:  $(du -h "${OUT}" | cut -f1)"
echo ""
echo "Upload this file as a GitHub Release asset:"
echo ""
echo "Or build and upload the .deb too:"
echo "  ./build-deb.sh 1.0.1"
echo ""
echo "Upload both to the public repo:"
echo "  gh release create v1.0.1 ${OUT} hashwatcher-hub-pi_1.0.1_all.deb --repo gpena208777/HashWatcherHubPi --title 'v1.0.1' --notes 'HashWatcher Hub Pi v1.0.1'"
