#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# Publish HashWatcher Hub Pi to GitHub (HashWatcherHubPi) in OTA-ready format.
#
# Creates/updates the repo, builds the .deb, pushes code, and creates a
# GitHub Release with the .deb attached. Hubs can then update via OTA.
#
# Prerequisites:
#   - gh CLI (brew install gh) and authenticated
#   - dpkg-deb (or Docker for .deb build on macOS)
#
# Usage:
#   ./publish-to-github.sh [version]
#
# Example:
#   ./publish-to-github.sh 1.0.0
# ─────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION="${1:-1.0.0}"
REPO="gpena208777/HashWatcherHubPi"
CLONE_DIR="${SCRIPT_DIR}/_publish_clone"

echo "=== HashWatcher Hub Pi — Publish to GitHub ==="
echo "Version: ${VERSION}"
echo "Repo:    ${REPO}"
echo ""

# Ensure repo exists
if ! gh repo view "${REPO}" &>/dev/null; then
    echo "Creating repo ${REPO}..."
    gh repo create "${REPO}" --public --description "HashWatcher Hub Pi — OTA-updatable agent for Raspberry Pi"
fi

# Clone or pull
if [[ -d "${CLONE_DIR}" ]]; then
    echo "Updating clone..."
    (cd "${CLONE_DIR}" && git fetch origin 2>/dev/null; DEFAULT_BR="$(git remote show origin 2>/dev/null | grep 'HEAD branch' | cut -d' ' -f5)" || DEFAULT_BR="main"; git reset --hard "origin/${DEFAULT_BR}" 2>/dev/null || true)
else
    echo "Cloning ${REPO}..."
    git clone "https://github.com/${REPO}.git" "${CLONE_DIR}"
fi

# Copy files
echo "Copying pi-agent files..."
"${SCRIPT_DIR}/publish-to-hashwatcherpi.sh" "${CLONE_DIR}" "${VERSION}"

# Build .deb (requires dpkg-deb or Docker)
DEB_OUT="${SCRIPT_DIR}/hashwatcher-hub-pi_${VERSION}_all.deb"
if command -v dpkg-deb &>/dev/null; then
    "${SCRIPT_DIR}/build-deb.sh" "${VERSION}"
elif docker run --rm debian:bookworm-slim which dpkg-deb &>/dev/null 2>&1; then
    echo "Building .deb via Docker..."
    rm -rf "${SCRIPT_DIR}/_build"
    mkdir -p "${SCRIPT_DIR}/_build"
    docker run --rm -v "${SCRIPT_DIR}:/work" -w /work debian:bookworm-slim bash -c "
        apt-get update -qq && apt-get install -y -qq dpkg-dev
        ./build-deb.sh ${VERSION}
    " || {
        echo "Docker build failed. Build manually: ./build-deb.sh ${VERSION}"
        exit 1
    }
else
    echo "WARNING: dpkg-deb not found. Build the .deb manually:"
    echo "  ./build-deb.sh ${VERSION}"
    echo ""
    read -p "Continue without .deb? (y/N) " -n 1 -r
    echo
    [[ "${REPLY}" =~ ^[Yy]$ ]] || exit 1
    DEB_OUT=""
fi

# Commit and push
echo "Committing and pushing..."
cd "${CLONE_DIR}"
git add -A
if git diff --staged --quiet; then
    echo "No changes to commit."
else
    git commit -m "HashWatcher Hub Pi v${VERSION}"
fi
git push -u origin HEAD 2>/dev/null || git push origin main 2>/dev/null || git push origin master 2>/dev/null || true

# Create GitHub release with .deb
if [[ -f "${DEB_OUT}" ]]; then
    echo "Creating release v${VERSION} with .deb..."
    if gh release view "v${VERSION}" --repo "${REPO}" &>/dev/null; then
        echo "Release v${VERSION} already exists. Uploading asset..."
        gh release upload "v${VERSION}" "${DEB_OUT}" --repo "${REPO}" --clobber 2>/dev/null || true
    else
        gh release create "v${VERSION}" "${DEB_OUT}" \
            --repo "${REPO}" \
            --title "v${VERSION}" \
            --notes "HashWatcher Hub Pi v${VERSION}

**Install:** \`sudo dpkg -i hashwatcher-hub-pi_${VERSION}_all.deb\`

**Pre-configured image SSH:** user \`hashwatcherhub\`, password \`90218\`

**Commissioning:** After install, use the HashWatcher app to complete setup (BLE Wi‑Fi, Tailscale key, miner pairing). The app guides you through it.

**OTA:** Hubs check GitHub releases and update automatically."
    fi
    echo "Release: https://github.com/${REPO}/releases/tag/v${VERSION}"
fi

# Cleanup
rm -rf "${CLONE_DIR}"

echo ""
echo "Done. Hubs can now update via OTA from: https://github.com/${REPO}/releases"
