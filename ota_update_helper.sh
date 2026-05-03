#!/bin/bash
set -euo pipefail

INSTALL_DIR="/opt/hashwatcher-hub-pi"
UPDATES_DIR="${INSTALL_DIR}/updates"
PROGRESS_FILE="${UPDATES_DIR}/update-progress.json"
LOG_FILE="${UPDATES_DIR}/update-install.log"

DEB_PATH="${1:-}"
TARGET_VERSION="${2:-}"
PREVIOUS_VERSION="${3:-}"
DOWNLOAD_SHA="${4:-}"

write_progress() {
    local stage="$1"
    local percent="$2"
    local message="${3:-}"
    local error_message="${4:-}"

    python3 - "$PROGRESS_FILE" "$stage" "$percent" "$TARGET_VERSION" "$PREVIOUS_VERSION" "$DOWNLOAD_SHA" "$message" "$error_message" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone

path, stage, percent, version, previous, sha, message, error = sys.argv[1:9]
payload = {
    "stage": stage,
    "percent": int(percent),
    "updatedAtIso": datetime.now(timezone.utc).isoformat(),
}
if version:
    payload["version"] = version
if previous:
    payload["previousVersion"] = previous
if sha:
    payload["sha256"] = sha
if message:
    payload["message"] = message
if error:
    payload["error"] = error
os.makedirs(os.path.dirname(path), exist_ok=True)
tmp_path = f"{path}.tmp"
with open(tmp_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle)
os.replace(tmp_path, path)
PY
}

if [[ -z "${DEB_PATH}" || ! -f "${DEB_PATH}" ]]; then
    write_progress "failed" 0 "" "Update package missing: ${DEB_PATH}"
    exit 1
fi

install_log() {
    local label="$1"
    shift
    {
        printf '[%s] %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "${label}"
        "$@"
    } >> "${LOG_FILE}" 2>&1
}

write_progress "installing" 100 "Installing package..."

if ! install_log "Running dpkg -i ${DEB_PATH}" dpkg -i "${DEB_PATH}"; then
    tail_output="$(tail -n 25 "${LOG_FILE}" 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g' | cut -c1-400)"
    write_progress "failed" 0 "" "dpkg install failed. ${tail_output}"
    exit 1
fi

installed_version="$(dpkg-query -W -f='${Version}' hashwatcher-hub-pi 2>/dev/null || true)"
write_progress "restarting" 100 "Restarting hub service..."

if ! install_log "Restarting hashwatcher-hub-pi.service" systemctl restart hashwatcher-hub-pi.service; then
    tail_output="$(tail -n 25 "${LOG_FILE}" 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g' | cut -c1-400)"
    write_progress "failed" 0 "" "Service restart failed. ${tail_output}"
    exit 1
fi

write_progress "idle" 100 "Updated to ${installed_version:-${TARGET_VERSION}}."
