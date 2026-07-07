#!/usr/bin/env python3
"""Hashwatcher Pi Gateway Agent.

Local bridge and miner proxy. No cloud backend -- the app connects to miners
directly (via Tailscale when remote) or through this proxy on port 8787.
"""

import ipaddress
import json
import os
import re
import socket
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv

import hashlib
import html

import tailscale_setup

def resolve_agent_version() -> str:
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "VERSION"),
        "/opt/hashwatcher-hub-pi/VERSION",
    ]
    for path in candidates:
        try:
            with open(path, "r", encoding="utf-8") as f:
                version = f.read().strip()
            if version:
                return version
        except Exception:
            continue
    return "0.0.0-dev"


AGENT_VERSION = resolve_agent_version()
STATUS_API_VERSION = "1"
GITHUB_REPO = "gpena208777/HashWatcherHubPi"


DEFAULT_BITAXE_TUNE_PRESETS: Dict[str, Dict[str, Any]] = {
    "stock": {"label": "Stock", "frequency": 600, "coreVoltage": 1150},
    "mild_oc": {"label": "Mild OC", "frequency": 605, "coreVoltage": 1150},
    "med_oc": {"label": "Med OC", "frequency": 620, "coreVoltage": 1160},
    "heavy_oc": {"label": "Heavy OC", "frequency": 675, "coreVoltage": 1170},
}

CANAAN_WORK_MODE_OPTIONS: Dict[str, str] = {
    "0": "Low",
    "1": "Mid",
    "2": "High",
}

CANAAN_WORK_MODE_PROFILES: Dict[str, List[Dict[str, str]]] = {
    "mini3": [
        {"id": "0", "label": "Heater"},
        {"id": "1", "label": "Mining"},
        {"id": "2", "label": "Night"},
    ],
    "avalon_q": [
        {"id": "0", "label": "Eco"},
        {"id": "1", "label": "Standard"},
        {"id": "2", "label": "Super"},
    ],
    "default": [
        {"id": "0", "label": "Low"},
        {"id": "1", "label": "Mid"},
        {"id": "2", "label": "High"},
    ],
}

# Hub-managed Canaan fan PID defaults.
# The loop computes fan output in percent and sends `ascset|0,fan-spd,<percent>`.
CANAAN_FAN_PID_DEFAULT_MIN_FAN = 35
CANAAN_FAN_PID_DEFAULT_MAX_FAN = 100
CANAAN_FAN_PID_DEFAULT_KP = 2.8
CANAAN_FAN_PID_DEFAULT_KI = 0.06
CANAAN_FAN_PID_DEFAULT_KD = 1.2
CANAAN_FAN_PID_DEFAULT_DEADBAND_C = 0.3
CANAAN_FAN_PID_DEFAULT_MAX_STEP_UP_PERCENT = 16
CANAAN_FAN_PID_DEFAULT_MAX_STEP_DOWN_PERCENT = 6
CANAAN_FAN_PID_DEFAULT_MODE_CHANGE_RESET_FAN_PERCENT = 55
CANAAN_FAN_PID_SCAN_COOLDOWN_SECONDS = 300
CANAAN_FAN_PID_AUTOTUNE_DEFAULT_ENABLED = True
CANAAN_FAN_PID_AUTOTUNE_INTERVAL_SECONDS = 10
HUB_MIN_POLL_SECONDS = 3
HUB_DEFAULT_POLL_SECONDS = 3
CANAAN_PID_LOOP_SECONDS = 1

DEVICE_LOG_STREAMS: Dict[str, Dict[str, Any]] = {
    "hub": {"label": "Hub Service", "units": ["hashwatcher-hub-pi", "hashwatcher-hub"]},
    "tailscale": {"label": "Tailscale", "units": ["tailscaled"]},
    "wifi": {"label": "Wi-Fi / Network", "units": ["NetworkManager", "wpa_supplicant", "dhcpcd", "systemd-networkd"]},
    "kernel": {"label": "Kernel", "kernel": True},
}

DEFAULT_PERSISTENT_LOG_PATH = "/opt/hashwatcher-hub-pi/logs/hub-agent.log"
DEFAULT_PERSISTENT_LOG_MAX_BYTES = 512 * 1024
DEFAULT_PERSISTENT_LOG_BACKUP_COUNT = 2
DEFAULT_LOG_BUNDLE_MAX_BYTES = 1_500_000
DEFAULT_LOG_BUNDLE_COMPACT_MAX_LINES = 180

LOG_BUNDLE_SIGNAL_RE = re.compile(
    r"\b(error|warn(?:ing)?|fail(?:ed|ure)?|panic|segfault|disconnect(?:ed)?|"
    r"timeout|timed out|denied|refused|restart(?:ed|ing)?|stop(?:ped|ping)|"
    r"crash(?:ed)?|fatal|unreachable|exception|not running)\b",
    re.IGNORECASE,
)

LOG_BUNDLE_NOISE_PATTERNS: Dict[str, List[str]] = {
    "all": [
        "pam_unix(sudo:session): session opened for user root",
        "pam_unix(sudo:session): session closed for user root",
        "command=/usr/bin/vcgencmd",
        "command=/usr/bin/journalctl",
    ],
    "tailscale": [
        "magicsock:",
        "derphttp.Client",
        "dns: set",
        "dns: resolvercfg",
        "dns: oscfg",
        "health(warnable=",
        "peerapi:",
        "rebind",
    ],
    "wifi": [
        "device (wlan0): state change",
        "dhcp4 (wlan0): state changed",
        "supplicant interface state:",
        "manager: startup complete",
    ],
}


def env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value else default


def fsync_parent_dir(path: str) -> None:
    directory = os.path.dirname(path) or "."
    dir_fd = None
    try:
        dir_fd = os.open(directory, os.O_RDONLY)
        os.fsync(dir_fd)
    except Exception:
        pass
    finally:
        if dir_fd is not None:
            try:
                os.close(dir_fd)
            except Exception:
                pass


def atomic_write_json(path: str, payload: Dict[str, Any], mode: int = 0o600) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    tmp_path = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp_path, mode)
        os.replace(tmp_path, path)
        fsync_parent_dir(path)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        raise


def env_int(name: str, default: int) -> int:
    try:
        return int(env_str(name, str(default)))
    except ValueError:
        return default


def parse_endpoints(raw: str) -> List[str]:
    endpoints: List[str] = []
    for item in raw.split(","):
        endpoint = item.strip()
        if not endpoint:
            continue
        if not endpoint.startswith("/"):
            endpoint = "/" + endpoint
        endpoints.append(endpoint)
    return endpoints


def pick_first(data: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]
    return None


def sd_notify(message: str) -> None:
    """Send a message to the systemd notify socket (Type=notify / WatchdogSec).

    No-op when not running under systemd (NOTIFY_SOCKET unset), so the agent
    still runs fine from a plain shell.
    """
    sock_path = os.environ.get("NOTIFY_SOCKET", "")
    if not sock_path:
        return
    if sock_path.startswith("@"):
        sock_path = "\0" + sock_path[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.connect(sock_path)
            sock.sendall(message.encode("utf-8"))
    except Exception:
        pass


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_int(value: Any, default: int, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    if minimum is not None:
        parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return default


def normalize_mac(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    hex_only = re.sub(r"[^0-9a-f]", "", raw)
    if len(hex_only) != 12:
        return ""
    return ":".join(hex_only[i:i + 2] for i in range(0, 12, 2))


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _version_tuple(value: str) -> Optional[tuple[int, ...]]:
    """Extract comparable numeric version parts from a string like v1.2.3."""
    parts = re.findall(r"\d+", value or "")
    if not parts:
        return None
    return tuple(int(part) for part in parts[:4])


def is_newer_version(candidate: str, current: str) -> bool:
    """Return True when candidate is semantically newer than current."""
    cand_tuple = _version_tuple(candidate)
    curr_tuple = _version_tuple(current)
    if cand_tuple is None or curr_tuple is None:
        return candidate.strip() != current.strip()

    width = max(len(cand_tuple), len(curr_tuple))
    cand_padded = cand_tuple + (0,) * (width - len(cand_tuple))
    curr_padded = curr_tuple + (0,) * (width - len(curr_tuple))
    return cand_padded > curr_padded


def is_version_at_least(current: str, minimum: str) -> bool:
    current_tuple = _version_tuple(current)
    minimum_tuple = _version_tuple(minimum)
    if current_tuple is None or minimum_tuple is None:
        return current.strip() == minimum.strip()

    width = max(len(current_tuple), len(minimum_tuple))
    current_padded = current_tuple + (0,) * (width - len(current_tuple))
    minimum_padded = minimum_tuple + (0,) * (width - len(minimum_tuple))
    return current_padded >= minimum_padded


class HubState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.started_at = time.time()
        self.last_poll_at_iso: Optional[str] = None
        self.last_poll_error: Optional[str] = None
        self.last_miner_data: Dict[str, Any] = {}

    def set_poll_success(self, data: Dict[str, Any]) -> None:
        with self.lock:
            self.last_poll_at_iso = now_iso()
            self.last_miner_data = data
            self.last_poll_error = None

    def set_poll_error(self, error: str) -> None:
        with self.lock:
            self.last_poll_error = error

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "startedAtIso": datetime.fromtimestamp(self.started_at, tz=timezone.utc).isoformat(),
                "uptimeSeconds": int(time.time() - self.started_at),
                "lastPollAtIso": self.last_poll_at_iso,
                "lastPollError": self.last_poll_error,
                "lastMinerData": self.last_miner_data,
            }


class HubAgent:
    def __init__(self) -> None:
        load_dotenv()

        self.pi_hostname = env_str("PI_HOSTNAME", "hashwatcherhub")
        self.bitaxe_host = env_str("BITAXE_HOST", "")
        self.bitaxe_scheme = env_str("BITAXE_SCHEME", "http")
        self.endpoints = parse_endpoints(env_str("BITAXE_ENDPOINTS", "/system/info,/api/system/info"))
        self.poll_seconds = max(HUB_MIN_POLL_SECONDS, env_int("POLL_SECONDS", HUB_DEFAULT_POLL_SECONDS))
        self.canaan_pid_loop_seconds = max(1, env_int("CANAAN_PID_LOOP_SECONDS", CANAAN_PID_LOOP_SECONDS))
        self.http_timeout_seconds = max(2, env_int("HTTP_TIMEOUT_SECONDS", 5))

        self.status_http_bind = env_str("STATUS_HTTP_BIND", "0.0.0.0")
        self.status_http_port = max(1, env_int("STATUS_HTTP_PORT", 8787))
        self.runtime_config_path = env_str("RUNTIME_CONFIG_PATH", "/opt/hashwatcher-hub-pi/runtime_config.json")
        self.hub_env_path = env_str("HUB_ENV_PATH", "/etc/hashwatcher-hub-pi/hub.env")
        self.update_progress_path = env_str("UPDATE_PROGRESS_PATH", "/opt/hashwatcher-hub-pi/updates/update-progress.json")
        self.update_helper_path = env_str("UPDATE_HELPER_PATH", "/opt/hashwatcher-hub-pi/ota_update_helper.sh")
        self.update_unit_name = env_str("UPDATE_UNIT_NAME", "hashwatcher-hub-update")

        self.agent_id = env_str("AGENT_ID", socket.gethostname())
        self.paired_device_type = ""
        self.paired_miner_mac = ""
        self.paired_miner_hostname = ""
        self.paired = bool(self.bitaxe_host.strip())
        self.user_subnet_cidr = ""
        self.experimental_features_enabled = False
        self.fleet_schedules: List[Dict[str, Any]] = []
        self.fleet_inventory: List[Dict[str, Any]] = []
        self.bitaxe_tune_by_mac: Dict[str, str] = {}
        self.bitaxe_tune_presets: Dict[str, Dict[str, Any]] = {
            key: dict(value) for key, value in DEFAULT_BITAXE_TUNE_PRESETS.items()
        }
        self.bitaxe_tune_overrides_by_mac: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.luxos_profile_rules: Dict[str, Any] = {
            "eco": {"selection": "lowestNumericStep"},
            "turbo": {"requiredStep": 1},
        }
        self.canaan_fan_pid_programs: List[Dict[str, Any]] = []
        self._canaan_pid_runtime: Dict[str, Dict[str, Any]] = {}
        self._canaan_pid_lock = threading.Lock()

        # RLock: update_runtime_config/reset_pairing return get_runtime_config()
        # while holding the lock, which self-deadlocks with a plain Lock.
        self.config_lock = threading.RLock()
        self._fleet_lock = threading.Lock()
        self._main_loop_heartbeat = time.time()
        self._load_runtime_config()
        self._cpu_prev_total = 0
        self._cpu_prev_idle = 0
        total, idle = self._read_cpu_totals()
        if total is not None and idle is not None:
            self._cpu_prev_total = total
            self._cpu_prev_idle = idle

        self.session = requests.Session()
        self.state = HubState()
        self._update_lock = threading.Lock()
        self._update_progress: Dict[str, Any] = self._read_update_progress() or {"stage": "idle"}
        self._led_lock = threading.Lock()
        self._led_default_trigger: Optional[str] = None
        self._self_heal_retry_after: Dict[str, float] = {}

        self._error_log: List[Dict[str, Any]] = []
        self._log_lock = threading.Lock()
        self._max_log_entries = max(200, min(5000, env_int("INTERNAL_LOG_MAX_ENTRIES", 1500)))
        self.persistent_log_path = env_str("PERSISTENT_LOG_PATH", DEFAULT_PERSISTENT_LOG_PATH)
        self.persistent_log_max_bytes = max(128 * 1024, env_int("PERSISTENT_LOG_MAX_BYTES", DEFAULT_PERSISTENT_LOG_MAX_BYTES))
        self.persistent_log_backup_count = max(1, min(5, env_int("PERSISTENT_LOG_BACKUP_COUNT", DEFAULT_PERSISTENT_LOG_BACKUP_COUNT)))
        self.log_bundle_max_bytes = max(256 * 1024, env_int("LOG_BUNDLE_MAX_BYTES", DEFAULT_LOG_BUNDLE_MAX_BYTES))
        self.log_bundle_compact_default = parse_bool(env_str("LOG_BUNDLE_COMPACT_DEFAULT", "1"), default=True)
        self.log_bundle_compact_max_lines = max(
            50,
            min(800, env_int("LOG_BUNDLE_COMPACT_MAX_LINES", DEFAULT_LOG_BUNDLE_COMPACT_MAX_LINES)),
        )
        self._load_persistent_internal_logs()

    def _log(self, source: str, message: str, level: str = "error") -> None:
        entry: Dict[str, Any] = {
            "ts": now_iso(),
            "level": level,
            "source": source,
            "message": message,
        }
        print(f"[{entry['ts']}] [{level.upper()}] [{source}] {message}", flush=True)
        with self._log_lock:
            self._error_log.append(entry)
            if len(self._error_log) > self._max_log_entries:
                self._error_log = self._error_log[-self._max_log_entries:]
        self._append_persistent_internal_log(entry)

    def _persistent_log_files_oldest_first(self) -> List[str]:
        files = [
            f"{self.persistent_log_path}.{idx}"
            for idx in range(self.persistent_log_backup_count, 0, -1)
        ]
        files.append(self.persistent_log_path)
        return files

    def _load_persistent_internal_logs(self) -> None:
        entries: List[Dict[str, Any]] = []
        for path in self._persistent_log_files_oldest_first():
            if not os.path.exists(path):
                continue
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    for raw in f:
                        line = raw.strip()
                        if not line:
                            continue
                        try:
                            parsed = json.loads(line)
                        except Exception:
                            continue
                        if not isinstance(parsed, dict):
                            continue
                        if "message" not in parsed:
                            continue
                        entries.append({
                            "ts": str(parsed.get("ts") or now_iso()),
                            "level": str(parsed.get("level") or "info"),
                            "source": str(parsed.get("source") or ""),
                            "message": str(parsed.get("message") or ""),
                        })
            except Exception as exc:  # pylint: disable=broad-except
                print(f"[{now_iso()}] WARNING: failed to read persistent logs from {path}: {exc}", flush=True)
                continue

        if not entries:
            return
        with self._log_lock:
            self._error_log = entries[-self._max_log_entries:]

    def _rotate_persistent_internal_logs(self) -> None:
        base = self.persistent_log_path
        try:
            oldest = f"{base}.{self.persistent_log_backup_count}"
            if os.path.exists(oldest):
                os.remove(oldest)
            for idx in range(self.persistent_log_backup_count - 1, 0, -1):
                src = f"{base}.{idx}"
                dst = f"{base}.{idx + 1}"
                if os.path.exists(src):
                    os.replace(src, dst)
            if os.path.exists(base):
                os.replace(base, f"{base}.1")
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[{now_iso()}] WARNING: failed rotating persistent logs: {exc}", flush=True)

    def _append_persistent_internal_log(self, entry: Dict[str, Any]) -> None:
        try:
            log_dir = os.path.dirname(self.persistent_log_path)
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)
            line = json.dumps(entry, ensure_ascii=True, separators=(",", ":")) + "\n"
            line_bytes = len(line.encode("utf-8"))
            current_size = 0
            if os.path.exists(self.persistent_log_path):
                current_size = os.path.getsize(self.persistent_log_path)
            if current_size + line_bytes > self.persistent_log_max_bytes:
                self._rotate_persistent_internal_logs()
            with open(self.persistent_log_path, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[{now_iso()}] WARNING: failed writing persistent log entry: {exc}", flush=True)

    def get_internal_logs(self, limit: int = 100, level_filter: Optional[str] = None) -> Dict[str, Any]:
        safe_limit = parse_int(limit, default=100, minimum=1, maximum=1000)
        with self._log_lock:
            entries = list(self._error_log)
        if level_filter:
            entries = [e for e in entries if e.get("level") == level_filter]
        total = len(entries)
        return {"entries": entries[-safe_limit:], "total": total}

    def _bundle_line_has_signal(self, line: str) -> bool:
        return LOG_BUNDLE_SIGNAL_RE.search(line or "") is not None

    def _bundle_line_is_noise(self, stream: str, line: str) -> bool:
        lower = (line or "").lower()
        if stream == "tailscale" and "health(warnable=" in lower and ": ok" not in lower:
            return False
        patterns = LOG_BUNDLE_NOISE_PATTERNS.get("all", []) + LOG_BUNDLE_NOISE_PATTERNS.get(stream, [])
        return any(pattern in lower for pattern in patterns)

    def _normalize_for_dedupe(self, line: str) -> str:
        normalized = re.sub(r"^\d{4}-\d{2}-\d{2}T\S+\s+\S+\s+", "", line or "")
        normalized = re.sub(r"\[\d+\]", "[]", normalized)
        return normalized.strip()

    def _compact_bundle_stream_logs(self, stream: str, logs: str, max_lines: int) -> Dict[str, Any]:
        raw_lines = [line for line in (logs or "").splitlines() if line.strip()]
        kept: List[str] = []
        removed_noise = 0
        removed_repeat = 0
        previous_norm = ""

        for line in raw_lines:
            if line.startswith("====="):
                kept.append(line)
                previous_norm = ""
                continue

            has_signal = self._bundle_line_has_signal(line)
            if self._bundle_line_is_noise(stream, line) and not has_signal:
                removed_noise += 1
                continue

            normalized = self._normalize_for_dedupe(line)
            if normalized and normalized == previous_norm and not has_signal:
                removed_repeat += 1
                continue

            previous_norm = normalized
            kept.append(line)

        removed_older = 0
        if len(kept) > max_lines:
            removed_older = len(kept) - max_lines
            kept = kept[-max_lines:]

        summary_parts: List[str] = []
        if removed_noise:
            summary_parts.append(f"noisy={removed_noise}")
        if removed_repeat:
            summary_parts.append(f"repeat={removed_repeat}")
        if removed_older:
            summary_parts.append(f"older={removed_older}")

        return {
            "logs": "\n".join(kept).strip(),
            "summary": f"[compact] removed {'; '.join(summary_parts)}" if summary_parts else "",
        }

    def build_logs_bundle_text(self, per_stream_limit: int = 300, compact: Optional[bool] = None) -> str:
        safe_limit = parse_int(per_stream_limit, default=300, minimum=50, maximum=1200)
        compact_mode = self.log_bundle_compact_default if compact is None else bool(compact)
        now = now_iso()

        snapshot = self.state.snapshot()
        ts_status = tailscale_setup.status()
        wifi_status = self.get_wifi_status()
        telemetry = self.get_pi_telemetry()
        internal = self.get_internal_logs(limit=safe_limit)

        lines: List[str] = []
        lines.append("HashWatcher Hub Pi Diagnostic Logs Bundle")
        lines.append(f"Generated: {now}")
        lines.append(f"Hostname: {self.pi_hostname}")
        lines.append(f"Agent ID: {self.agent_id}")
        lines.append(f"Agent Version: {AGENT_VERSION}")
        lines.append(f"Status API Version: {STATUS_API_VERSION}")
        lines.append(f"Bundle Mode: {'compact' if compact_mode else 'full'}")
        lines.append(
            "Persistent Internal Log Storage: "
            f"{self.persistent_log_path} (max {self.persistent_log_max_bytes} bytes, backups {self.persistent_log_backup_count})"
        )
        lines.append("")
        lines.append("===== STATUS SNAPSHOT =====")
        status_payload = {
            "generatedAtIso": now,
            "runtime": snapshot,
            "wifi": wifi_status,
            "tailscale": ts_status,
            "piTelemetry": telemetry,
        }
        lines.append(json.dumps(status_payload, indent=2, sort_keys=True, default=str))
        lines.append("")
        lines.append("===== INTERNAL AGENT LOGS =====")
        entries = internal.get("entries", [])
        if entries:
            for entry in entries:
                lines.append(
                    f"[{entry.get('ts', '')}] [{entry.get('level', 'info')}] "
                    f"[{entry.get('source', '')}] {entry.get('message', '')}"
                )
        else:
            lines.append("No internal agent log entries available.")

        for stream in ("hub", "tailscale", "wifi", "kernel"):
            lines.append("")
            lines.append(f"===== {stream.upper()} JOURNAL =====")
            result = self.get_device_logs(stream=stream, limit=safe_limit)
            if not result.get("ok"):
                lines.append(f"Failed to read {stream} logs: {result.get('error', 'unknown error')}")
                continue
            logs = str(result.get("logs") or "").strip()
            if not logs:
                lines.append("No logs returned.")
                continue

            if compact_mode:
                compact_result = self._compact_bundle_stream_logs(
                    stream=stream,
                    logs=logs,
                    max_lines=min(safe_limit, self.log_bundle_compact_max_lines),
                )
                compact_summary = str(compact_result.get("summary") or "").strip()
                compact_logs = str(compact_result.get("logs") or "").strip()
                if compact_summary:
                    lines.append(compact_summary)
                lines.append(compact_logs if compact_logs else "No compact log lines returned.")
                continue

            lines.append(logs)

        bundle_text = "\n".join(lines).rstrip() + "\n"
        raw = bundle_text.encode("utf-8", errors="replace")
        if len(raw) <= self.log_bundle_max_bytes:
            return bundle_text

        marker = (
            f"[trimmed] Bundle exceeded {self.log_bundle_max_bytes} bytes. "
            "Showing most recent content only.\n"
        ).encode("utf-8")
        keep = raw[-max(0, self.log_bundle_max_bytes - len(marker)):]
        newline_idx = keep.find(b"\n")
        if newline_idx != -1:
            keep = keep[newline_idx + 1:]
        clipped = marker + keep
        return clipped.decode("utf-8", errors="replace")

    def _run_capture(self, command: List[str], timeout: int = 10) -> Dict[str, Any]:
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
            return {
                "ok": result.returncode == 0,
                "output": (result.stdout or "").strip(),
                "error": (result.stderr or "").strip(),
                "exitCode": result.returncode,
            }
        except Exception as exc:  # pylint: disable=broad-except
            return {"ok": False, "output": "", "error": str(exc), "exitCode": None}

    def _journal_permission_issue(self, result: Dict[str, Any]) -> bool:
        text = f"{result.get('output', '')}\n{result.get('error', '')}".lower()
        patterns = (
            "insufficient permissions",
            "not seeing messages from other users and the system",
            "users in groups 'adm', 'systemd-journal'",
            "no journal files were opened due to insufficient permissions",
            "permission denied",
        )
        return any(pattern in text for pattern in patterns)

    def _read_journal(self, args: List[str], timeout: int = 10) -> Dict[str, Any]:
        direct = self._run_capture(["journalctl", *args], timeout=timeout)
        if direct.get("ok") and not self._journal_permission_issue(direct):
            return direct

        needs_sudo = self._journal_permission_issue(direct)
        if not needs_sudo:
            return direct

        elevated = self._run_capture(["sudo", "-n", "journalctl", *args], timeout=timeout)
        if elevated.get("ok"):
            return elevated
        return elevated

    def get_device_logs(self, stream: str = "hub", limit: int = 200) -> Dict[str, Any]:
        stream_key = (stream or "hub").strip().lower()
        if stream_key == "all":
            per_stream_limit = max(20, min(400, limit) // 4)
            sections: List[str] = []
            errors: List[str] = []
            for part_stream in ("hub", "tailscale", "wifi", "kernel"):
                result = self.get_device_logs(part_stream, per_stream_limit)
                if result.get("ok"):
                    payload = str(result.get("logs") or "").strip()
                    if payload:
                        sections.append(f"===== {result.get('streamLabel', part_stream)} =====\n{payload}")
                else:
                    errors.append(f"{part_stream}: {result.get('error', 'unknown error')}")
            combined = "\n\n".join(sections).strip()
            if not combined and errors:
                return {"ok": False, "stream": "all", "streamLabel": "All Streams", "error": "; ".join(errors)}
            return {
                "ok": True,
                "stream": "all",
                "streamLabel": "All Streams",
                "limitRequested": limit,
                "linesReturned": len(combined.splitlines()) if combined else 0,
                "generatedAtIso": now_iso(),
                "logs": combined or "No logs available.",
                "errors": errors,
            }

        stream_meta = DEVICE_LOG_STREAMS.get(stream_key)
        if stream_meta is None:
            return {"ok": False, "error": f"Invalid stream '{stream_key}'. Use hub, tailscale, wifi, kernel, or all."}

        safe_limit = parse_int(limit, default=200, minimum=20, maximum=1000)
        base_args = ["-q", "--no-pager", "--output=short-iso", "-n", str(safe_limit)]

        if stream_meta.get("kernel"):
            result = self._read_journal([*base_args, "-k"])
            if not result.get("ok"):
                return {"ok": False, "stream": stream_key, "streamLabel": stream_meta["label"], "error": result.get("error") or "journalctl failed"}
            logs = str(result.get("output") or "").strip()
            return {
                "ok": True,
                "stream": stream_key,
                "streamLabel": stream_meta["label"],
                "limitRequested": safe_limit,
                "linesReturned": len(logs.splitlines()) if logs else 0,
                "generatedAtIso": now_iso(),
                "logs": logs or "No kernel log lines returned.",
            }

        units = [str(unit) for unit in stream_meta.get("units", []) if str(unit).strip()]
        sections: List[str] = []
        errors: List[str] = []
        for unit in units:
            result = self._read_journal([*base_args, "-u", unit])
            if result.get("ok"):
                output = str(result.get("output") or "").strip()
                if output:
                    sections.append(f"===== {unit} =====\n{output}")
                continue
            err = str(result.get("error") or "journalctl failed").strip()
            errors.append(f"{unit}: {err}")

        combined = "\n\n".join(sections).strip()
        if not combined and errors:
            return {
                "ok": False,
                "stream": stream_key,
                "streamLabel": stream_meta["label"],
                "error": "; ".join(errors),
            }

        return {
            "ok": True,
            "stream": stream_key,
            "streamLabel": stream_meta["label"],
            "limitRequested": safe_limit,
            "linesReturned": len(combined.splitlines()) if combined else 0,
            "generatedAtIso": now_iso(),
            "logs": combined or "No log lines returned for this stream.",
            "errors": errors,
        }

    def _load_runtime_config(self) -> None:
        if not os.path.exists(self.runtime_config_path):
            return
        try:
            with open(self.runtime_config_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            bitaxe_host = str(cfg.get("bitaxeHost", "")).strip()
            endpoints = cfg.get("endpoints")
            poll_seconds = cfg.get("pollSeconds")
            paired_device_type = str(cfg.get("deviceType", "")).strip()
            paired_miner_mac = str(cfg.get("minerMac", "")).strip()
            paired_miner_hostname = str(cfg.get("minerHostname", "")).strip()
            paired_flag = cfg.get("paired")
            user_subnet_cidr = str(cfg.get("userSubnetCIDR", "")).strip()
            experimental_features_enabled = bool(cfg.get("experimentalFeaturesEnabled", False))
            fleet_schedules = cfg.get("fleetSchedules", [])
            fleet_inventory = cfg.get("fleetInventory", [])
            bitaxe_tune_by_mac = cfg.get("bitaxeTuneByMac", {})
            bitaxe_tune_presets = cfg.get("bitaxeTunePresets", [])
            bitaxe_tune_overrides_by_mac = cfg.get("bitaxeTuneOverridesByMac", {})
            luxos_profile_rules = cfg.get("luxosProfileRules", {})
            canaan_fan_pid_programs = cfg.get("canaanFanPIDPrograms", [])

            if bitaxe_host:
                self.bitaxe_host = bitaxe_host
            if isinstance(endpoints, list) and endpoints:
                self.endpoints = parse_endpoints(",".join(str(item) for item in endpoints if isinstance(item, str)))
            if isinstance(poll_seconds, int):
                self.poll_seconds = max(HUB_MIN_POLL_SECONDS, poll_seconds)
            if paired_device_type:
                self.paired_device_type = paired_device_type.lower()
            if paired_miner_mac:
                self.paired_miner_mac = paired_miner_mac.lower()
            if paired_miner_hostname:
                self.paired_miner_hostname = paired_miner_hostname
            if isinstance(paired_flag, bool):
                self.paired = paired_flag
            else:
                self.paired = bool(self.bitaxe_host.strip())
            if user_subnet_cidr:
                self.user_subnet_cidr = user_subnet_cidr
            self.experimental_features_enabled = bool(experimental_features_enabled)
            normalized_preset_metadata = self._normalize_fleet_preset_metadata({
                "bitaxeTunes": bitaxe_tune_presets,
                "bitaxeTuneOverridesByMac": bitaxe_tune_overrides_by_mac,
                "luxosProfileRules": luxos_profile_rules,
            })
            self.bitaxe_tune_presets = normalized_preset_metadata["bitaxeTunePresets"]
            self.bitaxe_tune_overrides_by_mac = normalized_preset_metadata["bitaxeTuneOverridesByMac"]
            self.luxos_profile_rules = normalized_preset_metadata["luxosProfileRules"]
            if isinstance(fleet_schedules, list):
                normalized = [self._normalize_fleet_schedule(item) for item in fleet_schedules]
                self.fleet_schedules = [item for item in normalized if item is not None]
            if isinstance(fleet_inventory, list):
                normalized_inventory = [self._normalize_fleet_inventory_item(item) for item in fleet_inventory]
                self.fleet_inventory = [item for item in normalized_inventory if item is not None]
            if isinstance(bitaxe_tune_by_mac, dict):
                normalized_tunes: Dict[str, str] = {}
                for raw_mac, raw_tune in bitaxe_tune_by_mac.items():
                    mac = normalize_mac(raw_mac)
                    tune = str(raw_tune or "").strip().lower()
                    if mac and tune in self.bitaxe_tune_presets:
                        normalized_tunes[mac] = tune
                self.bitaxe_tune_by_mac = normalized_tunes
            if isinstance(canaan_fan_pid_programs, list):
                normalized_pid = [self._normalize_canaan_fan_pid_program(item) for item in canaan_fan_pid_programs]
                self.canaan_fan_pid_programs = [item for item in normalized_pid if item is not None]
            self._hydrate_inventory_with_saved_bitaxe_tunes(self.fleet_inventory)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[{now_iso()}] WARNING: failed to load runtime config: {exc}", flush=True)

    def _runtime_config_payload(self) -> Dict[str, Any]:
        return {
            "bitaxeHost": self.bitaxe_host,
            "endpoints": self.endpoints,
            "pollSeconds": self.poll_seconds,
            "deviceType": self.paired_device_type,
            "minerMac": self.paired_miner_mac,
            "minerHostname": self.paired_miner_hostname,
            "paired": self.paired,
            "userSubnetCIDR": self.user_subnet_cidr,
            "experimentalFeaturesEnabled": self.experimental_features_enabled,
            "fleetSchedules": self.fleet_schedules,
            "fleetInventory": self.fleet_inventory,
            "bitaxeTuneByMac": self.bitaxe_tune_by_mac,
            "bitaxeTunePresets": [
                {"id": key, **preset}
                for key, preset in self.bitaxe_tune_presets.items()
            ],
            "bitaxeTuneOverridesByMac": self.bitaxe_tune_overrides_by_mac,
            "luxosProfileRules": self.luxos_profile_rules,
            "canaanFanPIDPrograms": self.canaan_fan_pid_programs,
            "updatedAtIso": now_iso(),
        }

    def _persist_runtime_config(self) -> None:
        cfg = self._runtime_config_payload()
        atomic_write_json(self.runtime_config_path, cfg, mode=0o600)

    def _read_update_progress(self) -> Optional[Dict[str, Any]]:
        try:
            with open(self.update_progress_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            return payload if isinstance(payload, dict) else None
        except Exception:
            return None

    def _set_update_progress(self, progress: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(progress)
        payload["updatedAtIso"] = now_iso()
        with self._update_lock:
            self._update_progress = payload
        try:
            atomic_write_json(self.update_progress_path, payload, mode=0o600)
        except Exception:
            pass
        return payload

    def get_wifi_status(self) -> Dict[str, Any]:
        """Return real Wi-Fi connection state from wlan0."""
        info: Dict[str, Any] = {
            "connected": False,
            "ssid": None,
            "ip": None,
            "signalDbm": None,
            "interface": "wlan0",
        }

        try:
            result = subprocess.run(
                ["iwgetid", "wlan0", "--raw"], capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0 and result.stdout.strip():
                info["ssid"] = result.stdout.strip()
        except Exception:
            pass

        try:
            result = subprocess.run(
                ["ip", "-4", "-o", "addr", "show", "wlan0"], capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0 and result.stdout.strip():
                match = re.search(r"inet\s+(\d+\.\d+\.\d+\.\d+)", result.stdout)
                if match:
                    info["ip"] = match.group(1)
        except Exception:
            pass

        try:
            result = subprocess.run(
                ["iwconfig", "wlan0"], capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                match = re.search(r"Signal level[=:](-?\d+)\s*dBm", result.stdout)
                if match:
                    info["signalDbm"] = int(match.group(1))
        except Exception:
            pass

        info["connected"] = bool(info["ssid"] and info["ip"])
        return info

    def get_current_step(self) -> Dict[str, Any]:
        """Return current onboarding step for status display."""
        ts_status = tailscale_setup.status()
        ts_authenticated = ts_status.get("authenticated", False)
        wifi = self.get_wifi_status()
        wifi_connected = wifi.get("connected", False)
        routes_approved = ts_status.get("routesApproved", False)
        routes_pending = ts_status.get("routesPending", False)

        step_num = 1
        step_name = "Connect to Wi-Fi"
        step_detail = "Join your network via BLE provisioning in the app."
        if not wifi_connected:
            pass
        elif not ts_authenticated:
            step_num = 2
            step_name = "Set up Tailscale"
            step_detail = f"Wi-Fi connected ({wifi.get('ssid', '?')}). Provide a Tailscale auth key to enable remote access. Generate one at login.tailscale.com \u2192 Settings \u2192 Keys."
        elif not routes_approved:
            step_num = 3
            step_name = "Approve Subnet Route"
            if routes_pending:
                step_detail = "Tailscale is connected, but subnet routing is still waiting for approval in the Tailscale admin console."
            else:
                step_detail = "Tailscale is connected. Approve subnet routing so devices on your LAN are reachable remotely."
        else:
            step_num = 3
            step_name = "Ready"
            ts_ip = ts_status.get("ip", "")
            step_detail = f"Gateway is online. Tailscale IP: {ts_ip}. Miners are accessible locally and via Tailscale."
        return {
            "step": step_num,
            "stepName": step_name,
            "stepDetail": step_detail,
            "totalSteps": 3,
        }

    def is_setup_complete(
        self,
        wifi_status: Optional[Dict[str, Any]] = None,
        ts_status: Optional[Dict[str, Any]] = None,
    ) -> bool:
        wifi = wifi_status if wifi_status is not None else self.get_wifi_status()
        ts = ts_status if ts_status is not None else tailscale_setup.status()
        return bool(
            wifi.get("connected", False)
            and ts.get("authenticated", False)
            and ts.get("online", False)
            and ts.get("routesApproved", False)
            and not ts.get("keyExpired", False)
        )

    def is_paired_status(
        self,
        wifi_status: Optional[Dict[str, Any]] = None,
        ts_status: Optional[Dict[str, Any]] = None,
    ) -> bool:
        wifi = wifi_status if wifi_status is not None else self.get_wifi_status()
        ts = ts_status if ts_status is not None else tailscale_setup.status()
        return bool(
            wifi.get("connected", False)
            and ts.get("authenticated", False)
            and ts.get("routesApproved", False)
        )

    def get_runtime_config(self) -> Dict[str, Any]:
        with self.config_lock:
            return {
                "bitaxeHost": self.bitaxe_host,
                "endpoints": self.endpoints,
                "pollSeconds": self.poll_seconds,
                "deviceType": self.paired_device_type,
                "minerMac": self.paired_miner_mac,
                "minerHostname": self.paired_miner_hostname,
                "paired": self.paired,
                "userSubnetCIDR": self.user_subnet_cidr,
                "experimentalFeaturesEnabled": self.experimental_features_enabled,
                "fleetSchedules": self.fleet_schedules,
                "fleetInventory": self.fleet_inventory,
                "bitaxeTuneByMac": self.bitaxe_tune_by_mac,
                "canaanFanPIDPrograms": self.canaan_fan_pid_programs,
                "piHostname": self.pi_hostname,
                "statusHttpPort": self.status_http_port,
            }

    def update_runtime_config(self, updates: Dict[str, Any]) -> Dict[str, Any]:
        with self.config_lock:
            if "bitaxeHost" in updates:
                host = str(updates["bitaxeHost"]).strip()
                self.bitaxe_host = host
                self.paired = bool(host)
            if "pollSeconds" in updates:
                try:
                    self.poll_seconds = max(HUB_MIN_POLL_SECONDS, int(updates["pollSeconds"]))
                except (ValueError, TypeError):
                    pass
            if "endpoints" in updates and isinstance(updates["endpoints"], list):
                raw_endpoints = [str(item) for item in updates["endpoints"] if str(item).strip()]
                if raw_endpoints:
                    self.endpoints = parse_endpoints(",".join(raw_endpoints))
            if "deviceType" in updates:
                self.paired_device_type = str(updates.get("deviceType") or "").strip().lower()
            if "minerMac" in updates:
                self.paired_miner_mac = str(updates.get("minerMac") or "").strip().lower()
            if "minerHostname" in updates:
                self.paired_miner_hostname = str(updates.get("minerHostname") or "").strip()
            if "paired" in updates:
                self.paired = bool(updates.get("paired"))

            self._persist_runtime_config()
            return self.get_runtime_config()

    def set_experimental_features_enabled(self, enabled: Any) -> Dict[str, Any]:
        with self.config_lock:
            self.experimental_features_enabled = bool(enabled)
            self._persist_runtime_config()
        return {
            "ok": True,
            "experimentalFeaturesEnabled": self.experimental_features_enabled,
            "message": (
                "Experimental hub features enabled."
                if self.experimental_features_enabled
                else "Experimental hub features disabled."
            ),
        }

    def feature_gates_status(self) -> Dict[str, Any]:
        return {
            "experimentalFeaturesEnabled": bool(self.experimental_features_enabled),
        }

    def local_clock_status(self) -> Dict[str, Any]:
        local_now = datetime.now().astimezone()
        offset = local_now.utcoffset()
        offset_minutes = int(offset.total_seconds() / 60) if offset is not None else 0
        tz_name = local_now.tzname() or time.tzname[0] if time.tzname else "local"
        return {
            "localTimeIso": local_now.isoformat(),
            "timezone": tz_name,
            "utcOffsetMinutes": offset_minutes,
        }

    def _normalize_fleet_device_type(self, raw_device_type: Any) -> str:
        raw = str(raw_device_type or "").strip().lower()
        if raw in {"bitaxe", "nerdq", "bitdsk", "octaxe"}:
            return "bitaxe"
        if raw == "canaan":
            return "canaan"
        if raw in {"luxos", "luxor"}:
            return "luxos"
        if raw == "braiins":
            return "braiins"
        return raw

    def _normalize_canaan_subtype(self, value: Any) -> str:
        text = str(value or "").strip().lower()
        if "avalon" in text and "q" in text:
            return "avalon_q"
        if "mini3" in text:
            return "mini3"
        if "nano3" in text:
            return "nano3"
        return "unknown"

    def _canaan_work_modes_for_subtype(self, subtype: str) -> List[Dict[str, str]]:
        profile_key = subtype if subtype in CANAAN_WORK_MODE_PROFILES else "default"
        if profile_key == "nano3":
            profile_key = "default"
        profile = CANAAN_WORK_MODE_PROFILES.get(profile_key, CANAAN_WORK_MODE_PROFILES["default"])
        return [dict(item) for item in profile]

    def _normalize_canaan_mode_options(self, raw_modes: Any, subtype: str) -> List[Dict[str, str]]:
        if not isinstance(raw_modes, list):
            return self._canaan_work_modes_for_subtype(subtype)
        normalized: List[Dict[str, str]] = []
        for item in raw_modes:
            if not isinstance(item, dict):
                continue
            mode_id = str(item.get("id") or "").strip()
            label = str(item.get("label") or "").strip()
            if not mode_id:
                continue
            normalized.append({
                "id": mode_id,
                "label": label or f"Mode {mode_id}",
            })
        return normalized or self._canaan_work_modes_for_subtype(subtype)

    def _is_valid_canaan_mode_value(self, value: str) -> bool:
        valid_values = {str(key) for key in CANAAN_WORK_MODE_OPTIONS.keys()}
        for options in CANAAN_WORK_MODE_PROFILES.values():
            valid_values.update(str(item.get("id") or "").strip() for item in options)
        return value in valid_values

    def _normalize_fleet_inventory_item(self, raw: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return None

        device_type = self._normalize_fleet_device_type(raw.get("deviceType"))
        if device_type not in {"bitaxe", "canaan", "luxos", "braiins"}:
            return None

        ip = str(raw.get("ip") or "").strip()
        mac = normalize_mac(raw.get("mac"))
        if not ip and not mac:
            return None

        model = str(raw.get("model") or "").strip() or None
        name = str(raw.get("name") or raw.get("hostname") or "").strip()
        if not name:
            name = model or ip or mac

        subtype_raw = raw.get("deviceSubtype") or raw.get("subtype")
        subtype = self._normalize_canaan_subtype(subtype_raw if subtype_raw is not None else model)
        saved_tune = str(raw.get("savedBitaxeTune") or "").strip().lower()
        if saved_tune not in self.bitaxe_tune_presets:
            saved_tune = self.bitaxe_tune_by_mac.get(mac, "") if mac else ""
        if saved_tune not in self.bitaxe_tune_presets:
            saved_tune = ""

        supported_modes = (
            self._normalize_canaan_mode_options(raw.get("supportedWorkModes"), subtype)
            if device_type == "canaan"
            else []
        )
        username = str(raw.get("username") or "").strip() or None
        password = str(raw.get("password") or "").strip() or None

        device_id = (mac or ip).lower()
        return {
            "id": device_id,
            "name": name,
            "deviceType": device_type,
            "ip": ip,
            "mac": mac or None,
            "model": model,
            "deviceSubtype": subtype if device_type == "canaan" else None,
            "supportedWorkModes": supported_modes,
            "savedBitaxeTune": saved_tune if device_type == "bitaxe" and saved_tune else None,
            "username": username if device_type == "braiins" else None,
            "password": password if device_type == "braiins" else None,
            "lastSeenAtIso": str(raw.get("lastSeenAtIso") or "").strip() or None,
            "lastCapabilityProbeAtIso": str(raw.get("lastCapabilityProbeAtIso") or "").strip() or None,
            "lastCapabilityProbeError": str(raw.get("lastCapabilityProbeError") or "").strip() or None,
        }

    def _hydrate_inventory_with_saved_bitaxe_tunes(self, inventory: List[Dict[str, Any]]) -> None:
        for item in inventory:
            if str(item.get("deviceType") or "").strip().lower() != "bitaxe":
                continue
            mac = normalize_mac(item.get("mac"))
            if not mac:
                continue
            saved = self.bitaxe_tune_by_mac.get(mac)
            if saved in self.bitaxe_tune_presets:
                item["savedBitaxeTune"] = saved

    def _send_cgminer_json_command(
        self,
        target_ip: str,
        command: str,
        port: int = 4028,
        timeout_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        sock = None
        try:
            connect_timeout = timeout_seconds if timeout_seconds is not None else float(self.http_timeout_seconds)
            connect_timeout = max(0.4, float(connect_timeout))
            sock = socket.create_connection((target_ip, port), timeout=connect_timeout)
            sock.settimeout(5)
            payload = json.dumps({"command": command}) + "\n"
            sock.sendall(payload.encode("utf-8"))
            chunks: List[bytes] = []
            while True:
                try:
                    chunk = sock.recv(4096)
                except socket.timeout:
                    break
                if not chunk:
                    break
                chunks.append(chunk)
            raw_text = b"".join(chunks).decode("utf-8", errors="ignore").replace("\x00", "").strip()
            if not raw_text:
                return {"ok": False, "error": "No response from miner."}
            try:
                data = json.loads(raw_text)
            except Exception as exc:  # pylint: disable=broad-except
                return {"ok": False, "error": f"Invalid JSON response: {exc}", "raw": raw_text[:6000]}
            return {"ok": True, "data": data, "raw": raw_text[:6000]}
        except Exception as exc:  # pylint: disable=broad-except
            return {"ok": False, "error": str(exc)}
        finally:
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass

    def _infer_canaan_subtype_from_version(self, version_entry: Dict[str, Any]) -> str:
        parts: List[str] = []
        for key in ["PROD", "Model", "MODEL", "HWTYPE", "SWTYPE", "Type", "type"]:
            value = version_entry.get(key)
            if isinstance(value, str) and value.strip():
                parts.append(value.strip().lower())
        combined = " ".join(parts)
        if "avalon" in combined and "q" in combined:
            return "avalon_q"
        if "mini3" in combined:
            return "mini3"
        if "nano3" in combined:
            return "nano3"
        return "unknown"

    def _probe_canaan_capabilities(self, target_ip: str, timeout_seconds: Optional[float] = None) -> Dict[str, Any]:
        result = self._send_cgminer_json_command(target_ip, "version", timeout_seconds=timeout_seconds)
        if not result.get("ok"):
            return {"ok": False, "error": result.get("error") or "Version command failed."}
        data = result.get("data")
        if not isinstance(data, dict):
            return {"ok": False, "error": "Unexpected version payload."}

        version_entries = data.get("VERSION")
        if not isinstance(version_entries, list) or not version_entries:
            return {"ok": False, "error": "VERSION data missing in response."}
        first = version_entries[0] if isinstance(version_entries[0], dict) else {}
        model = (
            str(first.get("Model") or "").strip()
            or str(first.get("MODEL") or "").strip()
            or str(first.get("PROD") or "").strip()
        )
        mac = normalize_mac(first.get("MAC") or first.get("Mac") or first.get("mac"))
        subtype = self._infer_canaan_subtype_from_version(first)
        supported_modes = self._canaan_work_modes_for_subtype(subtype)

        return {
            "ok": True,
            "model": model or None,
            "mac": mac or None,
            "deviceSubtype": subtype,
            "supportedWorkModes": supported_modes,
            "raw": result.get("raw"),
        }

    def _refresh_canaan_capabilities_for_inventory(self, inventory: List[Dict[str, Any]]) -> None:
        probe_ts = now_iso()
        for item in inventory:
            if str(item.get("deviceType") or "").strip().lower() != "canaan":
                continue
            ip = str(item.get("ip") or "").strip()
            if not ip:
                continue
            probe = self._probe_canaan_capabilities(ip)
            item["lastCapabilityProbeAtIso"] = probe_ts
            if probe.get("ok"):
                mac = normalize_mac(probe.get("mac") or item.get("mac"))
                if mac:
                    item["mac"] = mac
                if probe.get("model"):
                    item["model"] = probe.get("model")
                subtype = self._normalize_canaan_subtype(probe.get("deviceSubtype"))
                item["deviceSubtype"] = subtype
                item["supportedWorkModes"] = self._normalize_canaan_mode_options(probe.get("supportedWorkModes"), subtype)
                item["lastCapabilityProbeError"] = None
                if str(item.get("name") or "").strip() in {"", ip} and item.get("model"):
                    item["name"] = str(item.get("model"))
            else:
                item["lastCapabilityProbeError"] = str(probe.get("error") or "Unknown error")
                subtype = self._normalize_canaan_subtype(item.get("deviceSubtype") or item.get("model"))
                item["deviceSubtype"] = subtype
                item["supportedWorkModes"] = self._normalize_canaan_mode_options(item.get("supportedWorkModes"), subtype)

    def set_fleet_inventory(self, devices: Any) -> Dict[str, Any]:
        if not isinstance(devices, list):
            return {"ok": False, "error": "inventory must be an array."}

        normalized = [self._normalize_fleet_inventory_item(item) for item in devices]
        sanitized = [item for item in normalized if item is not None]
        if len(sanitized) > 512:
            return {"ok": False, "error": "Too many inventory devices. Max 512."}

        working_inventory = [dict(item) for item in sanitized]
        self._hydrate_inventory_with_saved_bitaxe_tunes(working_inventory)
        self._refresh_canaan_capabilities_for_inventory(working_inventory)

        with self._fleet_lock:
            self.fleet_inventory = working_inventory
            for device in self.fleet_inventory:
                mac = normalize_mac(device.get("mac"))
                if not mac:
                    continue
                device["id"] = mac
                if str(device.get("deviceType") or "").strip().lower() == "bitaxe":
                    tune = str(device.get("savedBitaxeTune") or "").strip().lower()
                    if tune in self.bitaxe_tune_presets:
                        self.bitaxe_tune_by_mac[mac] = tune

        with self.config_lock:
            self._persist_runtime_config()

        return {
            "ok": True,
            "message": f"Saved {len(sanitized)} fleet inventory device(s).",
            "fleetManager": self.get_fleet_manager_status(),
        }

    def _find_inventory_device_by_mac_or_ip(self, target_mac: str, target_ip: str, device_type_hint: str) -> Optional[Dict[str, Any]]:
        normalized_mac = normalize_mac(target_mac)
        normalized_ip = str(target_ip or "").strip()
        normalized_type = self._normalize_fleet_device_type(device_type_hint)

        if normalized_mac:
            for item in self.fleet_inventory:
                if normalize_mac(item.get("mac")) == normalized_mac:
                    return item
        if normalized_ip:
            for item in self.fleet_inventory:
                item_ip = str(item.get("ip") or "").strip()
                item_type = self._normalize_fleet_device_type(item.get("deviceType"))
                if item_ip != normalized_ip:
                    continue
                if normalized_type and item_type and item_type != normalized_type:
                    continue
                return item
        return None

    def _normalize_fleet_schedule(self, raw: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return None

        target_ip = str(raw.get("targetIp") or "").strip()
        target_mac = normalize_mac(raw.get("targetMac"))
        if not target_ip and not target_mac:
            return None

        action_type = str(raw.get("actionType") or "").strip().lower()
        if action_type not in {"bitaxe_tune", "canaan_work_mode", "luxos_profile_preset", "luxos_sleep", "luxos_wakeup", "fleet_preset"}:
            return None

        mode_value_raw = raw.get("modeValue")
        mode_value = str(mode_value_raw).strip().lower()
        if action_type == "bitaxe_tune":
            if mode_value not in self.bitaxe_tune_presets:
                return None
        elif action_type == "canaan_work_mode":
            if not self._is_valid_canaan_mode_value(mode_value):
                return None
        elif action_type == "luxos_sleep":
            mode_value = "sleep"
        elif action_type == "luxos_wakeup":
            mode_value = "wakeup"
        elif mode_value not in {"eco", "turbo"}:
            return None

        device_type = self._normalize_fleet_device_type(raw.get("deviceType"))
        if not device_type:
            if action_type == "bitaxe_tune":
                device_type = "bitaxe"
            elif action_type in {"luxos_profile_preset", "luxos_sleep", "luxos_wakeup", "fleet_preset"}:
                device_type = "luxos"
            else:
                device_type = "canaan"

        hour = parse_int(raw.get("hour"), default=0, minimum=0, maximum=23)
        minute = parse_int(raw.get("minute"), default=0, minimum=0, maximum=59)
        days_mask = parse_int(raw.get("daysMask"), default=127, minimum=1, maximum=127)
        target_identity = target_mac or target_ip
        schedule_id = str(raw.get("id") or "").strip() or hashlib.sha1(
            f"{target_identity}:{action_type}:{mode_value}:{hour}:{minute}:{days_mask}".encode("utf-8")
        ).hexdigest()[:12]
        name = str(raw.get("name") or "").strip()
        if not name:
            pretty_mode = mode_value.replace("_", " ").title()
            name = f"{target_identity} {pretty_mode}"

        last_slot = str(raw.get("lastTriggeredSlot") or "").strip() or None
        last_triggered_at_iso = str(raw.get("lastTriggeredAtIso") or "").strip() or None
        last_result = raw.get("lastResult")
        if not isinstance(last_result, dict):
            last_result = None

        return {
            "id": schedule_id,
            "name": name,
            "enabled": bool(raw.get("enabled", True)),
            "targetIp": target_ip,
            "targetMac": target_mac or None,
            "deviceType": device_type,
            "actionType": action_type,
            "modeValue": mode_value,
            "hour": hour,
            "minute": minute,
            "daysMask": days_mask,
            "lastTriggeredSlot": last_slot,
            "lastTriggeredAtIso": last_triggered_at_iso,
            "lastResult": last_result,
        }

    def _normalize_fleet_preset_metadata(self, raw: Any) -> Dict[str, Any]:
        source = raw if isinstance(raw, dict) else {}
        presets: Dict[str, Dict[str, Any]] = {
            key: dict(value) for key, value in DEFAULT_BITAXE_TUNE_PRESETS.items()
        }
        raw_tunes = source.get("bitaxeTunes")
        if isinstance(raw_tunes, list):
            for item in raw_tunes:
                if not isinstance(item, dict):
                    continue
                tune_id = str(item.get("id") or "").strip().lower()
                if tune_id not in presets:
                    continue
                frequency = parse_int(item.get("frequency"), default=presets[tune_id]["frequency"], minimum=50, maximum=2000)
                core_voltage = parse_int(
                    item.get("coreVoltage", item.get("voltage")),
                    default=presets[tune_id]["coreVoltage"],
                    minimum=600,
                    maximum=2000,
                )
                label = str(item.get("label") or presets[tune_id]["label"]).strip() or presets[tune_id]["label"]
                presets[tune_id] = {
                    "label": label,
                    "frequency": frequency,
                    "coreVoltage": core_voltage,
                }

        overrides_by_mac: Dict[str, Dict[str, Dict[str, Any]]] = {}
        raw_overrides = source.get("bitaxeTuneOverridesByMac")
        if isinstance(raw_overrides, dict):
            for raw_mac, raw_tunes_for_mac in raw_overrides.items():
                mac = normalize_mac(raw_mac)
                if not mac or not isinstance(raw_tunes_for_mac, dict):
                    continue
                normalized_for_mac: Dict[str, Dict[str, Any]] = {}
                for raw_tune_id, raw_tune_values in raw_tunes_for_mac.items():
                    tune_id = str(raw_tune_id or "").strip().lower()
                    if tune_id not in presets or not isinstance(raw_tune_values, dict):
                        continue
                    frequency = parse_int(raw_tune_values.get("frequency"), default=presets[tune_id]["frequency"], minimum=50, maximum=2000)
                    core_voltage = parse_int(
                        raw_tune_values.get("coreVoltage", raw_tune_values.get("voltage")),
                        default=presets[tune_id]["coreVoltage"],
                        minimum=600,
                        maximum=2000,
                    )
                    normalized_for_mac[tune_id] = {
                        "label": presets[tune_id]["label"],
                        "frequency": frequency,
                        "coreVoltage": core_voltage,
                    }
                if normalized_for_mac:
                    overrides_by_mac[mac] = normalized_for_mac

        rules = source.get("luxosProfileRules")
        if not isinstance(rules, dict):
            rules = {}
        eco_rule = rules.get("eco") if isinstance(rules.get("eco"), dict) else {}
        turbo_rule = rules.get("turbo") if isinstance(rules.get("turbo"), dict) else {}
        luxos_rules = {
            "eco": {"selection": "lowestNumericStep"},
            "turbo": {
                "requiredStep": parse_int(turbo_rule.get("requiredStep"), default=1, minimum=-100, maximum=100),
            },
        }
        if str(eco_rule.get("selection") or "").strip():
            luxos_rules["eco"]["selection"] = "lowestNumericStep"

        return {
            "bitaxeTunePresets": presets,
            "bitaxeTuneOverridesByMac": overrides_by_mac,
            "luxosProfileRules": luxos_rules,
        }

    def set_fleet_preset_metadata(self, payload: Any) -> Dict[str, Any]:
        normalized = self._normalize_fleet_preset_metadata(payload)
        with self._fleet_lock:
            self.bitaxe_tune_presets = normalized["bitaxeTunePresets"]
            self.bitaxe_tune_overrides_by_mac = normalized["bitaxeTuneOverridesByMac"]
            self.luxos_profile_rules = normalized["luxosProfileRules"]
            valid_tunes = set(self.bitaxe_tune_presets.keys())
            self.bitaxe_tune_by_mac = {
                mac: tune
                for mac, tune in self.bitaxe_tune_by_mac.items()
                if tune in valid_tunes
            }
            self._hydrate_inventory_with_saved_bitaxe_tunes(self.fleet_inventory)

        with self.config_lock:
            self._persist_runtime_config()

        return {
            "ok": True,
            "message": "Fleet preset metadata synced.",
            "fleetManager": self.get_fleet_manager_status(),
        }

    def apply_fleet_preset_now(self, payload: Any) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {"ok": False, "error": "Payload must be an object."}

        preset_id = str(payload.get("preset") or payload.get("presetId") or "").strip().lower()
        if preset_id not in {"eco", "turbo"}:
            return {"ok": False, "error": "Preset must be eco or turbo."}

        raw_targets = payload.get("targets")
        if not isinstance(raw_targets, list):
            return {"ok": False, "error": "targets must be an array."}

        targets = [self._normalize_fleet_inventory_item(item) for item in raw_targets]
        targets = [item for item in targets if item is not None]
        if not targets:
            return {"ok": False, "error": "No compatible targets were supplied."}
        if len(targets) > 256:
            return {"ok": False, "error": "Too many targets. Max 256."}

        results: List[Dict[str, Any]] = []
        success_count = 0
        skipped_count = 0
        failure_count = 0

        for target in targets:
            result = self._apply_fleet_preset_to_target(preset_id, target)
            result["targetName"] = target.get("name")
            result["targetIp"] = target.get("ip")
            result["targetMac"] = target.get("mac")
            result["deviceType"] = target.get("deviceType")
            results.append(result)
            if result.get("ok"):
                success_count += 1
            elif result.get("skipped"):
                skipped_count += 1
            else:
                failure_count += 1

        message_parts = [
            f"Applied {preset_id.title()} to {success_count} device{'s' if success_count != 1 else ''}"
        ]
        if skipped_count:
            message_parts.append(f"{skipped_count} skipped")
        if failure_count:
            message_parts.append(f"{failure_count} failed")

        return {
            "ok": success_count > 0 and failure_count == 0,
            "partial": success_count > 0 and failure_count > 0,
            "message": " • ".join(message_parts),
            "successCount": success_count,
            "skippedCount": skipped_count,
            "failureCount": failure_count,
            "results": results,
            "fleetManager": self.get_fleet_manager_status(),
        }

    def fleet_presets(self) -> Dict[str, Any]:
        return {
            "bitaxeTunes": [
                {
                    "id": key,
                    "label": preset["label"],
                    "frequency": preset["frequency"],
                    "coreVoltage": preset["coreVoltage"],
                }
                for key, preset in self.bitaxe_tune_presets.items()
            ],
            "canaanWorkModes": [
                {"id": key, "label": label}
                for key, label in CANAAN_WORK_MODE_OPTIONS.items()
            ],
            "bitaxeTuneOverridesByMac": {
                mac: dict(tunes)
                for mac, tunes in self.bitaxe_tune_overrides_by_mac.items()
            },
            "luxosProfileRules": dict(self.luxos_profile_rules),
        }

    def get_fleet_manager_status(self) -> Dict[str, Any]:
        with self._fleet_lock:
            schedules = [dict(item) for item in self.fleet_schedules]
            inventory = [self._public_fleet_inventory_item(item) for item in self.fleet_inventory]
            bitaxe_tune_by_mac = dict(self.bitaxe_tune_by_mac)
        return {
            "clock": self.local_clock_status(),
            "schedules": schedules,
            "presets": self.fleet_presets(),
            "inventory": inventory,
            "bitaxeTuneByMac": bitaxe_tune_by_mac,
        }

    def _public_fleet_inventory_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        public = dict(item)
        public.pop("password", None)
        if public.get("username"):
            public["hasCredentials"] = True
        public.pop("username", None)
        return public

    def set_fleet_schedules(self, schedules: Any) -> Dict[str, Any]:
        if not isinstance(schedules, list):
            return {"ok": False, "error": "schedules must be an array."}

        normalized = [self._normalize_fleet_schedule(item) for item in schedules]
        sanitized = [item for item in normalized if item is not None]
        if len(sanitized) > 256:
            return {"ok": False, "error": "Too many schedules. Max 256."}

        with self._fleet_lock:
            self.fleet_schedules = sanitized
            for schedule in sanitized:
                if schedule.get("actionType") != "bitaxe_tune":
                    continue
                target_mac = normalize_mac(schedule.get("targetMac"))
                mode_value = str(schedule.get("modeValue") or "").strip().lower()
                if target_mac and mode_value in self.bitaxe_tune_presets:
                    self.bitaxe_tune_by_mac[target_mac] = mode_value
            self._hydrate_inventory_with_saved_bitaxe_tunes(self.fleet_inventory)

        with self.config_lock:
            self._persist_runtime_config()

        return {
            "ok": True,
            "message": f"Saved {len(sanitized)} fleet schedule(s).",
            "fleetManager": self.get_fleet_manager_status(),
        }

    def _normalize_canaan_fan_pid_program(self, raw: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return None

        target_ip = str(raw.get("targetIp") or "").strip()
        target_mac = normalize_mac(raw.get("targetMac"))
        if not target_ip and not target_mac:
            return None

        target_temp = to_float(raw.get("targetTempC"))
        if target_temp is None:
            target_temp = 70.0
        target_temp = max(30.0, min(120.0, target_temp))

        min_fan = parse_int(
            raw.get("minFanPercent"),
            default=CANAAN_FAN_PID_DEFAULT_MIN_FAN,
            minimum=20,
            maximum=100,
        )
        max_fan = parse_int(
            raw.get("maxFanPercent"),
            default=CANAAN_FAN_PID_DEFAULT_MAX_FAN,
            minimum=min_fan,
            maximum=100,
        )
        if min_fan > max_fan:
            min_fan, max_fan = max_fan, min_fan

        kp = to_float(raw.get("kp"))
        if kp is None:
            kp = CANAAN_FAN_PID_DEFAULT_KP
        kp = max(0.0, min(12.0, kp))

        ki = to_float(raw.get("ki"))
        if ki is None:
            ki = CANAAN_FAN_PID_DEFAULT_KI
        ki = max(0.0, min(2.0, ki))

        kd = to_float(raw.get("kd"))
        if kd is None:
            kd = CANAAN_FAN_PID_DEFAULT_KD
        kd = max(0.0, min(8.0, kd))

        deadband_c = to_float(raw.get("deadbandC"))
        if deadband_c is None:
            deadband_c = CANAAN_FAN_PID_DEFAULT_DEADBAND_C
        deadband_c = max(0.0, min(5.0, deadband_c))

        max_step_up = parse_int(
            raw.get("maxStepUpPercent"),
            default=CANAAN_FAN_PID_DEFAULT_MAX_STEP_UP_PERCENT,
            minimum=1,
            maximum=40,
        )
        max_step_down = parse_int(
            raw.get("maxStepDownPercent"),
            default=CANAAN_FAN_PID_DEFAULT_MAX_STEP_DOWN_PERCENT,
            minimum=1,
            maximum=40,
        )
        mode_change_reset_fan = parse_int(
            raw.get("modeChangeResetFanPercent"),
            default=CANAAN_FAN_PID_DEFAULT_MODE_CHANGE_RESET_FAN_PERCENT,
            minimum=min_fan,
            maximum=max_fan,
        )
        auto_tune_enabled = parse_bool(raw.get("autoTuneEnabled"), default=CANAAN_FAN_PID_AUTOTUNE_DEFAULT_ENABLED)
        auto_tune_interval_sec = parse_int(
            raw.get("autoTuneIntervalSec"),
            default=CANAAN_FAN_PID_AUTOTUNE_INTERVAL_SECONDS,
            minimum=5,
            maximum=300,
        )

        identity = target_mac or target_ip
        program_id = str(raw.get("id") or "").strip()
        if not program_id:
            program_id = hashlib.sha1(f"canaan-fan-pid:{identity}".encode("utf-8")).hexdigest()[:12]

        created_at = str(raw.get("createdAtIso") or "").strip() or now_iso()
        updated_at = str(raw.get("updatedAtIso") or "").strip() or created_at
        device_name = str(raw.get("deviceName") or raw.get("name") or "").strip()
        if not device_name:
            device_name = target_ip or target_mac or "Canaan Device"

        normalized: Dict[str, Any] = {
            "id": program_id,
            "enabled": parse_bool(raw.get("enabled"), default=True),
            "deviceName": device_name,
            "targetIp": target_ip,
            "targetMac": target_mac or None,
            "targetTempC": round(target_temp, 2),
            "minFanPercent": min_fan,
            "maxFanPercent": max_fan,
            "kp": round(kp, 4),
            "ki": round(ki, 4),
            "kd": round(kd, 4),
            "deadbandC": round(deadband_c, 3),
            "maxStepUpPercent": max_step_up,
            "maxStepDownPercent": max_step_down,
            "modeChangeResetFanPercent": mode_change_reset_fan,
            "autoTuneEnabled": auto_tune_enabled,
            "autoTuneIntervalSec": auto_tune_interval_sec,
            "autoTuneLastAction": str(raw.get("autoTuneLastAction") or "").strip() or None,
            "autoTuneLastAtIso": str(raw.get("autoTuneLastAtIso") or "").strip() or None,
            "createdAtIso": created_at,
            "updatedAtIso": updated_at,
            "lastRunAtIso": str(raw.get("lastRunAtIso") or "").strip() or None,
            "lastCommandAtIso": str(raw.get("lastCommandAtIso") or "").strip() or None,
            "lastResolvedAtIso": str(raw.get("lastResolvedAtIso") or "").strip() or None,
            "lastIpScanAtIso": str(raw.get("lastIpScanAtIso") or "").strip() or None,
            "lastIpScanEpoch": to_float(raw.get("lastIpScanEpoch")),
            "lastChipTempC": to_float(raw.get("lastChipTempC")),
            "lastObservedFanPercent": parse_int(raw.get("lastObservedFanPercent"), default=0, minimum=0, maximum=100),
            "lastFanPercent": parse_int(raw.get("lastFanPercent"), default=0, minimum=0, maximum=100),
            "lastControlErrorC": to_float(raw.get("lastControlErrorC")),
            "lastCommandStatus": str(raw.get("lastCommandStatus") or "").strip() or None,
            "lastError": str(raw.get("lastError") or "").strip() or None,
        }
        return normalized

    def _find_canaan_fan_pid_program_index(self, program_id: str = "", target_mac: str = "", target_ip: str = "") -> int:
        normalized_mac = normalize_mac(target_mac)
        normalized_ip = str(target_ip or "").strip()
        normalized_id = str(program_id or "").strip()
        for idx, program in enumerate(self.canaan_fan_pid_programs):
            if normalized_id and str(program.get("id") or "").strip() == normalized_id:
                return idx
            if normalized_mac and normalize_mac(program.get("targetMac")) == normalized_mac:
                return idx
            if normalized_ip and str(program.get("targetIp") or "").strip() == normalized_ip:
                return idx
        return -1

    def _upsert_inventory_for_canaan_pid(self, device_name: str, target_ip: str, target_mac: str) -> bool:
        normalized_mac = normalize_mac(target_mac)
        normalized_ip = str(target_ip or "").strip()
        if not normalized_ip and not normalized_mac:
            return False

        changed = False
        acquired = self._fleet_lock.acquire(timeout=0.25)
        if not acquired:
            # Never block PID start/update requests on inventory lock contention.
            self._log("canaan-pid", "Fleet inventory lock busy; skipping inventory upsert for this cycle.", level="warn")
            return False
        try:
            matched: Optional[Dict[str, Any]] = None
            if normalized_mac:
                for item in self.fleet_inventory:
                    if normalize_mac(item.get("mac")) == normalized_mac:
                        matched = item
                        break
            if matched is None and normalized_ip:
                for item in self.fleet_inventory:
                    if str(item.get("deviceType") or "").strip().lower() != "canaan":
                        continue
                    if str(item.get("ip") or "").strip() == normalized_ip:
                        matched = item
                        break

            if matched is None:
                candidate_raw = {
                    "name": device_name or normalized_ip or normalized_mac,
                    "deviceType": "canaan",
                    "ip": normalized_ip,
                    "mac": normalized_mac or None,
                }
                candidate = self._normalize_fleet_inventory_item(candidate_raw)
                if candidate is not None:
                    self.fleet_inventory.append(candidate)
                    changed = True
            else:
                if normalized_ip and str(matched.get("ip") or "").strip() != normalized_ip:
                    matched["ip"] = normalized_ip
                    changed = True
                if normalized_mac and normalize_mac(matched.get("mac")) != normalized_mac:
                    matched["mac"] = normalized_mac
                    changed = True
                if device_name and str(matched.get("name") or "").strip() != device_name:
                    matched["name"] = device_name
                    changed = True
                if str(matched.get("deviceType") or "").strip().lower() != "canaan":
                    matched["deviceType"] = "canaan"
                    changed = True
                matched["lastSeenAtIso"] = now_iso()

            if changed:
                for item in self.fleet_inventory:
                    item_mac = normalize_mac(item.get("mac"))
                    if item_mac:
                        item["id"] = item_mac
                    else:
                        item_ip = str(item.get("ip") or "").strip()
                        item["id"] = item_ip.lower() if item_ip else str(item.get("id") or "")
        finally:
            self._fleet_lock.release()
        return changed

    def get_canaan_fan_pid_status(self) -> Dict[str, Any]:
        with self._canaan_pid_lock:
            programs = [dict(item) for item in self.canaan_fan_pid_programs]
        enabled = [item for item in programs if item.get("enabled", True)]
        enabled.sort(key=lambda item: str(item.get("deviceName") or item.get("targetIp") or item.get("id") or "").lower())
        return {
            "enabledCount": len(enabled),
            "totalCount": len(programs),
            "programs": enabled,
            "updatedAtIso": now_iso(),
        }

    def start_canaan_fan_pid_program(self, payload: Any) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {"ok": False, "error": "Payload must be an object."}

        normalized = self._normalize_canaan_fan_pid_program(payload)
        if normalized is None:
            return {"ok": False, "error": "targetIp or targetMac is required."}
        normalized["enabled"] = True
        normalized["updatedAtIso"] = now_iso()

        changed_inventory = self._upsert_inventory_for_canaan_pid(
            device_name=str(normalized.get("deviceName") or ""),
            target_ip=str(normalized.get("targetIp") or ""),
            target_mac=str(normalized.get("targetMac") or ""),
        )

        with self._canaan_pid_lock:
            idx = self._find_canaan_fan_pid_program_index(
                program_id=str(payload.get("id") or normalized.get("id") or ""),
                target_mac=str(normalized.get("targetMac") or ""),
                target_ip=str(normalized.get("targetIp") or ""),
            )
            if idx >= 0:
                existing = self.canaan_fan_pid_programs[idx]
                normalized["createdAtIso"] = str(existing.get("createdAtIso") or normalized.get("createdAtIso") or now_iso())
                for field in (
                    "lastRunAtIso",
                    "lastCommandAtIso",
                    "lastResolvedAtIso",
                    "lastIpScanAtIso",
                    "lastIpScanEpoch",
                    "lastChipTempC",
                    "lastObservedFanPercent",
                    "lastFanPercent",
                    "lastControlErrorC",
                    "lastCommandStatus",
                    "lastError",
                    "autoTuneEnabled",
                    "autoTuneIntervalSec",
                    "autoTuneLastAction",
                    "autoTuneLastAtIso",
                    "deadbandC",
                    "maxStepUpPercent",
                    "maxStepDownPercent",
                    "modeChangeResetFanPercent",
                ):
                    if field not in payload and field in existing:
                        normalized[field] = existing.get(field)
                self.canaan_fan_pid_programs[idx] = normalized
                verb = "Updated"
            else:
                self.canaan_fan_pid_programs.append(normalized)
                verb = "Started"

        with self.config_lock:
            self._persist_runtime_config()

        program_id = str(normalized.get("id") or "")
        if program_id:
            with self._canaan_pid_lock:
                existing_runtime = self._canaan_pid_runtime.get(program_id) or {}
                seed_fan = parse_int(
                    normalized.get("lastFanPercent"),
                    default=parse_int(existing_runtime.get("lastCommandFanPercent"), default=0, minimum=0, maximum=100),
                    minimum=0,
                    maximum=100,
                )
                # Reset PID accumulators on start/update so old integral state does not pin fan speeds.
                self._canaan_pid_runtime[program_id] = {
                    "integral": 0.0,
                    "lastError": 0.0,
                    "lastSampleAtEpoch": time.time(),
                    "lastCommandFanPercent": seed_fan if seed_fan > 0 else None,
                    "lastTempC": (
                        to_float(normalized.get("lastChipTempC"))
                        or to_float(existing_runtime.get("lastTempC"))
                        or to_float(normalized.get("targetTempC"))
                        or 70.0
                    ),
                    "tempSlopeCPerSecEma": 0.0,
                    "derivativeEma": 0.0,
                    "coolingBiasFan": float(
                        seed_fan if seed_fan > 0 else parse_int(normalized.get("minFanPercent"), default=CANAAN_FAN_PID_DEFAULT_MIN_FAN)
                    ),
                    "autoTuneLastEpoch": time.time(),
                    "autoTuneSamples": 0,
                    "autoTuneOvershootHits": 0,
                    "autoTuneHotHits": 0,
                    "autoTuneColdHits": 0,
                    "autoTuneApproachLagHits": 0,
                    "autoTuneOscillationHits": 0,
                    "lastWorkMode": parse_int(existing_runtime.get("lastWorkMode"), default=-1, minimum=-1, maximum=2),
                }

        target_label = str(normalized.get("deviceName") or normalized.get("targetIp") or normalized.get("targetMac") or program_id)
        self._log("canaan-pid", f"{verb} fan PID for {target_label}.", level="info")
        message = f"{verb} fan PID program for {target_label}."
        if changed_inventory:
            message += " Inventory mapping updated."
        return {
            "ok": True,
            "message": message,
            "program": normalized,
            "canaanFanPID": self.get_canaan_fan_pid_status(),
        }

    def stop_canaan_fan_pid_program(self, payload: Any) -> Dict[str, Any]:
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            return {"ok": False, "error": "Payload must be an object."}

        target_id = str(payload.get("id") or "").strip()
        target_mac = normalize_mac(payload.get("targetMac"))
        target_ip = str(payload.get("targetIp") or "").strip()

        with self._canaan_pid_lock:
            idx = self._find_canaan_fan_pid_program_index(program_id=target_id, target_mac=target_mac, target_ip=target_ip)
            if idx < 0:
                return {"ok": False, "error": "Fan PID program not found."}
            program = self.canaan_fan_pid_programs[idx]
            previous_enabled = bool(program.get("enabled", True))
            program["enabled"] = False
            program["updatedAtIso"] = now_iso()

        auto_result = self._restore_canaan_auto_fan(program, preferred_target_ip=target_ip)
        if not auto_result.get("ok"):
            with self._canaan_pid_lock:
                idx_retry = self._find_canaan_fan_pid_program_index(
                    program_id=str(program.get("id") or ""),
                    target_mac=str(program.get("targetMac") or ""),
                    target_ip=str(program.get("targetIp") or ""),
                )
                if idx_retry >= 0:
                    self.canaan_fan_pid_programs[idx_retry]["enabled"] = previous_enabled
                    self.canaan_fan_pid_programs[idx_retry]["updatedAtIso"] = now_iso()
            return {
                "ok": False,
                "error": f"Could not stop PID because auto fan restore failed: {auto_result.get('error')}",
                "program": dict(program),
                "autoFanRestore": auto_result,
            }

        with self._canaan_pid_lock:
            idx = self._find_canaan_fan_pid_program_index(
                program_id=str(program.get("id") or ""),
                target_mac=str(program.get("targetMac") or ""),
                target_ip=str(program.get("targetIp") or ""),
            )
            if idx < 0:
                removed = dict(program)
            else:
                removed = self.canaan_fan_pid_programs.pop(idx)
            removed_id = str(removed.get("id") or "")
            if removed_id in self._canaan_pid_runtime:
                self._canaan_pid_runtime.pop(removed_id, None)

        with self.config_lock:
            self._persist_runtime_config()

        target_label = str(removed.get("deviceName") or removed.get("targetIp") or removed.get("targetMac") or removed.get("id") or "device")
        self._log("canaan-pid", f"Stopped fan PID for {target_label}.", level="info")
        return {
            "ok": True,
            "message": f"Stopped fan PID program for {target_label}.",
            "program": removed,
            "autoFanRestore": auto_result,
            "canaanFanPID": self.get_canaan_fan_pid_status(),
        }

    def _extract_canaan_stats_mm_id0(self, data: Any) -> str:
        if not isinstance(data, dict):
            return ""
        entries = data.get("STATS")
        if not isinstance(entries, list):
            entries = []
        candidate_dicts: List[Dict[str, Any]] = [item for item in entries if isinstance(item, dict)]
        candidate_dicts.append(data)
        for item in candidate_dicts:
            direct = item.get("MM ID0:Summary")
            if isinstance(direct, str) and direct.strip():
                return direct.strip()
            direct = item.get("MM ID0")
            if isinstance(direct, str) and direct.strip():
                return direct.strip()
        return ""

    def _extract_canaan_stat_string_value(self, mm_id0: str, key: str) -> Optional[str]:
        if not mm_id0 or not key:
            return None
        match = re.search(rf"{re.escape(key)}\[([^\]]+)\]", mm_id0)
        if not match:
            return None
        return match.group(1).strip()

    def _extract_canaan_stat_int_value(self, mm_id0: str, key: str) -> Optional[int]:
        raw = self._extract_canaan_stat_string_value(mm_id0, key)
        if raw is None:
            return None
        # Prefer the first integer token so values like "49%" or "49 50"
        # parse consistently without concatenating multiple numbers together.
        match = re.search(r"-?\d+", raw)
        if not match:
            return None
        try:
            return int(match.group(0))
        except Exception:
            return None

    def _extract_canaan_stat_float_value(self, mm_id0: str, key: str) -> Optional[float]:
        raw = self._extract_canaan_stat_string_value(mm_id0, key)
        if raw is None:
            return None
        match = re.search(r"-?\d+(?:\.\d+)?", raw)
        if not match:
            return None
        try:
            return float(match.group(0))
        except Exception:
            return None

    def _extract_canaan_stat_array(self, mm_id0: str, key: str) -> List[float]:
        raw = self._extract_canaan_stat_string_value(mm_id0, key)
        if raw is None:
            return []
        values: List[float] = []
        for part in re.findall(r"-?\d+(?:\.\d+)?", raw):
            try:
                values.append(float(part))
            except Exception:
                continue
        return values

    def _read_canaan_pid_telemetry(self, target_ip: str) -> Dict[str, Any]:
        data: Any = None
        mm_id0 = ""
        attempts: List[str] = []

        # Try estats first for richer/per-chip fields, then fall back to stats.
        for command in ("estats", "stats"):
            result = self._send_cgminer_json_command(target_ip, command)
            if not result.get("ok"):
                attempts.append(f"{command}: {result.get('error') or 'request failed'}")
                continue
            candidate_data = result.get("data")
            candidate_mm_id0 = self._extract_canaan_stats_mm_id0(candidate_data)
            if candidate_mm_id0:
                data = candidate_data
                mm_id0 = candidate_mm_id0
                break
            attempts.append(f"{command}: MM ID0 payload missing")

        if not mm_id0:
            error_detail = "; ".join(attempts) if attempts else "No telemetry attempts recorded."
            return {"ok": False, "error": f"Could not read usable Canaan telemetry ({error_detail})."}

        chip_temp_candidates = [value for value in self._extract_canaan_stat_array(mm_id0, "PVT_T0") if value > 0]
        tmax = self._extract_canaan_stat_float_value(mm_id0, "TMax")
        tavg = self._extract_canaan_stat_float_value(mm_id0, "TAvg")
        hottest_chip_temp = max(chip_temp_candidates) if chip_temp_candidates else (tmax or tavg or 0.0)
        # Control should track the same displayed average chip temperature when available.
        chip_temp = tavg if tavg is not None and tavg > 0 else hottest_chip_temp

        fan_percent = self._extract_canaan_stat_int_value(mm_id0, "FanR")
        if fan_percent is None:
            fan_percent = 0
        fan_percent = max(0, min(100, fan_percent))
        work_mode = self._extract_canaan_stat_int_value(mm_id0, "WORKMODE")
        work_level = self._extract_canaan_stat_int_value(mm_id0, "WORKLEVEL")

        return {
            "ok": True,
            "chipTempC": float(chip_temp),
            "hottestChipTempC": float(hottest_chip_temp),
            "fanPercent": int(fan_percent),
            "avgTempC": float(tavg) if tavg is not None else None,
            "maxTempC": float(tmax) if tmax is not None else None,
            "workMode": work_mode,
            "workLevel": work_level,
            "rawMMID0": mm_id0[:6000],
        }

    def _apply_canaan_fan_speed(self, target_ip: str, fan_percent: int) -> Dict[str, Any]:
        speed = parse_int(
            fan_percent,
            default=CANAAN_FAN_PID_DEFAULT_MIN_FAN,
            minimum=CANAAN_FAN_PID_DEFAULT_MIN_FAN,
            maximum=CANAAN_FAN_PID_DEFAULT_MAX_FAN,
        )
        command = f"ascset|0,fan-spd,{speed}"
        result = self._send_cgminer_raw_command(target_ip, command)
        if result.get("ok"):
            result["message"] = f"Applied fan speed {speed}%."
            result["fanPercent"] = speed
            return result
        return {
            "ok": False,
            "error": result.get("error") or "Canaan fan speed command failed.",
            "fanPercent": speed,
            "raw": result.get("raw"),
        }

    def _restore_canaan_auto_fan(self, program: Dict[str, Any], preferred_target_ip: str = "") -> Dict[str, Any]:
        preferred_ip = str(preferred_target_ip or "").strip()
        configured_ip = str(program.get("targetIp") or "").strip()

        resolved_fast = self._resolve_canaan_pid_target_ip(program, force_scan=False)
        resolved_scan = self._resolve_canaan_pid_target_ip(program, force_scan=True)

        candidate_ips: List[str] = []
        for candidate in [
            preferred_ip,
            str(resolved_fast.get("ip") or "").strip(),
            str(resolved_scan.get("ip") or "").strip(),
            configured_ip,
        ]:
            if candidate and candidate not in candidate_ips:
                candidate_ips.append(candidate)

        if not candidate_ips:
            return {"ok": False, "error": "No target IP available to restore auto fan mode."}

        attempts_log: List[Dict[str, Any]] = []
        for candidate_ip in candidate_ips:
            # Retry each candidate a couple times to tolerate transient socket hiccups.
            for attempt in range(1, 3):
                result = self._send_cgminer_raw_command(candidate_ip, "ascset|0,fan-spd,-1")
                attempt_entry = {
                    "targetIp": candidate_ip,
                    "attempt": attempt,
                    "ok": bool(result.get("ok")),
                    "error": result.get("error"),
                }
                attempts_log.append(attempt_entry)
                if result.get("ok"):
                    return {
                        "ok": True,
                        "message": "Restored miner auto fan mode.",
                        "targetIp": candidate_ip,
                        "raw": result.get("raw"),
                        "attempts": attempts_log,
                    }
                time.sleep(0.2)

        last_error = ""
        if attempts_log:
            last_error = str(attempts_log[-1].get("error") or "")
        return {
            "ok": False,
            "error": last_error or "Failed to send auto fan command.",
            "targetIp": candidate_ips[0],
            "attempts": attempts_log,
        }

    def _lookup_ip_by_mac_from_neighbors(self, target_mac: str) -> Optional[str]:
        normalized_target = normalize_mac(target_mac)
        if not normalized_target:
            return None

        try:
            with open("/proc/net/arp", "r", encoding="utf-8", errors="ignore") as f:
                for line in f.readlines()[1:]:
                    fields = line.split()
                    if len(fields) < 4:
                        continue
                    ip_addr = fields[0].strip()
                    mac_addr = normalize_mac(fields[3].strip())
                    if mac_addr == normalized_target:
                        return ip_addr
        except Exception:
            pass

        try:
            result = subprocess.run(["ip", "neigh", "show"], capture_output=True, text=True, timeout=4)
            if result.returncode == 0 and result.stdout:
                for raw_line in result.stdout.splitlines():
                    line = raw_line.strip()
                    if not line:
                        continue
                    match = re.search(r"(?P<ip>\d+\.\d+\.\d+\.\d+).+lladdr\s+(?P<mac>[0-9a-f:]{17})", line, re.IGNORECASE)
                    if not match:
                        continue
                    mac_addr = normalize_mac(match.group("mac"))
                    if mac_addr == normalized_target:
                        return match.group("ip")
        except Exception:
            pass
        return None

    def _scan_subnet_for_canaan_mac(self, target_mac: str) -> Optional[Dict[str, Any]]:
        normalized_target = normalize_mac(target_mac)
        if not normalized_target:
            return None
        local_ip = self._get_local_ip()
        if not local_ip:
            return None
        octets = local_ip.split(".")
        if len(octets) != 4:
            return None

        try:
            network = ipaddress.ip_network(f"{octets[0]}.{octets[1]}.{octets[2]}.0/24", strict=False)
        except Exception:
            return None

        hosts = [str(ip) for ip in network.hosts()]
        if len(hosts) > 512:
            hosts = hosts[:512]

        found: Optional[Dict[str, Any]] = None
        with ThreadPoolExecutor(max_workers=32) as executor:
            future_map = {
                executor.submit(self._probe_canaan_capabilities, host, 0.7): host
                for host in hosts
            }
            for future in as_completed(future_map):
                host = future_map[future]
                try:
                    probe = future.result()
                except Exception:
                    continue
                if not isinstance(probe, dict) or not probe.get("ok"):
                    continue
                probe_mac = normalize_mac(probe.get("mac"))
                if probe_mac != normalized_target:
                    continue
                found = {
                    "ip": host,
                    "mac": probe_mac,
                    "model": probe.get("model"),
                    "deviceSubtype": probe.get("deviceSubtype"),
                }
                break
        return found

    def _resolve_canaan_pid_target_ip(self, program: Dict[str, Any], force_scan: bool = False) -> Dict[str, Any]:
        changed = False
        resolved_ip = str(program.get("targetIp") or "").strip()
        normalized_mac = normalize_mac(program.get("targetMac"))
        source = "program"

        if normalized_mac:
            with self._fleet_lock:
                inventory_match = next(
                    (item for item in self.fleet_inventory if normalize_mac(item.get("mac")) == normalized_mac),
                    None,
                )
            if inventory_match is not None:
                inventory_ip = str(inventory_match.get("ip") or "").strip()
                if inventory_ip and inventory_ip != resolved_ip:
                    resolved_ip = inventory_ip
                    source = "inventory"
                    changed = True

            neighbor_ip = self._lookup_ip_by_mac_from_neighbors(normalized_mac)
            if neighbor_ip and neighbor_ip != resolved_ip:
                resolved_ip = neighbor_ip
                source = "neighbor"
                changed = True

            if force_scan:
                now_epoch = time.time()
                last_scan_epoch = to_float(program.get("lastIpScanEpoch")) or 0.0
                if (now_epoch - last_scan_epoch) >= CANAAN_FAN_PID_SCAN_COOLDOWN_SECONDS:
                    program["lastIpScanEpoch"] = now_epoch
                    program["lastIpScanAtIso"] = now_iso()
                    scan_result = self._scan_subnet_for_canaan_mac(normalized_mac)
                    if scan_result and scan_result.get("ip"):
                        scanned_ip = str(scan_result.get("ip") or "").strip()
                        if scanned_ip and scanned_ip != resolved_ip:
                            resolved_ip = scanned_ip
                            source = "scan"
                            changed = True
                            if scan_result.get("model") and not str(program.get("deviceName") or "").strip():
                                program["deviceName"] = str(scan_result.get("model"))

        if normalized_mac and program.get("targetMac") != normalized_mac:
            program["targetMac"] = normalized_mac
            changed = True
        if resolved_ip and resolved_ip != str(program.get("targetIp") or "").strip():
            program["targetIp"] = resolved_ip
            program["lastResolvedAtIso"] = now_iso()
            changed = True

        if changed and resolved_ip:
            self._upsert_inventory_for_canaan_pid(
                device_name=str(program.get("deviceName") or ""),
                target_ip=resolved_ip,
                target_mac=normalized_mac,
            )

        return {
            "ip": resolved_ip,
            "mac": normalized_mac or None,
            "changed": changed,
            "source": source,
        }

    def _auto_tune_canaan_pid_program(
        self,
        program: Dict[str, Any],
        runtime: Dict[str, Any],
        *,
        error_c: float,
        previous_error: float,
        temp_slope_ema: float,
        target_fan: int,
        base_fan: int,
        observed_fan: int,
    ) -> bool:
        if not parse_bool(program.get("autoTuneEnabled"), default=CANAAN_FAN_PID_AUTOTUNE_DEFAULT_ENABLED):
            return False

        now_epoch = time.time()
        interval_sec = parse_int(
            program.get("autoTuneIntervalSec"),
            default=CANAAN_FAN_PID_AUTOTUNE_INTERVAL_SECONDS,
            minimum=5,
            maximum=300,
        )
        deadband_c = to_float(program.get("deadbandC"))
        if deadband_c is None:
            deadband_c = CANAAN_FAN_PID_DEFAULT_DEADBAND_C
        deadband_c = max(0.0, min(5.0, deadband_c))

        samples = parse_int(runtime.get("autoTuneSamples"), default=0, minimum=0, maximum=100_000) + 1
        runtime["autoTuneSamples"] = samples

        overshoot_hits = parse_int(runtime.get("autoTuneOvershootHits"), default=0, minimum=0, maximum=100_000)
        if error_c > (deadband_c + 0.6):
            overshoot_hits += 1
        runtime["autoTuneOvershootHits"] = overshoot_hits

        hot_hits = parse_int(runtime.get("autoTuneHotHits"), default=0, minimum=0, maximum=100_000)
        if error_c > (deadband_c + 0.2):
            hot_hits += 1
        runtime["autoTuneHotHits"] = hot_hits

        cold_hits = parse_int(runtime.get("autoTuneColdHits"), default=0, minimum=0, maximum=100_000)
        if error_c < -(deadband_c + 0.8):
            cold_hits += 1
        runtime["autoTuneColdHits"] = cold_hits

        approach_lag_hits = parse_int(runtime.get("autoTuneApproachLagHits"), default=0, minimum=0, maximum=100_000)
        # Detect "late reaction": temp rising toward setpoint but fan command is barely increasing.
        if -4.0 <= error_c <= -0.3 and temp_slope_ema > 0.008 and (target_fan - base_fan) <= 2:
            approach_lag_hits += 1
        runtime["autoTuneApproachLagHits"] = approach_lag_hits

        oscillation_hits = parse_int(runtime.get("autoTuneOscillationHits"), default=0, minimum=0, maximum=100_000)
        if previous_error * error_c < 0 and abs(previous_error) > (deadband_c + 0.5) and abs(error_c) > (deadband_c + 0.5):
            oscillation_hits += 1
        runtime["autoTuneOscillationHits"] = oscillation_hits

        last_tune_epoch = to_float(runtime.get("autoTuneLastEpoch")) or now_epoch
        elapsed = now_epoch - last_tune_epoch
        min_samples = max(5, int(interval_sec / max(1, self.canaan_pid_loop_seconds)))
        if elapsed < interval_sec or samples < min_samples:
            return False

        kp = to_float(program.get("kp"))
        if kp is None:
            kp = CANAAN_FAN_PID_DEFAULT_KP
        ki = to_float(program.get("ki"))
        if ki is None:
            ki = CANAAN_FAN_PID_DEFAULT_KI
        kd = to_float(program.get("kd"))
        if kd is None:
            kd = CANAAN_FAN_PID_DEFAULT_KD

        old_kp, old_ki, old_kd, old_deadband = kp, ki, kd, deadband_c
        reason = ""
        changed = False

        # Priority 1: fix "fan waits until setpoint" by increasing predictive action.
        if approach_lag_hits >= max(2, int(samples * 0.08)) and overshoot_hits <= max(2, int(samples * 0.06)):
            kd = min(8.0, kd + 0.25)
            kp = min(12.0, kp + 0.12)
            deadband_c = max(0.05, deadband_c - 0.08)
            reason = "Improved preemptive ramp-up before setpoint."
            changed = True
        # Priority 2: damp oscillation around setpoint.
        elif oscillation_hits >= max(2, int(samples * 0.08)) or (
            overshoot_hits >= max(2, int(samples * 0.08)) and cold_hits >= max(2, int(samples * 0.08))
        ):
            kp = max(0.15, kp * 0.90)
            ki = max(0.0, ki * 0.78)
            kd = min(8.0, (kd * 1.12) + 0.08)
            deadband_c = min(2.0, deadband_c + 0.05)
            reason = "Damped oscillation and overshoot."
            changed = True
        # Priority 3: sustained above target without oscillation.
        elif hot_hits >= max(3, int(samples * 0.15)):
            kp = min(12.0, kp + 0.15)
            ki = min(2.0, ki + 0.02)
            kd = min(8.0, kd + 0.06)
            reason = "Raised gain for sustained above-target temperature."
            changed = True
        # Priority 4: sustained below target with fan still high.
        elif cold_hits >= max(3, int(samples * 0.15)) and observed_fan > (parse_int(program.get("minFanPercent"), default=20) + 6):
            ki = max(0.0, ki * 0.72)
            deadband_c = min(2.0, deadband_c + 0.05)
            reason = "Reduced integral to avoid over-cooling."
            changed = True

        runtime["autoTuneLastEpoch"] = now_epoch
        runtime["autoTuneSamples"] = 0
        runtime["autoTuneOvershootHits"] = 0
        runtime["autoTuneHotHits"] = 0
        runtime["autoTuneColdHits"] = 0
        runtime["autoTuneApproachLagHits"] = 0
        runtime["autoTuneOscillationHits"] = 0

        if not changed:
            return False

        program["kp"] = round(max(0.0, min(12.0, kp)), 4)
        program["ki"] = round(max(0.0, min(2.0, ki)), 4)
        program["kd"] = round(max(0.0, min(8.0, kd)), 4)
        program["deadbandC"] = round(max(0.0, min(5.0, deadband_c)), 3)
        program["updatedAtIso"] = now_iso()
        program["autoTuneLastAtIso"] = now_iso()
        program["autoTuneLastAction"] = reason

        target_label = str(program.get("deviceName") or program.get("targetIp") or program.get("targetMac") or "canaan")
        self._log(
            "canaan-pid",
            (
                f"Auto-tuned {target_label}: {reason} "
                f"(kp {old_kp:.3f}->{program['kp']:.3f}, "
                f"ki {old_ki:.3f}->{program['ki']:.3f}, "
                f"kd {old_kd:.3f}->{program['kd']:.3f}, "
                f"deadband {old_deadband:.3f}->{program['deadbandC']:.3f})."
            ),
            level="info",
        )
        return True

    def _run_single_canaan_fan_pid_program(self, program: Dict[str, Any]) -> bool:
        program_id = str(program.get("id") or "").strip()
        if not program_id:
            return False

        persist_needed = False
        resolved = self._resolve_canaan_pid_target_ip(program, force_scan=False)
        persist_needed = persist_needed or bool(resolved.get("changed"))
        target_ip = str(resolved.get("ip") or "").strip()
        target_mac = normalize_mac(resolved.get("mac"))

        if not target_ip:
            program["lastError"] = "No target IP available for PID control."
            program["lastRunAtIso"] = now_iso()
            return persist_needed

        telemetry = self._read_canaan_pid_telemetry(target_ip)
        if not telemetry.get("ok") and target_mac:
            retry_resolved = self._resolve_canaan_pid_target_ip(program, force_scan=True)
            persist_needed = persist_needed or bool(retry_resolved.get("changed"))
            retry_ip = str(retry_resolved.get("ip") or "").strip()
            if retry_ip and retry_ip != target_ip:
                target_ip = retry_ip
                telemetry = self._read_canaan_pid_telemetry(target_ip)

        if not telemetry.get("ok"):
            program["lastError"] = str(telemetry.get("error") or "Failed to read Canaan telemetry.")
            program["lastRunAtIso"] = now_iso()
            program["lastCommandStatus"] = None
            return persist_needed

        chip_temp = to_float(telemetry.get("chipTempC")) or 0.0
        observed_fan = parse_int(telemetry.get("fanPercent"), default=0, minimum=0, maximum=100)
        target_temp = to_float(program.get("targetTempC")) or 70.0
        min_fan = parse_int(program.get("minFanPercent"), default=CANAAN_FAN_PID_DEFAULT_MIN_FAN, minimum=20, maximum=100)
        max_fan = parse_int(program.get("maxFanPercent"), default=CANAAN_FAN_PID_DEFAULT_MAX_FAN, minimum=min_fan, maximum=100)
        if min_fan > max_fan:
            min_fan, max_fan = max_fan, min_fan

        kp = to_float(program.get("kp"))
        if kp is None:
            kp = CANAAN_FAN_PID_DEFAULT_KP
        ki = to_float(program.get("ki"))
        if ki is None:
            ki = CANAAN_FAN_PID_DEFAULT_KI
        kd = to_float(program.get("kd"))
        if kd is None:
            kd = CANAAN_FAN_PID_DEFAULT_KD
        deadband_c = to_float(program.get("deadbandC"))
        if deadband_c is None:
            deadband_c = CANAAN_FAN_PID_DEFAULT_DEADBAND_C
        deadband_c = max(0.0, min(5.0, deadband_c))
        max_step_up = parse_int(
            program.get("maxStepUpPercent"),
            default=CANAAN_FAN_PID_DEFAULT_MAX_STEP_UP_PERCENT,
            minimum=1,
            maximum=40,
        )
        max_step_down = parse_int(
            program.get("maxStepDownPercent"),
            default=CANAAN_FAN_PID_DEFAULT_MAX_STEP_DOWN_PERCENT,
            minimum=1,
            maximum=40,
        )

        runtime = self._canaan_pid_runtime.get(program_id)
        now_epoch = time.time()
        if runtime is None:
            runtime = {
                "integral": 0.0,
                "lastError": 0.0,
                "lastSampleAtEpoch": now_epoch,
                "lastCommandFanPercent": None,
                "lastTempC": chip_temp,
                "tempSlopeCPerSecEma": 0.0,
                "derivativeEma": 0.0,
                "coolingBiasFan": float(min_fan),
                "autoTuneLastEpoch": now_epoch,
                "autoTuneSamples": 0,
                "autoTuneOvershootHits": 0,
                "autoTuneHotHits": 0,
                "autoTuneColdHits": 0,
                "autoTuneApproachLagHits": 0,
                "autoTuneOscillationHits": 0,
                "lastWorkMode": -1,
            }
            self._canaan_pid_runtime[program_id] = runtime

        current_work_mode = parse_int(telemetry.get("workMode"), default=-1, minimum=-1, maximum=2)
        if current_work_mode < 0:
            current_work_mode = parse_int(telemetry.get("workLevel"), default=-1, minimum=-1, maximum=2)
        previous_work_mode = parse_int(runtime.get("lastWorkMode"), default=-1, minimum=-1, maximum=2)
        mode_change_reset_fan = parse_int(
            program.get("modeChangeResetFanPercent"),
            default=CANAAN_FAN_PID_DEFAULT_MODE_CHANGE_RESET_FAN_PERCENT,
            minimum=min_fan,
            maximum=max_fan,
        )
        if previous_work_mode >= 0 and current_work_mode >= 0 and current_work_mode != previous_work_mode:
            reset_result = self._apply_canaan_fan_speed(target_ip, mode_change_reset_fan)
            if reset_result.get("ok"):
                runtime["lastCommandFanPercent"] = mode_change_reset_fan
                runtime["integral"] = 0.0
                runtime["derivativeEma"] = 0.0
                runtime["coolingBiasFan"] = float(mode_change_reset_fan)
                observed_fan = mode_change_reset_fan
                program["lastFanPercent"] = mode_change_reset_fan
                program["lastCommandAtIso"] = now_iso()
                self._log(
                    "canaan-pid",
                    f"Detected work mode change {previous_work_mode}->{current_work_mode}; reset fan to {mode_change_reset_fan}% before PID ramp.",
                    level="info",
                )
            else:
                self._log(
                    "canaan-pid",
                    (
                        f"Work mode changed {previous_work_mode}->{current_work_mode}, "
                        f"but reset fan command failed: {reset_result.get('error') or 'unknown error'}"
                    ),
                    level="warn",
                )

        previous_sample_epoch = to_float(runtime.get("lastSampleAtEpoch")) or now_epoch
        dt_seconds = max(1.0, min(120.0, now_epoch - previous_sample_epoch))
        error_c = chip_temp - target_temp
        last_commanded_fan = parse_int(runtime.get("lastCommandFanPercent"), default=0, minimum=0, maximum=100)
        base_fan = last_commanded_fan if last_commanded_fan > 0 else observed_fan
        if base_fan <= 0:
            base_fan = min_fan

        previous_temp = to_float(runtime.get("lastTempC"))
        temp_slope = 0.0
        if previous_temp is not None:
            temp_slope = (chip_temp - previous_temp) / dt_seconds
        prev_temp_slope_ema = to_float(runtime.get("tempSlopeCPerSecEma")) or 0.0
        temp_slope_ema = (0.45 * prev_temp_slope_ema) + (0.55 * temp_slope)

        previous_error = to_float(runtime.get("lastError")) or 0.0
        derivative = (error_c - previous_error) / dt_seconds
        prev_derivative_ema = to_float(runtime.get("derivativeEma")) or 0.0
        derivative_ema = (0.35 * prev_derivative_ema) + (0.65 * derivative)

        previous_integral = to_float(runtime.get("integral")) or 0.0
        if error_c > deadband_c:
            error_gain = 1.20 if error_c > (deadband_c + 1.0) else 0.45
            integral = previous_integral + (error_c * dt_seconds * error_gain)
        elif error_c < -deadband_c:
            # Bleed integral aggressively when below setpoint to prevent fan pinning.
            integral = previous_integral + (error_c * dt_seconds * 0.45)
            if integral > 0.0 and error_c < -0.8:
                integral *= 0.55
        else:
            # In deadband, decay integral so the controller can settle.
            integral = previous_integral * 0.72
        integral = max(-300.0, min(300.0, integral))

        cooling_bias = to_float(runtime.get("coolingBiasFan"))
        if cooling_bias is None:
            cooling_bias = float(base_fan if base_fan > 0 else min_fan)
        if chip_temp >= (target_temp - deadband_c):
            # Track required fan while around/above setpoint.
            cooling_bias = (0.75 * cooling_bias) + (0.25 * float(base_fan))
        else:
            # Decay slowly to minimum when comfortably under setpoint.
            cooling_bias = (0.92 * cooling_bias) + (0.08 * float(min_fan))

        dynamic_min_fan = min_fan
        if chip_temp >= (target_temp - 0.4):
            dynamic_min_fan = max(dynamic_min_fan, int(round(cooling_bias * 0.96)))
        elif chip_temp >= (target_temp - 1.2):
            dynamic_min_fan = max(dynamic_min_fan, int(round(cooling_bias * 0.88)))
        elif chip_temp >= (target_temp - 2.5):
            dynamic_min_fan = max(dynamic_min_fan, int(round(cooling_bias * 0.74)))
        dynamic_min_fan = max(min_fan, min(max_fan, dynamic_min_fan))

        raw_fan = float(base_fan) + (kp * error_c) + (ki * integral) + (kd * derivative_ema)
        target_fan = int(round(max(dynamic_min_fan, min(max_fan, raw_fan))))

        # Preemptive approach control: start adding fan before crossing setpoint.
        approach_gap_c = target_temp - chip_temp  # positive when below target
        approach_window_c = max(6.0, deadband_c + 3.0)
        if approach_gap_c > 0.0 and approach_gap_c <= approach_window_c:
            closeness = max(0.0, min(1.0, 1.0 - (approach_gap_c / approach_window_c)))
            predicted_temp_20s = chip_temp + (temp_slope_ema * 20.0)
            approaching = temp_slope_ema > -0.002
            likely_to_reach = (
                predicted_temp_20s >= (target_temp - max(0.5, deadband_c))
                or closeness >= 0.72
            )
            if approaching and likely_to_reach:
                slope_term = max(0.0, min(5.0, temp_slope_ema * 48.0))
                proactive_up = int(round((closeness * (max_step_up * 0.9)) + slope_term))
                proactive_up = max(1, min(max_step_up + 4, proactive_up))
                approach_floor = dynamic_min_fan + int(round(closeness * (6.0 + (max_step_up * 0.45))))
                target_fan = max(target_fan, base_fan + proactive_up, approach_floor)

        # Asymmetric reaction: cool quickly when above setpoint, unwind smoothly when below.
        if error_c > deadband_c:
            error_above_c = error_c - deadband_c
            boost_up = int(round(error_above_c * 4.2))
            if error_above_c > 0.8:
                boost_up = max(2, boost_up)
            else:
                boost_up = max(0, boost_up)
            if temp_slope_ema > 0.0:
                predictive_up = int(round(temp_slope_ema * 60.0 * 1.25))
                boost_up += max(0, min(max_step_up + 4, predictive_up))
            target_fan = max(target_fan, base_fan + boost_up)
        elif error_c < -deadband_c:
            drop_down = max(2, int(round((abs(error_c) - deadband_c) * 2.8)))
            if temp_slope_ema < -0.015:
                predictive_down = int(round(abs(temp_slope_ema) * 60.0 * 0.6))
                drop_down += max(0, min(4, predictive_down))
            target_fan = min(target_fan, base_fan - drop_down)

        # Rate limit each loop to avoid 100% -> 20% cliffs and thermal overshoot.
        up_limit = max_step_up + (4 if error_c > 1.0 else 2 if error_c > 0.5 else 0)
        down_limit = max_step_down
        if chip_temp >= (target_temp - 0.5):
            down_limit = min(down_limit, max(2, int(round(max_step_down * 0.6))))
        target_fan = min(target_fan, base_fan + up_limit)
        target_fan = max(target_fan, base_fan - down_limit)

        # Near target, avoid unnecessary 100% fan unless temperature is still rising quickly.
        if chip_temp >= (target_temp - 1.2) and error_c <= 1.2 and temp_slope_ema < 0.03:
            soft_cap = max(dynamic_min_fan + 18, int(round(base_fan + 16 + (max(0.0, error_c) * 7.0))))
            target_fan = min(target_fan, min(max_fan, soft_cap))

        # Anti-windup: near-target saturation should unwind quickly to avoid fan pinning.
        if target_fan >= (max_fan - 1) and error_c < 1.8:
            integral *= 0.72
        if error_c < 0.0 and integral > 0.0:
            integral *= 0.86

        target_fan = int(max(dynamic_min_fan, min(max_fan, target_fan)))

        should_send = (
            observed_fan <= 0
            or abs(target_fan - observed_fan) >= 1
            or abs(target_fan - last_commanded_fan) >= 1
        )

        command_status: Optional[str] = None
        command_error: Optional[str] = None
        if should_send:
            command_result = self._apply_canaan_fan_speed(target_ip, target_fan)
            if command_result.get("ok"):
                runtime["lastCommandFanPercent"] = target_fan
                program["lastFanPercent"] = target_fan
                program["lastCommandAtIso"] = now_iso()
                command_status = str(command_result.get("message") or f"Applied fan speed {target_fan}%.")
            else:
                command_error = str(command_result.get("error") or "Failed to apply fan speed.")
                command_status = command_error
        else:
            if runtime.get("lastCommandFanPercent") is not None:
                program["lastFanPercent"] = parse_int(
                    runtime.get("lastCommandFanPercent"),
                    default=observed_fan,
                    minimum=0,
                    maximum=100,
                )
            else:
                program["lastFanPercent"] = observed_fan
            command_status = "No fan change required."

        runtime["integral"] = integral
        runtime["lastError"] = error_c
        runtime["lastSampleAtEpoch"] = now_epoch
        runtime["lastTempC"] = chip_temp
        runtime["tempSlopeCPerSecEma"] = temp_slope_ema
        runtime["derivativeEma"] = derivative_ema
        runtime["coolingBiasFan"] = float(cooling_bias)
        runtime["lastWorkMode"] = current_work_mode

        program["lastObservedFanPercent"] = observed_fan
        program["lastChipTempC"] = round(chip_temp, 2)
        program["lastControlErrorC"] = round(error_c, 2)
        program["lastRunAtIso"] = now_iso()
        program["updatedAtIso"] = now_iso()
        program["lastCommandStatus"] = command_status
        if command_error:
            program["lastError"] = command_error
        else:
            program["lastError"] = None

        if self._auto_tune_canaan_pid_program(
            program,
            runtime,
            error_c=error_c,
            previous_error=previous_error,
            temp_slope_ema=temp_slope_ema,
            target_fan=target_fan,
            base_fan=base_fan,
            observed_fan=observed_fan,
        ):
            persist_needed = True

        return persist_needed

    def _run_canaan_fan_pid_if_needed(self) -> None:
        with self._canaan_pid_lock:
            active_programs = [item for item in self.canaan_fan_pid_programs if item.get("enabled", True)]
        if not active_programs:
            return

        persist_needed = False
        for program in active_programs:
            try:
                if self._run_single_canaan_fan_pid_program(program):
                    persist_needed = True
            except Exception as exc:  # pylint: disable=broad-except
                program["lastError"] = str(exc)
                program["lastRunAtIso"] = now_iso()
                self._log("canaan-pid", f"PID loop error for {program.get('id', 'unknown')}: {exc}", level="warn")

        if persist_needed:
            with self.config_lock:
                self._persist_runtime_config()

    def _schedule_includes_weekday(self, days_mask: int, weekday_index: int) -> bool:
        return (days_mask & (1 << weekday_index)) != 0

    def _apply_bitaxe_tune(self, target_ip: str, tune_id: str, target_mac: str = "") -> Dict[str, Any]:
        normalized_mac = normalize_mac(target_mac)
        preset = None
        if normalized_mac:
            preset = self.bitaxe_tune_overrides_by_mac.get(normalized_mac, {}).get(tune_id)
        if not preset:
            preset = self.bitaxe_tune_presets.get(tune_id)
        if not preset:
            return {"ok": False, "error": f"Unknown BitAxe tune '{tune_id}'."}

        url = f"{self.bitaxe_scheme}://{target_ip}/api/system"
        payload = {
            "frequency": preset["frequency"],
            "coreVoltage": preset["coreVoltage"],
        }
        try:
            response = self.session.patch(url, json=payload, timeout=max(3.0, float(self.http_timeout_seconds)))
            if not response.ok:
                return {"ok": False, "error": f"HTTP {response.status_code}", "statusCode": response.status_code}
            return {
                "ok": True,
                "message": f"Applied {preset['label']} tune.",
                "appliedTuneId": tune_id,
                "targetMac": normalized_mac or None,
                "url": url,
                "payload": payload,
                "statusCode": response.status_code,
            }
        except Exception as exc:  # pylint: disable=broad-except
            return {"ok": False, "error": str(exc), "url": url}

    def _send_cgminer_raw_command(self, target_ip: str, command: str, port: int = 4028) -> Dict[str, Any]:
        sock = None
        try:
            sock = socket.create_connection((target_ip, port), timeout=max(2.0, float(self.http_timeout_seconds)))
            sock.settimeout(5)
            sock.sendall((command + "\n").encode("utf-8"))
            chunks: List[bytes] = []
            while True:
                try:
                    chunk = sock.recv(4096)
                except socket.timeout:
                    break
                if not chunk:
                    break
                chunks.append(chunk)
            raw_text = b"".join(chunks).decode("utf-8", errors="ignore").strip()
            code_match = re.search(r"Code=(\d+)", raw_text)
            code_value = int(code_match.group(1)) if code_match else None
            ok = code_value in (118, 119) if code_value is not None else ("STATUS=S" in raw_text)
            return {
                "ok": bool(ok),
                "code": code_value,
                "raw": raw_text[:6000],
            }
        except Exception as exc:  # pylint: disable=broad-except
            return {"ok": False, "error": str(exc)}
        finally:
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass

    def _apply_canaan_work_mode(self, target_ip: str, device_type: str, mode_value: str) -> Dict[str, Any]:
        mode_int = parse_int(mode_value, default=-1, minimum=0, maximum=2)
        if mode_int not in (0, 1, 2):
            return {"ok": False, "error": "Work mode must be 0, 1, or 2."}

        preferred_command = (
            f"ascset|0,worklevel,set,{mode_int}"
            if "nano3" in (device_type or "")
            else f"ascset|0,workmode,set,{mode_int}"
        )
        fallback_command = (
            f"ascset|0,workmode,set,{mode_int}"
            if preferred_command.endswith("worklevel,set," + str(mode_int))
            else f"ascset|0,worklevel,set,{mode_int}"
        )

        first = self._send_cgminer_raw_command(target_ip, preferred_command)
        if first.get("ok"):
            first["message"] = f"Applied Canaan work mode {mode_int}."
            return first

        second = self._send_cgminer_raw_command(target_ip, fallback_command)
        if second.get("ok"):
            second["message"] = f"Applied Canaan work mode {mode_int}."
            return second

        return {
            "ok": False,
            "error": second.get("error") or first.get("error") or "Canaan work mode command failed.",
            "firstAttempt": first,
            "secondAttempt": second,
        }

    def _canaan_mode_for_fleet_preset(self, preset_id: str, subtype: str) -> str:
        normalized_subtype = self._normalize_canaan_subtype(subtype)
        if normalized_subtype == "mini3":
            return "2" if preset_id == "eco" else "0"
        return "0" if preset_id == "eco" else "2"

    def _nested_value(self, source: Any, path: List[str]) -> Any:
        current = source
        for key in path:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        return current

    def _apply_braiins_fleet_preset(self, target_ip: str, preset_id: str, username: str, password: str) -> Dict[str, Any]:
        if not username or not password:
            return {"ok": False, "error": "Missing Braiins credentials for scheduled preset command."}
        base_url = f"http://{target_ip}/api/v1"
        try:
            login = self.session.post(
                f"{base_url}/auth/login",
                json={"username": username, "password": password},
                timeout=max(3.0, float(self.http_timeout_seconds)),
            )
            if not login.ok:
                return {"ok": False, "error": f"Braiins login failed HTTP {login.status_code}", "statusCode": login.status_code}
            token = str((login.json() if login.content else {}).get("token") or "").strip()
            if not token:
                return {"ok": False, "error": "Braiins login did not return a token."}
            headers = {"Authorization": token}

            if preset_id == "turbo":
                response = self.session.put(
                    f"{base_url}/performance/hashrate-target",
                    headers=headers,
                    timeout=max(3.0, float(self.http_timeout_seconds)),
                )
                if not response.ok:
                    return {"ok": False, "error": f"Braiins default target failed HTTP {response.status_code}", "statusCode": response.status_code}
                return {"ok": True, "message": "Restored Braiins default hashrate target.", "appliedPresetId": "turbo"}

            stats_response = self.session.get(
                f"{base_url}/miner/stats",
                headers=headers,
                timeout=max(3.0, float(self.http_timeout_seconds)),
            )
            if not stats_response.ok:
                return {"ok": False, "error": f"Braiins stats failed HTTP {stats_response.status_code}", "statusCode": stats_response.status_code}
            stats = stats_response.json()
            nominal_gh = float(self._nested_value(stats, ["miner_stats", "nominal_hashrate", "gigahash_per_second"]) or 0)
            current_target_th = float(self._nested_value(stats, ["tuner_state", "mode_state", "hashrate_mode", "hashrate_target", "terahash_per_second"]) or 0)
            baseline_th = max(nominal_gh / 1000.0, current_target_th)
            if baseline_th <= 0:
                return {"ok": False, "skipped": True, "error": "Braiins nominal/current target unavailable; skipped."}
            eco_target_th = max(1, int(round(baseline_th * 0.72)))
            response = self.session.put(
                f"{base_url}/performance/hashrate-target",
                headers={**headers, "Content-Type": "application/json"},
                json={"terahash_per_second": float(eco_target_th)},
                timeout=max(3.0, float(self.http_timeout_seconds)),
            )
            if not response.ok:
                return {"ok": False, "error": f"Braiins Eco target failed HTTP {response.status_code}", "statusCode": response.status_code}
            return {
                "ok": True,
                "message": f"Applied Braiins Eco target {eco_target_th} TH/s.",
                "appliedPresetId": "eco",
                "targetTH": eco_target_th,
            }
        except Exception as exc:  # pylint: disable=broad-except
            return {"ok": False, "error": str(exc)}

    def _apply_fleet_preset_to_target(self, preset_id: str, target: Dict[str, Any]) -> Dict[str, Any]:
        target_ip = str(target.get("ip") or "").strip()
        target_mac = normalize_mac(target.get("mac"))
        device_type = self._normalize_fleet_device_type(target.get("deviceType"))
        if not target_ip:
            return {"ok": False, "error": "Missing target IP."}

        if device_type == "bitaxe":
            tune_id = "stock" if preset_id == "eco" else "heavy_oc"
            return self._apply_bitaxe_tune(target_ip, tune_id, target_mac)

        if device_type == "canaan":
            subtype = self._normalize_canaan_subtype(target.get("deviceSubtype"))
            if subtype == "unknown":
                inventory_device = self._find_inventory_device_by_mac_or_ip(target_mac, target_ip, "canaan")
                if inventory_device:
                    subtype = self._normalize_canaan_subtype(inventory_device.get("deviceSubtype"))
            mode_value = self._canaan_mode_for_fleet_preset(preset_id, subtype)
            return self._apply_canaan_work_mode(target_ip, subtype, mode_value)

        if device_type == "luxos":
            return self._apply_luxos_profile_preset(target_ip, preset_id)

        if device_type == "braiins":
            return self._apply_braiins_fleet_preset(
                target_ip,
                preset_id,
                str(target.get("username") or "").strip(),
                str(target.get("password") or "").strip(),
            )

        return {"ok": False, "skipped": True, "error": f"{device_type or 'Device'} does not support fleet presets."}

    def _send_luxos_json_command(self, target_ip: str, command: str, parameter: Optional[str] = None, port: int = 4028) -> Dict[str, Any]:
        sock = None
        payload: Dict[str, Any] = {"command": command}
        if parameter:
            payload["parameter"] = parameter
        try:
            sock = socket.create_connection((target_ip, port), timeout=max(2.0, float(self.http_timeout_seconds)))
            sock.settimeout(5)
            sock.sendall((json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8"))
            chunks: List[bytes] = []
            while True:
                try:
                    chunk = sock.recv(65536)
                except socket.timeout:
                    break
                if not chunk:
                    break
                chunks.append(chunk)
            raw_text = b"".join(chunks).decode("utf-8", errors="ignore").replace("\x00", "").strip()
            if not raw_text:
                return {"ok": False, "error": f"LuxOS {command} returned no data.", "payload": payload}
            response = json.loads(raw_text)
            if not isinstance(response, dict):
                return {"ok": False, "error": f"LuxOS {command} returned an unexpected response.", "raw": raw_text[:6000]}
            error = self._luxos_response_error(response)
            return {
                "ok": error is None,
                "error": error,
                "response": response,
                "raw": raw_text[:6000],
                "payload": payload,
            }
        except Exception as exc:  # pylint: disable=broad-except
            return {"ok": False, "error": str(exc), "payload": payload}
        finally:
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass

    def _luxos_response_error(self, response: Dict[str, Any]) -> Optional[str]:
        for item in self._luxos_array_items(response, "STATUS"):
            state = str(item.get("STATUS") or "").strip().upper()
            if state in {"E", "F"}:
                return str(
                    item.get("Msg")
                    or item.get("Message")
                    or item.get("Description")
                    or "LuxOS command failed."
                )
        return None

    def _luxos_array_items(self, response: Dict[str, Any], preferred_key: str) -> List[Dict[str, Any]]:
        value = response.get(preferred_key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            return [value]
        preferred_upper = preferred_key.upper()
        for key, value in response.items():
            if str(key).upper() != preferred_upper:
                continue
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
            if isinstance(value, dict):
                return [value]
        for value in response.values():
            if isinstance(value, list) and all(isinstance(item, dict) for item in value):
                return value
        return []

    def _luxos_string_value(self, item: Dict[str, Any], keys: List[str]) -> Optional[str]:
        for key in keys:
            value = item.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        lower_map = {str(key).lower(): value for key, value in item.items()}
        for key in keys:
            value = lower_map.get(key.lower())
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return None

    def _luxos_session_id_from_response(self, response: Dict[str, Any]) -> Optional[str]:
        direct = self._luxos_string_value(response, ["SessionID", "SESSIONID", "session"])
        if direct:
            return direct
        for item in self._luxos_array_items(response, "SESSION"):
            session_id = self._luxos_string_value(item, ["SessionID", "SESSIONID", "session"])
            if session_id:
                return session_id
        for value in response.values():
            if isinstance(value, list):
                for item in value:
                    if not isinstance(item, dict):
                        continue
                    session_id = self._luxos_string_value(item, ["SessionID", "SESSIONID", "session"])
                    if session_id:
                        return session_id
        return None

    def _luxos_session_id(self, target_ip: str) -> Optional[str]:
        logon = self._send_luxos_json_command(target_ip, "logon")
        response = logon.get("response")
        if isinstance(response, dict):
            session_id = self._luxos_session_id_from_response(response)
            if session_id:
                return session_id

        session = self._send_luxos_json_command(target_ip, "session")
        response = session.get("response")
        if isinstance(response, dict):
            return self._luxos_session_id_from_response(response)
        return None

    def _luxos_step_value(self, profile: Dict[str, Any]) -> Optional[int]:
        raw_step = self._luxos_string_value(profile, ["Step"])
        if raw_step is None:
            return None
        match = re.search(r"[-+]?\d+", raw_step)
        if not match:
            return None
        try:
            return int(match.group(0))
        except ValueError:
            return None

    def _luxos_profile_name(self, profile: Dict[str, Any]) -> Optional[str]:
        return self._luxos_string_value(profile, ["Profile Name", "Profile"])

    def _luxos_profile_for_preset(self, preset_id: str, profiles: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        candidates: List[Tuple[int, Dict[str, Any]]] = []
        for profile in profiles:
            step = self._luxos_step_value(profile)
            name = self._luxos_profile_name(profile)
            if step is None or not name:
                continue
            candidates.append((step, profile))

        if preset_id == "eco":
            return min(candidates, key=lambda item: item[0])[1] if candidates else None

        if preset_id == "turbo":
            required_step = 1
            turbo_rule = self.luxos_profile_rules.get("turbo") if isinstance(self.luxos_profile_rules, dict) else {}
            if isinstance(turbo_rule, dict):
                required_step = parse_int(turbo_rule.get("requiredStep"), default=1, minimum=-100, maximum=100)
            for step, profile in candidates:
                if step == required_step:
                    return profile
        return None

    def _apply_luxos_profile_preset(self, target_ip: str, preset_id: str) -> Dict[str, Any]:
        if preset_id not in {"eco", "turbo"}:
            return {"ok": False, "error": f"Unknown LuxOS preset '{preset_id}'."}

        profiles_result = self._send_luxos_json_command(target_ip, "profiles")
        if not profiles_result.get("ok"):
            return {
                "ok": False,
                "error": profiles_result.get("error") or "Failed to fetch LuxOS profiles.",
                "profilesResult": profiles_result,
            }

        response = profiles_result.get("response")
        profiles = self._luxos_array_items(response, "PROFILES") if isinstance(response, dict) else []
        target_profile = self._luxos_profile_for_preset(preset_id, profiles)
        if not target_profile:
            expected = "lowest numeric Step" if preset_id == "eco" else "Step 1"
            return {
                "ok": False,
                "error": f"No LuxOS profile matched {expected}; skipped for device safety.",
            }

        profile_name = self._luxos_profile_name(target_profile)
        if not profile_name:
            return {"ok": False, "error": "Matched LuxOS profile is missing a name; skipped for device safety."}

        session_id = self._luxos_session_id(target_ip)
        if not session_id:
            return {"ok": False, "error": "Unable to start or reuse a LuxOS session."}

        set_result = self._send_luxos_json_command(target_ip, "profileset", f"{session_id},{profile_name}")
        if not set_result.get("ok"):
            return {
                "ok": False,
                "error": set_result.get("error") or "LuxOS profile command failed.",
                "setResult": set_result,
            }

        return {
            "ok": True,
            "message": f"Applied LuxOS {preset_id.title()} profile {profile_name}.",
            "appliedPresetId": preset_id,
            "appliedProfile": profile_name,
            "appliedStep": self._luxos_step_value(target_profile),
        }

    def _apply_luxos_curtail(self, target_ip: str, action: str) -> Dict[str, Any]:
        action = str(action or "").strip().lower()
        if action not in {"sleep", "wakeup"}:
            return {"ok": False, "error": f"Unsupported LuxOS curtail action '{action}'."}

        session_id = self._luxos_session_id(target_ip)
        if not session_id:
            return {"ok": False, "error": "Unable to start or reuse a LuxOS session."}

        curtail_result = self._send_luxos_json_command(target_ip, "curtail", f"{session_id},{action}")
        if not curtail_result.get("ok"):
            return {
                "ok": False,
                "error": curtail_result.get("error") or f"LuxOS {action} command failed.",
                "curtailResult": curtail_result,
            }

        label = "wake" if action == "wakeup" else "sleep"
        return {
            "ok": True,
            "message": f"Sent LuxOS {label} command.",
            "appliedAction": action,
        }

    def _apply_fleet_schedule(self, schedule: Dict[str, Any]) -> Dict[str, Any]:
        target_ip = str(schedule.get("targetIp") or "").strip()
        target_mac = normalize_mac(schedule.get("targetMac"))
        action_type = str(schedule.get("actionType") or "").strip().lower()
        mode_value = str(schedule.get("modeValue") or "").strip().lower()
        device_type = self._normalize_fleet_device_type(schedule.get("deviceType"))
        canaan_subtype = ""

        inventory_device = self._find_inventory_device_by_mac_or_ip(target_mac, target_ip, device_type)
        if inventory_device:
            inventory_ip = str(inventory_device.get("ip") or "").strip()
            inventory_mac = normalize_mac(inventory_device.get("mac"))
            inventory_type = self._normalize_fleet_device_type(inventory_device.get("deviceType"))
            canaan_subtype = self._normalize_canaan_subtype(inventory_device.get("deviceSubtype"))
            if inventory_ip:
                target_ip = inventory_ip
            if inventory_mac:
                target_mac = inventory_mac
            if inventory_type:
                device_type = inventory_type

        if not target_ip:
            if target_mac:
                return {"ok": False, "error": f"Missing target IP for MAC {target_mac}."}
            return {"ok": False, "error": "Missing target IP."}

        if action_type == "bitaxe_tune":
            result = self._apply_bitaxe_tune(target_ip, mode_value, target_mac)
            result["resolvedTargetIp"] = target_ip
            result["resolvedTargetMac"] = target_mac or None
            result["resolvedDeviceType"] = device_type or "bitaxe"
            return result
        if action_type == "fleet_preset":
            target = {
                "ip": target_ip,
                "mac": target_mac,
                "deviceType": device_type,
                "deviceSubtype": canaan_subtype,
                "username": inventory_device.get("username") if inventory_device else schedule.get("username"),
                "password": inventory_device.get("password") if inventory_device else schedule.get("password"),
            }
            result = self._apply_fleet_preset_to_target(mode_value, target)
            result["resolvedTargetIp"] = target_ip
            result["resolvedTargetMac"] = target_mac or None
            result["resolvedDeviceType"] = device_type or "device"
            return result
        if action_type == "canaan_work_mode":
            if inventory_device:
                supported_modes = inventory_device.get("supportedWorkModes")
                if isinstance(supported_modes, list) and supported_modes:
                    allowed = {
                        str(item.get("id") or "").strip()
                        for item in supported_modes
                        if isinstance(item, dict)
                    }
                    if mode_value not in allowed:
                        return {
                            "ok": False,
                            "error": f"Mode {mode_value} is not supported for {target_ip}.",
                            "supportedModes": sorted([value for value in allowed if value]),
                            "resolvedTargetIp": target_ip,
                            "resolvedTargetMac": target_mac or None,
                            "resolvedDeviceType": device_type or "canaan",
                        }
            mode_device_type = canaan_subtype or device_type
            result = self._apply_canaan_work_mode(target_ip, mode_device_type, mode_value)
            result["resolvedTargetIp"] = target_ip
            result["resolvedTargetMac"] = target_mac or None
            result["resolvedDeviceType"] = mode_device_type or "canaan"
            return result
        if action_type == "luxos_profile_preset":
            result = self._apply_luxos_profile_preset(target_ip, mode_value)
            result["resolvedTargetIp"] = target_ip
            result["resolvedTargetMac"] = target_mac or None
            result["resolvedDeviceType"] = device_type or "luxos"
            return result
        if action_type == "luxos_sleep":
            result = self._apply_luxos_curtail(target_ip, "sleep")
            result["resolvedTargetIp"] = target_ip
            result["resolvedTargetMac"] = target_mac or None
            result["resolvedDeviceType"] = device_type or "luxos"
            return result
        if action_type == "luxos_wakeup":
            result = self._apply_luxos_curtail(target_ip, "wakeup")
            result["resolvedTargetIp"] = target_ip
            result["resolvedTargetMac"] = target_mac or None
            result["resolvedDeviceType"] = device_type or "luxos"
            return result
        return {"ok": False, "error": f"Unsupported action type '{action_type}'."}

    def _run_fleet_manager_if_needed(self) -> None:
        local_now = datetime.now().astimezone()
        weekday_index = local_now.weekday()  # Monday=0 ... Sunday=6
        hour = local_now.hour
        minute = local_now.minute
        slot_key = local_now.strftime("%Y%m%d-%H%M")

        with self._fleet_lock:
            schedules = self.fleet_schedules
            changed = False
            for schedule in schedules:
                if not schedule.get("enabled", True):
                    continue
                if not self._schedule_includes_weekday(parse_int(schedule.get("daysMask"), default=127, minimum=1, maximum=127), weekday_index):
                    continue
                if parse_int(schedule.get("hour"), default=0, minimum=0, maximum=23) != hour:
                    continue
                if parse_int(schedule.get("minute"), default=0, minimum=0, maximum=59) != minute:
                    continue
                if str(schedule.get("lastTriggeredSlot") or "") == slot_key:
                    continue

                result = self._apply_fleet_schedule(schedule)
                resolved_ip = str(result.get("resolvedTargetIp") or "").strip()
                resolved_mac = normalize_mac(result.get("resolvedTargetMac") or schedule.get("targetMac"))
                if resolved_ip and schedule.get("targetIp") != resolved_ip:
                    schedule["targetIp"] = resolved_ip
                if resolved_mac:
                    schedule["targetMac"] = resolved_mac
                schedule["lastTriggeredSlot"] = slot_key
                schedule["lastTriggeredAtIso"] = now_iso()
                schedule["lastResult"] = result
                changed = True

                action_type = str(schedule.get("actionType") or "").strip().lower()
                resolved_device_type = self._normalize_fleet_device_type(result.get("resolvedDeviceType") or schedule.get("deviceType"))
                if result.get("ok") and resolved_device_type == "bitaxe" and action_type in {"bitaxe_tune", "fleet_preset"} and resolved_mac:
                    applied_tune = str(result.get("appliedTuneId") or schedule.get("modeValue") or "").strip().lower()
                    if action_type == "fleet_preset":
                        applied_tune = "stock" if applied_tune == "eco" else "heavy_oc" if applied_tune == "turbo" else applied_tune
                    if applied_tune in self.bitaxe_tune_presets:
                        self.bitaxe_tune_by_mac[resolved_mac] = applied_tune
                        for device in self.fleet_inventory:
                            if normalize_mac(device.get("mac")) == resolved_mac:
                                device["savedBitaxeTune"] = applied_tune

                if result.get("ok"):
                    target_label = resolved_ip or str(schedule.get("targetIp") or "unknown")
                    self._log("fleet", f"Applied schedule '{schedule.get('name', schedule.get('id'))}' to {target_label}.", level="info")
                else:
                    self._log("fleet", f"Schedule '{schedule.get('name', schedule.get('id'))}' failed: {result.get('error', 'unknown')}", level="warn")

        if changed:
            with self.config_lock:
                self._persist_runtime_config()

    def reset_pairing(self) -> Dict[str, Any]:
        with self.config_lock:
            self.bitaxe_host = ""
            self.paired = False
            self.paired_device_type = ""
            self.paired_miner_mac = ""
            self.paired_miner_hostname = ""
            self._persist_runtime_config()
            return self.get_runtime_config()

    def _purge_all_wifi_credentials(self) -> Dict[str, Any]:
        """Remove Wi-Fi credentials from all common Pi network config sources."""
        wifi_creds_path = os.getenv("LAST_WIFI_PATH", "/opt/hashwatcher-hub-pi/last_wifi_credentials.json")
        try:
            if os.path.exists(wifi_creds_path):
                os.remove(wifi_creds_path)
        except Exception:
            pass

        helper_path = os.getenv("WIFI_RESET_HELPER_PATH", "/opt/hashwatcher-hub-pi/wifi_reset_helper.sh")
        try:
            result = subprocess.run(
                ["sudo", "-n", helper_path],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode != 0:
                detail = (result.stderr or result.stdout or "").strip()
                return {"ok": False, "error": detail or f"{helper_path} exited {result.returncode}"}
            return {"ok": True}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def _force_disconnect_wifi(self) -> None:
        """Aggressively disconnect wlan0 so HTTP access drops before reboot."""
        commands = [
            ["sudo", "-n", "nmcli", "device", "disconnect", "wlan0"],
            ["sudo", "-n", "wpa_cli", "-i", "wlan0", "disconnect"],
            ["sudo", "-n", "nmcli", "radio", "wifi", "off"],
            ["sudo", "-n", "ip", "link", "set", "wlan0", "down"],
        ]
        for cmd in commands:
            try:
                subprocess.run(cmd, capture_output=True, text=True, timeout=8)
            except Exception:
                continue

    def _schedule_sudo_command(
        self,
        command: List[str],
        delay_seconds: float,
        source: str,
        sync_before: bool = False,
    ) -> None:
        def _runner() -> None:
            time.sleep(delay_seconds)
            try:
                if sync_before:
                    subprocess.run(["sync"], capture_output=True, text=True, timeout=10)
                    time.sleep(1)
                result = subprocess.run(command, capture_output=True, text=True, timeout=15)
                if result.returncode != 0:
                    detail = (result.stderr or result.stdout or "unknown error").strip()
                    self._log(source, f"Command failed ({' '.join(command)}): {detail}", level="error")
            except Exception as exc:  # pylint: disable=broad-except
                self._log(source, f"Command failed ({' '.join(command)}): {exc}", level="error")

        threading.Thread(target=_runner, daemon=True).start()

    def reboot_hub(self) -> Dict[str, Any]:
        self._schedule_sudo_command(["sudo", "-n", "reboot"], delay_seconds=2, source="device-action", sync_before=True)
        return {
            "ok": True,
            "action": "reboot",
            "message": "Hub will reboot in about 2 seconds.",
        }

    def reset_wifi(self) -> Dict[str, Any]:
        """Clear saved Wi-Fi credentials and disconnect Wi-Fi.

        Tailscale and miner pairing are left untouched.
        """
        self._force_disconnect_wifi()
        purge_result = self._purge_all_wifi_credentials()
        if not purge_result.get("ok"):
            return {"ok": False, "error": f"Wi-Fi credential purge failed: {purge_result.get('error')}"}
        self._schedule_sudo_command(["sudo", "-n", "reboot"], delay_seconds=3, source="wifi-reset", sync_before=True)

        return {"ok": True, "message": "Wi-Fi disconnected and credentials cleared. Hub will reboot in ~3 seconds. Re-provision via BLE."}

    def disconnect_and_reset_wifi(self) -> Dict[str, Any]:
        """Force disconnect wlan0 immediately, then clear all Wi-Fi credentials and reboot."""
        self._force_disconnect_wifi()
        purge_result = self._purge_all_wifi_credentials()
        if not purge_result.get("ok"):
            return {"ok": False, "error": f"Wi-Fi credential purge failed: {purge_result.get('error')}"}
        self._schedule_sudo_command(["sudo", "-n", "reboot"], delay_seconds=2, source="wifi-reset", sync_before=True)

        return {
            "ok": True,
            "message": "Wi-Fi force-disconnected. Credentials cleared. Hub will reboot in ~2 seconds.",
        }

    def factory_reset(self) -> Dict[str, Any]:
        """Wipe all config, disconnect Tailscale, drop Wi-Fi, and reboot.

        Mimics a fresh-from-box power-on so the full onboarding can be tested.
        """
        tailscale_result = tailscale_setup.factory_forget()
        if not tailscale_result.get("ok"):
            return {
                "ok": False,
                "error": "Factory reset aborted before clearing Wi-Fi because Tailscale could not be deauthorized.",
                "tailscale": tailscale_result,
            }

        with self.config_lock:
            self.bitaxe_host = ""
            self.paired = False
            self.paired_device_type = ""
            self.paired_miner_mac = ""
            self.paired_miner_hostname = ""

            # Delete persisted runtime config
            try:
                if os.path.exists(self.runtime_config_path):
                    os.remove(self.runtime_config_path)
            except Exception:
                pass

        purge_result = self._purge_all_wifi_credentials()
        if not purge_result.get("ok"):
            return {
                "ok": False,
                "error": f"Factory reset aborted because Wi-Fi credential purge failed: {purge_result.get('error')}",
                "tailscale": tailscale_result,
            }
        self._schedule_sudo_command(["sudo", "-n", "reboot"], delay_seconds=3, source="factory-reset", sync_before=True)

        return {
            "ok": True,
            "message": "Factory reset complete. Tailscale node key was removed; hub will reboot in ~3 seconds.",
            "tailscale": tailscale_result,
        }

    def _get_storage_health(self) -> Dict[str, Any]:
        findings: List[Dict[str, Any]] = []

        mount_result = self._run_capture(["findmnt", "-no", "OPTIONS", "/"], timeout=5)
        mount_options = str(mount_result.get("output") or "").strip()
        mount_option_set = {item.strip() for item in mount_options.split(",") if item.strip()}
        root_read_only = "ro" in mount_option_set and "rw" not in mount_option_set
        root_writable = not root_read_only if mount_options else True
        if root_read_only:
            findings.append({
                "component": "filesystem",
                "issue": "root_read_only",
                "message": "Root filesystem is mounted read-only; storage may already be damaged.",
            })

        apt_archive_partial = "/var/cache/apt/archives/partial"
        apt_cache_present = os.path.isdir(apt_archive_partial)
        if not apt_cache_present:
            findings.append({
                "component": "filesystem",
                "issue": "apt_cache_missing",
                "message": f"apt archive cache directory is missing: {apt_archive_partial}",
            })

        disk_free_percent: Optional[float] = None
        try:
            stat = os.statvfs("/")
            total_bytes = stat.f_blocks * stat.f_frsize
            available_bytes = stat.f_bavail * stat.f_frsize
            if total_bytes > 0:
                disk_free_percent = round((available_bytes / total_bytes) * 100.0, 1)
                if disk_free_percent < 5.0:
                    findings.append({
                        "component": "filesystem",
                        "issue": "low_disk_space",
                        "message": f"Root filesystem free space is low ({disk_free_percent}%).",
                    })
        except Exception:
            pass

        journal_result = self._read_journal(["-k", "-n", "300", "--no-pager"], timeout=8)
        journal_text = str(journal_result.get("output") or "")
        fs_error_re = re.compile(
            r"(EXT4-fs error|I/O error|Buffer I/O|structure needs cleaning|mmcblk\S*.*error|end_request: I/O error)",
            re.IGNORECASE,
        )
        fs_error_lines = [
            line.strip()
            for line in journal_text.splitlines()
            if fs_error_re.search(line)
        ][-10:]
        if fs_error_lines:
            findings.append({
                "component": "filesystem",
                "issue": "kernel_filesystem_errors",
                "message": "Kernel log contains recent filesystem or storage I/O errors.",
                "samples": fs_error_lines,
            })

        throttled_raw = ""
        throttled_result = self._run_capture(["vcgencmd", "get_throttled"], timeout=5)
        if not throttled_result.get("ok"):
            throttled_result = self._run_capture(["sudo", "-n", "vcgencmd", "get_throttled"], timeout=5)
        if throttled_result.get("ok"):
            throttled_raw = str(throttled_result.get("output") or "").strip()
        throttled_value = 0
        match = re.search(r"0x[0-9a-fA-F]+|\d+", throttled_raw)
        if match:
            try:
                throttled_value = int(match.group(0), 0)
            except ValueError:
                throttled_value = 0
        undervoltage_or_throttled = throttled_value != 0
        if undervoltage_or_throttled:
            findings.append({
                "component": "power",
                "issue": "pi_throttled_or_undervoltage",
                "message": f"vcgencmd reports throttling/undervoltage history: {throttled_raw}",
            })

        healthy = (
            root_writable
            and apt_cache_present
            and not fs_error_lines
            and (disk_free_percent is None or disk_free_percent >= 5.0)
            and not undervoltage_or_throttled
        )

        return {
            "healthy": healthy,
            "rootWritable": root_writable,
            "rootMountOptions": mount_options,
            "aptArchiveCachePresent": apt_cache_present,
            "diskFreePercent": disk_free_percent,
            "kernelFilesystemErrorCount": len(fs_error_lines),
            "kernelFilesystemErrorSamples": fs_error_lines,
            "throttledRaw": throttled_raw,
            "undervoltageOrThrottled": undervoltage_or_throttled,
            "findings": findings,
        }

    def self_check(self) -> Dict[str, Any]:
        ts_status = tailscale_setup.status()
        diag = tailscale_setup.diagnose()
        usb_diag = diag.get("usbGadget", {})
        storage_health = self._get_storage_health()
        findings = list(diag.get("findings", [])) + list(storage_health.get("findings", []))

        checks: Dict[str, Any] = {
            "wifiConnected": self._get_local_ip() is not None,
            "tailscaleAuthenticated": ts_status.get("authenticated", False),
            "tailscaleOnline": ts_status.get("online", False),
            "tailscaleNodeNotFound": any(
                f.get("issue") == "node_not_found" for f in diag.get("findings", [])
            ),
            "tailscaleKeyExpired": ts_status.get("keyExpired", False),
            "usbGadgetUp": usb_diag.get("usb0Exists", False) and usb_diag.get("usb0HasIp", False),
            "usbGadgetServiceActive": usb_diag.get("serviceActive", False),
            "storageHealthy": storage_health.get("healthy", False),
            "rootFilesystemWritable": storage_health.get("rootWritable", True),
            "aptArchiveCachePresent": storage_health.get("aptArchiveCachePresent", False),
            "kernelFilesystemErrors": storage_health.get("kernelFilesystemErrorCount", 0),
            "diskFreePercent": storage_health.get("diskFreePercent"),
            "undervoltageOrThrottled": storage_health.get("undervoltageOrThrottled", False),
        }

        healthy = all([
            checks["wifiConnected"],
            checks["tailscaleAuthenticated"],
            not checks["tailscaleNodeNotFound"],
            checks["storageHealthy"],
        ])
        return {
            "ok": healthy,
            "checks": checks,
            "storage": storage_health,
            "findings": findings,
            "checkedAtIso": now_iso(),
        }

    def check_for_update(self) -> Dict[str, Any]:
        """Query GitHub releases API for a newer version."""
        url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
        try:
            resp = requests.get(url, timeout=15, headers={"Accept": "application/vnd.github+json"})
            if resp.status_code in (401, 403, 404):
                return {
                    "ok": True,
                    "updateAvailable": False,
                    "reason": "no update available",
                    "currentVersion": AGENT_VERSION,
                }
            resp.raise_for_status()
            release = resp.json()
        except Exception as exc:
            return {"ok": False, "error": f"Failed to check GitHub: {exc}"}

        tag = str(release.get("tag_name", "")).lstrip("v").strip()
        if not tag:
            return {"ok": False, "error": "Release has no version tag"}

        deb_asset = None
        for asset in release.get("assets", []):
            name = asset.get("name", "")
            if name.endswith(".deb"):
                deb_asset = asset
                break

        update_available = is_newer_version(tag, AGENT_VERSION)
        result: Dict[str, Any] = {
            "ok": True,
            "currentVersion": AGENT_VERSION,
            "latestVersion": tag,
            "updateAvailable": update_available,
            "releaseName": release.get("name", ""),
            "publishedAt": release.get("published_at", ""),
            "releaseUrl": release.get("html_url", ""),
        }
        if deb_asset:
            result["debAsset"] = {
                "name": deb_asset["name"],
                "size": deb_asset.get("size", 0),
                "downloadUrl": deb_asset.get("browser_download_url", ""),
            }
        return result

    def get_update_progress(self) -> Dict[str, Any]:
        disk_progress = self._read_update_progress()
        with self._update_lock:
            memory_progress = dict(self._update_progress)
        if not disk_progress:
            return memory_progress

        disk_updated = str(disk_progress.get("updatedAtIso", ""))
        memory_updated = str(memory_progress.get("updatedAtIso", ""))
        if disk_updated >= memory_updated:
            with self._update_lock:
                self._update_progress = dict(disk_progress)
            return disk_progress
        return memory_progress

    def apply_update(self) -> Dict[str, Any]:
        """Start background download and install of the latest .deb from GitHub."""
        current_progress = self.get_update_progress()
        if current_progress.get("stage") in ("downloading", "installing", "restarting"):
            return {"ok": True, "message": "Update already in progress.", **current_progress}

        check = self.check_for_update()
        if not check.get("ok"):
            return check
        if not check.get("updateAvailable"):
            return {"ok": True, "message": f"Already on latest version ({AGENT_VERSION})"}

        deb_info = check.get("debAsset")
        if not deb_info or not deb_info.get("downloadUrl"):
            return {"ok": False, "error": "No .deb asset found in the latest release"}

        self._set_update_progress({
            "stage": "downloading",
            "percent": 0,
            "version": check["latestVersion"],
            "message": "Starting download...",
        })

        def _run_update() -> None:
            download_url = deb_info["downloadUrl"]
            total_size = deb_info.get("size", 0)
            work_dir = "/opt/hashwatcher-hub-pi/updates"
            os.makedirs(work_dir, exist_ok=True)
            deb_path = os.path.join(work_dir, deb_info["name"])

            try:
                resp = requests.get(download_url, timeout=120, stream=True)
                resp.raise_for_status()
                sha = hashlib.sha256()
                downloaded = 0
                with open(deb_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=128 * 1024):
                        if chunk:
                            f.write(chunk)
                            sha.update(chunk)
                            downloaded += len(chunk)
                            pct = int(downloaded * 100 / total_size) if total_size > 0 else 0
                            self._set_update_progress({
                                "stage": "downloading",
                                "percent": min(pct, 100),
                                "version": check["latestVersion"],
                                "message": f"Downloaded {downloaded // 1024} KB" + (f" / {total_size // 1024} KB" if total_size else ""),
                            })
            except Exception as exc:
                self._set_update_progress({"stage": "failed", "percent": 0, "error": f"Download failed: {exc}"})
                return

            progress = self._set_update_progress({
                "stage": "installing",
                "percent": 100,
                "version": check["latestVersion"],
                "message": "Installing package...",
                "previousVersion": AGENT_VERSION,
                "sha256": sha.hexdigest(),
            })

            try:
                result = subprocess.run(
                    [
                        "sudo",
                        "/usr/bin/systemd-run",
                        "--unit",
                        self.update_unit_name,
                        "--collect",
                        "--service-type=oneshot",
                        self.update_helper_path,
                        deb_path,
                        check["latestVersion"],
                        AGENT_VERSION,
                        sha.hexdigest(),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=15,
                )
                if result.returncode != 0:
                    stderr = result.stderr.strip() or result.stdout.strip()
                    self._set_update_progress({"stage": "failed", "percent": 0, "error": f"update handoff failed: {stderr}"})
                    return
            except Exception as exc:
                self._set_update_progress({"stage": "failed", "percent": 0, "error": f"Install failed: {exc}"})
                return

            self._set_update_progress({
                **progress,
                "message": "Installer launched. Waiting for restart...",
            })

        threading.Thread(target=_run_update, daemon=True).start()
        return {"ok": True, "message": "Update started.", **self.get_update_progress()}

    def _bitaxe_url(self, host: str, endpoint: str) -> str:
        return f"{self.bitaxe_scheme}://{host}{endpoint}"

    def _parse_payload_data(self, parsed: Any) -> Dict[str, Any]:
        if isinstance(parsed, dict) and isinstance(parsed.get("data"), dict):
            return parsed["data"]
        if isinstance(parsed, dict):
            return parsed
        raise ValueError(f"Unexpected JSON type: {type(parsed)}")

    def _fetch_bitaxe_from_host(self, host: str, timeout_seconds: float) -> Optional[Dict[str, Any]]:
        for endpoint in self.endpoints:
            url = self._bitaxe_url(host, endpoint)
            try:
                response = self.session.get(url, timeout=timeout_seconds)
                response.raise_for_status()
                parsed = response.json()
                data = self._parse_payload_data(parsed)
                return {
                    "ip": host,
                    "endpoint": endpoint,
                    "source_url": url,
                    "data": data,
                }
            except Exception:
                continue
        return None

    def fetch_paired_miner(self) -> Optional[Dict[str, Any]]:
        if not self.paired or not self.bitaxe_host.strip():
            return None
        result = self._fetch_bitaxe_from_host(self.bitaxe_host, float(self.http_timeout_seconds))
        if not result:
            return None
        return {
            "source_url": result["source_url"],
            "endpoint": result["endpoint"],
            "data": result["data"],
        }

    def proxy_miner_request(self, target_ip: str, path: str, method: str = "GET", body: Optional[bytes] = None) -> Dict[str, Any]:
        """Proxy an HTTP request to a miner on the local network."""
        if not target_ip or not target_ip.strip():
            raise ValueError("target IP is required")

        clean_path = path.strip()
        if not clean_path.startswith("/"):
            clean_path = "/" + clean_path

        url = f"{self.bitaxe_scheme}://{target_ip.strip()}{clean_path}"
        timeout = float(self.http_timeout_seconds)

        if method.upper() == "POST":
            response = self.session.post(url, data=body, timeout=timeout, headers={"Content-Type": "application/json"} if body else {})
        else:
            response = self.session.get(url, timeout=timeout)

        try:
            data = response.json()
        except Exception:
            data = response.text

        return {
            "ok": response.ok,
            "statusCode": response.status_code,
            "url": url,
            "data": data,
        }

    def infer_device_type(self, data: Dict[str, Any], model: Any, hostname: Any) -> str:
        explicit = pick_first(data, ["deviceType", "minerType"])
        parts: List[str] = []
        for value in [explicit, model, hostname]:
            if isinstance(value, str) and value.strip():
                parts.append(value.strip().lower())
        combined = " ".join(parts)
        if "bitdsk" in combined:
            return "bitdsk"
        if "octaxe" in combined or "octa" in combined:
            return "octaxe"
        if "nerdq" in combined or "qaxe" in combined:
            return "nerdq"
        return "bitaxe"

    def discover_bitaxe_devices(self, cidr: Optional[str] = None) -> Dict[str, Any]:
        network: ipaddress.IPv4Network
        if cidr:
            network = ipaddress.ip_network(cidr, strict=False)
        else:
            local_ip = self._get_local_ip()
            if not local_ip:
                raise RuntimeError("Unable to determine Pi local IP for subnet scan")
            octets = local_ip.split(".")
            network = ipaddress.ip_network(f"{octets[0]}.{octets[1]}.{octets[2]}.0/24", strict=False)

        hosts = [str(ip) for ip in network.hosts()]
        if len(hosts) > 1024:
            hosts = hosts[:1024]

        found: List[Dict[str, Any]] = []
        start = time.time()

        def worker(host: str) -> Optional[Dict[str, Any]]:
            result = self._fetch_bitaxe_from_host(host, 0.8)
            if not result:
                return None
            data = result["data"]
            normalized = self.normalize(data)
            return {
                "ip": host,
                "hostname": normalized.get("hostname"),
                "mac": normalized.get("mac"),
                "model": normalized.get("model"),
                "deviceType": normalized.get("device_type"),
                "firmware": normalized.get("firmware"),
                "tempC": normalized.get("temp_c"),
                "hashrateTHS": normalized.get("hashrate_ths"),
                "powerW": normalized.get("power_w"),
                "powerEfficiencyJTH": normalized.get("power_efficiency_j_th"),
                "endpoint": result["endpoint"],
            }

        with ThreadPoolExecutor(max_workers=32) as executor:
            futures = [executor.submit(worker, host) for host in hosts]
            for future in as_completed(futures):
                try:
                    entry = future.result()
                    if entry:
                        found.append(entry)
                except Exception:
                    continue

        found.sort(key=lambda item: item.get("ip", ""))
        return {
            "ok": True,
            "scanCidr": str(network),
            "scannedHosts": len(hosts),
            "durationSeconds": round(time.time() - start, 2),
            "devices": found,
        }

    def _get_local_ip(self) -> Optional[str]:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect(("8.8.8.8", 80))
            ip = sock.getsockname()[0]
            sock.close()
            return ip
        except Exception:
            return None

    def _read_cpu_totals(self) -> tuple[Optional[int], Optional[int]]:
        try:
            with open("/proc/stat", "r", encoding="utf-8") as f:
                line = f.readline().strip()
            parts = line.split()
            if len(parts) < 5 or parts[0] != "cpu":
                return None, None
            values = [int(v) for v in parts[1:]]
            total = sum(values)
            idle = values[3] + (values[4] if len(values) > 4 else 0)
            return total, idle
        except Exception:
            return None, None

    def _cpu_usage_percent(self) -> Optional[float]:
        total, idle = self._read_cpu_totals()
        if total is None or idle is None:
            return None
        prev_total, prev_idle = self._cpu_prev_total, self._cpu_prev_idle
        self._cpu_prev_total, self._cpu_prev_idle = total, idle
        if prev_total <= 0 or total <= prev_total:
            return None
        total_delta = total - prev_total
        idle_delta = idle - prev_idle
        busy_delta = max(0, total_delta - idle_delta)
        return round((busy_delta / total_delta) * 100.0, 2) if total_delta > 0 else None

    def _memory_telemetry(self) -> Dict[str, Optional[float]]:
        result: Dict[str, Optional[float]] = {"totalMB": None, "availableMB": None, "usedPercent": None}
        try:
            values: Dict[str, int] = {}
            with open("/proc/meminfo", "r", encoding="utf-8") as f:
                for line in f:
                    key, _, rest = line.partition(":")
                    fields = rest.strip().split()
                    if fields:
                        values[key] = int(fields[0])
            total_kb = values.get("MemTotal")
            avail_kb = values.get("MemAvailable")
            if total_kb and avail_kb is not None:
                used_kb = max(0, total_kb - avail_kb)
                result["totalMB"] = round(total_kb / 1024.0, 2)
                result["availableMB"] = round(avail_kb / 1024.0, 2)
                result["usedPercent"] = round((used_kb / total_kb) * 100.0, 2)
        except Exception:
            pass
        return result

    def _disk_telemetry(self) -> Dict[str, Optional[float]]:
        result: Dict[str, Optional[float]] = {"totalGB": None, "freeGB": None, "usedPercent": None}
        try:
            stats = os.statvfs("/")
            total = stats.f_blocks * stats.f_frsize
            free = stats.f_bavail * stats.f_frsize
            used = max(0, total - free)
            if total > 0:
                result["totalGB"] = round(total / (1024.0 ** 3), 3)
                result["freeGB"] = round(free / (1024.0 ** 3), 3)
                result["usedPercent"] = round((used / total) * 100.0, 2)
        except Exception:
            pass
        return result

    def _soc_temp_c(self) -> Optional[float]:
        candidates = ["/sys/class/thermal/thermal_zone0/temp", "/sys/class/hwmon/hwmon0/temp1_input"]
        for path in candidates:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    raw = f.read().strip()
                value = float(raw)
                if value > 1000:
                    value = value / 1000.0
                return round(value, 2)
            except Exception:
                continue
        return None

    def _run_vcgencmd(self, *args: str) -> Optional[str]:
        commands = [
            ["sudo", "-n", "/usr/bin/vcgencmd", *args],
            ["/usr/bin/vcgencmd", *args],
            ["vcgencmd", *args],
        ]
        for command in commands:
            try:
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=3,
                    check=False,
                )
            except Exception:
                continue

            if result.returncode != 0:
                continue
            output = (result.stdout or "").strip()
            if output:
                return output
        return None

    def _core_voltage_v(self) -> Optional[float]:
        output = self._run_vcgencmd("measure_volts", "core")
        if not output:
            return None

        match = re.search(r"(-?\d+(?:\.\d+)?)\s*V", output)
        if not match:
            return None

        try:
            return round(float(match.group(1)), 4)
        except (TypeError, ValueError):
            return None

    def _arm_clock_mhz(self) -> Optional[int]:
        output = self._run_vcgencmd("measure_clock", "arm")
        if not output:
            return None

        match = re.search(r"=\s*(\d+)", output)
        if not match:
            match = re.search(r"(\d+)\s*$", output)
        if not match:
            return None

        try:
            hz = int(match.group(1))
        except (TypeError, ValueError):
            return None

        if hz <= 0:
            return None
        return int(round(hz / 1_000_000.0))

    def _max_arm_clock_mhz(self) -> Optional[int]:
        output = self._run_vcgencmd("get_config", "arm_freq")
        if output:
            match = re.search(r"arm_freq\s*=\s*(\d+)", output)
            if match:
                try:
                    mhz = int(match.group(1))
                    if mhz > 0:
                        return mhz
                except (TypeError, ValueError):
                    pass

        max_freq_path = "/sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq"
        try:
            raw = _read_text(max_freq_path)
            if raw:
                khz = int(raw.strip())
                if khz > 0:
                    return int(round(khz / 1000.0))
        except Exception:
            pass

        return None

    def get_pi_telemetry(self) -> Dict[str, Any]:
        load1, load5, load15 = (None, None, None)
        try:
            load = os.getloadavg()
            load1, load5, load15 = round(load[0], 3), round(load[1], 3), round(load[2], 3)
        except Exception:
            pass

        return {
            "timestampIso": now_iso(),
            "hostname": socket.gethostname(),
            "localIp": self._get_local_ip(),
            "cpuPercent": self._cpu_usage_percent(),
            "cpuCount": os.cpu_count(),
            "loadAvg1m": load1,
            "loadAvg5m": load5,
            "loadAvg15m": load15,
            "memory": self._memory_telemetry(),
            "diskRoot": self._disk_telemetry(),
            "socTempC": self._soc_temp_c(),
            "armClockMHz": self._arm_clock_mhz(),
            "maxArmClockMHz": self._max_arm_clock_mhz(),
            "coreVoltageV": self._core_voltage_v(),
            "agentUptimeSeconds": int(time.time() - self.state.started_at),
        }

    def _detect_status_led(self) -> Optional[Dict[str, str]]:
        leds_root = "/sys/class/leds"
        try:
            entries = sorted(os.listdir(leds_root))
        except Exception:
            return None

        preferred_names = ["ACT", "act", "led0"]
        ordered: List[str] = []
        seen = set()

        for name in preferred_names:
            if name in entries and name not in seen:
                ordered.append(name)
                seen.add(name)

        for name in entries:
            lowered = name.lower()
            if name not in seen and ("act" in lowered or lowered == "led0"):
                ordered.append(name)
                seen.add(name)

        if not ordered:
            return None

        led_name = ordered[0]
        return {
            "name": led_name,
            "dir": os.path.join(leds_root, led_name),
        }

    def _read_sysfs_text(self, path: str) -> Optional[str]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            return None

    def _write_sysfs_text(self, path: str, value: str) -> None:
        result = subprocess.run(
            ["sudo", "-n", "tee", path],
            input=f"{value}\n",
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            stderr = (result.stderr or result.stdout).strip()
            raise RuntimeError(stderr or f"Failed to write {path}")

    def _parse_led_trigger(self, raw: Optional[str]) -> tuple[Optional[str], List[str]]:
        if not raw:
            return None, []
        available: List[str] = []
        current: Optional[str] = None
        for part in raw.split():
            token = part.strip()
            if not token:
                continue
            if token.startswith("[") and token.endswith("]"):
                token = token[1:-1]
                current = token
            available.append(token)
        return current, available

    def _infer_led_default_trigger(
        self,
        current_trigger: Optional[str],
        available_triggers: List[str],
    ) -> Optional[str]:
        manual_triggers = {"none", "timer"}

        if self._led_default_trigger and self._led_default_trigger in available_triggers:
            return self._led_default_trigger

        if current_trigger and current_trigger not in manual_triggers:
            return current_trigger

        preferred_defaults = [
            "mmc0",
            "actpwr",
            "mmc1",
            "default-on",
            "heartbeat",
            "cpu",
            "cpu0",
            "input",
        ]
        for trigger in preferred_defaults:
            if trigger in available_triggers:
                return trigger

        for trigger in available_triggers:
            if trigger not in manual_triggers:
                return trigger

        return None

    def _read_led_status_unlocked(self) -> Dict[str, Any]:
        led = self._detect_status_led()
        if not led:
            return {
                "ok": False,
                "supported": False,
                "error": "Pi activity LED not detected",
            }

        brightness_path = os.path.join(led["dir"], "brightness")
        max_brightness_path = os.path.join(led["dir"], "max_brightness")
        trigger_path = os.path.join(led["dir"], "trigger")
        delay_on_path = os.path.join(led["dir"], "delay_on")
        delay_off_path = os.path.join(led["dir"], "delay_off")

        trigger_raw = self._read_sysfs_text(trigger_path)
        current_trigger, available_triggers = self._parse_led_trigger(trigger_raw)
        inferred_default_trigger = self._infer_led_default_trigger(current_trigger, available_triggers)
        if inferred_default_trigger:
            self._led_default_trigger = inferred_default_trigger

        brightness = parse_int(self._read_sysfs_text(brightness_path), default=0, minimum=0)
        max_brightness = parse_int(self._read_sysfs_text(max_brightness_path), default=1, minimum=1)
        delay_on_ms = parse_int(self._read_sysfs_text(delay_on_path), default=0, minimum=0)
        delay_off_ms = parse_int(self._read_sysfs_text(delay_off_path), default=0, minimum=0)

        if current_trigger == "timer":
            mode = "blink"
        elif current_trigger == "none":
            mode = "on" if brightness > 0 else "off"
        else:
            mode = "default"

        mode_label_map = {
            "on": "On",
            "off": "Off",
            "blink": "Blink",
            "default": "Default",
        }

        return {
            "ok": True,
            "supported": True,
            "name": led["name"],
            "path": led["dir"],
            "brightness": brightness,
            "maxBrightness": max_brightness,
            "currentTrigger": current_trigger,
            "defaultTrigger": inferred_default_trigger,
            "availableTriggers": available_triggers,
            "delayOnMs": delay_on_ms,
            "delayOffMs": delay_off_ms,
            "mode": mode,
            "modeLabel": mode_label_map.get(mode, mode.title()),
        }

    def get_led_status(self) -> Dict[str, Any]:
        with self._led_lock:
            return self._read_led_status_unlocked()

    def control_led(self, action: str, blink_on_ms: int = 250, blink_off_ms: int = 250) -> Dict[str, Any]:
        normalized_action = action.strip().lower()
        if normalized_action not in {"on", "off", "blink", "restore-default"}:
            raise ValueError("action must be one of: on, off, blink, restore-default")

        with self._led_lock:
            status = self._read_led_status_unlocked()
            if not status.get("supported"):
                raise RuntimeError(status.get("error") or "LED control unavailable")

            led_dir = status["path"]
            brightness_path = os.path.join(led_dir, "brightness")
            trigger_path = os.path.join(led_dir, "trigger")
            delay_on_path = os.path.join(led_dir, "delay_on")
            delay_off_path = os.path.join(led_dir, "delay_off")
            max_brightness = status.get("maxBrightness") or 1

            if normalized_action == "on":
                self._write_sysfs_text(trigger_path, "none")
                self._write_sysfs_text(brightness_path, str(max_brightness))
            elif normalized_action == "off":
                self._write_sysfs_text(trigger_path, "none")
                self._write_sysfs_text(brightness_path, "0")
            elif normalized_action == "blink":
                available = set(status.get("availableTriggers") or [])
                if "timer" not in available:
                    raise RuntimeError("LED does not support the timer trigger")
                self._write_sysfs_text(trigger_path, "timer")
                self._write_sysfs_text(delay_on_path, str(parse_int(blink_on_ms, default=250, minimum=50, maximum=5000)))
                self._write_sysfs_text(delay_off_path, str(parse_int(blink_off_ms, default=250, minimum=50, maximum=5000)))
            else:
                restore_trigger = status.get("defaultTrigger")
                if not restore_trigger:
                    raise RuntimeError("Default LED trigger is unknown")
                self._write_sysfs_text(trigger_path, str(restore_trigger))

            updated = self._read_led_status_unlocked()
            updated["action"] = normalized_action
            return updated

    def normalize(self, data: Dict[str, Any]) -> Dict[str, Any]:
        hashrate_ths = pick_first(data, ["hashRate", "hashRate_1m", "hashRateavg"])
        temp_c = pick_first(data, ["temp", "boardtemp", "boardTemp"])
        vr_temp_c = pick_first(data, ["vrTemp", "vrtemp"])
        power_w = pick_first(data, ["power"])
        hashrate_numeric = to_float(hashrate_ths)
        power_numeric = to_float(power_w)
        efficiency_j_th: Optional[float] = None
        if hashrate_numeric is not None and power_numeric is not None and hashrate_numeric > 0:
            efficiency_j_th = round(power_numeric / hashrate_numeric, 3)
        model = pick_first(data, ["deviceModel", "boardVersion", "ASICModel", "asicmodel"])
        hostname = pick_first(data, ["hostname", "hostip"])
        device_type = self.infer_device_type(data, model=model, hostname=hostname)

        return {
            "hostname": hostname,
            "ip": pick_first(data, ["hostip"]),
            "mac": pick_first(data, ["macAddr", "mac"]),
            "model": model,
            "device_type": device_type,
            "firmware": pick_first(data, ["version", "minerversion"]),
            "hashrate_ths": hashrate_ths,
            "temp_c": temp_c,
            "vr_temp_c": vr_temp_c,
            "power_w": power_w,
            "power_efficiency_j_th": efficiency_j_th,
            "fanspeed": pick_first(data, ["fanspeed", "fanspeedrpm", "fanrpm"]),
            "shares_accepted": pick_first(data, ["sharesAccepted"]),
            "shares_rejected": pick_first(data, ["sharesRejected"]),
            "best_diff": pick_first(data, ["bestDiff", "bestSessionDiff"]),
            "uptime_seconds": pick_first(data, ["uptimeSeconds"]),
            "wifi_status": pick_first(data, ["wifiStatus"]),
            "wifi_rssi": pick_first(data, ["wifiRSSI"]),
            "stratum_url": pick_first(data, ["stratumURL"]),
            "stratum_port": pick_first(data, ["stratumPort"]),
        }

    def start_status_server(self) -> None:
        agent = self

        class HubHandler(BaseHTTPRequestHandler):
            def _send_json(self, payload: Dict[str, Any], status: int = 200) -> None:
                body = json.dumps(payload, default=str).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.end_headers()
                try:
                    self.wfile.write(body)
                except (ConnectionResetError, BrokenPipeError):
                    # Client closed the connection before we finished (e.g. app timeout, navigation). Normal; no traceback.
                    pass

            def _send_text(self, body: str, status: int = 200, filename: Optional[str] = None) -> None:
                data = body.encode("utf-8", errors="replace")
                self.send_response(status)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Access-Control-Allow-Origin", "*")
                if filename:
                    self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.end_headers()
                try:
                    self.wfile.write(data)
                except (ConnectionResetError, BrokenPipeError):
                    pass

            def _send_html(self, body: str, status: int = 200) -> None:
                data = body.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, _format: str, *_args: Any) -> None:
                return

            def do_OPTIONS(self) -> None:  # noqa: N802
                self.send_response(204)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.end_headers()

            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                snapshot = agent.state.snapshot()
                live_pi = agent.get_pi_telemetry()
                current_step = agent.get_current_step()
                ts_status = tailscale_setup.status()
                wifi_status = agent.get_wifi_status()
                led_status = agent.get_led_status()
                feature_gates = agent.feature_gates_status()
                local_clock = agent.local_clock_status()
                fleet_manager = agent.get_fleet_manager_status()
                paired_status = agent.is_paired_status(wifi_status, ts_status)
                setup_complete = agent.is_setup_complete(wifi_status, ts_status)

                status = {
                    "ok": True,
                    "agentId": agent.agent_id,
                    "piHostname": agent.pi_hostname,
                    "agentVersion": AGENT_VERSION,
                    "hubVersion": AGENT_VERSION,
                    "statusApiVersion": STATUS_API_VERSION,
                    "bitaxeHost": agent.bitaxe_host,
                    "isPaired": paired_status,
                    "setupComplete": setup_complete,
                    "pollSeconds": agent.poll_seconds,
                    "statusHttpPort": agent.status_http_port,
                    "pairedDeviceType": agent.paired_device_type or None,
                    "pairedMinerMac": agent.paired_miner_mac or None,
                    "pairedMinerHostname": agent.paired_miner_hostname or None,
                    "endpoints": agent.endpoints,
                    "piTelemetry": live_pi,
                    "tailscale": ts_status,
                    "wifi": wifi_status,
                    "statusLed": led_status,
                    "featureGates": feature_gates,
                    "hubClock": local_clock,
                    "fleetManager": fleet_manager,
                    "currentStep": current_step,
                    **snapshot,
                }

                if parsed.path in ["/api/status", "/healthz"]:
                    self._send_json(status)
                    return

                if parsed.path == "/api/config":
                    self._send_json({"ok": True, "config": agent.get_runtime_config()})
                    return

                if parsed.path == "/api/discover":
                    query = parse_qs(parsed.query)
                    cidr = (query.get("cidr") or [None])[0]
                    try:
                        self._send_json(agent.discover_bitaxe_devices(cidr=cidr))
                    except Exception as exc:  # pylint: disable=broad-except
                        self._send_json({"ok": False, "error": str(exc)}, status=500)
                    return

                if parsed.path == "/api/self-check":
                    self._send_json(agent.self_check())
                    return

                if parsed.path == "/api/diagnostics":
                    self._send_json(tailscale_setup.diagnose())
                    return

                if parsed.path == "/api/logs":
                    query = parse_qs(parsed.query)
                    limit = parse_int((query.get("limit") or ["100"])[0], default=100, minimum=1, maximum=1000)
                    level_filter = (query.get("level") or [None])[0]
                    logs_result = agent.get_internal_logs(limit=limit, level_filter=level_filter)
                    self._send_json({
                        "ok": True,
                        "count": logs_result.get("total", 0),
                        "entries": logs_result.get("entries", []),
                        "persistentHistory": True,
                        "storage": {
                            "path": agent.persistent_log_path,
                            "maxBytes": agent.persistent_log_max_bytes,
                            "backupCount": agent.persistent_log_backup_count,
                        },
                    })
                    return

                if parsed.path == "/api/device/logs/bundle.txt":
                    query = parse_qs(parsed.query)
                    limit = parse_int((query.get("limit") or ["300"])[0], default=300, minimum=50, maximum=1200)
                    compact_raw = (query.get("compact") or [None])[0]
                    compact = parse_bool(compact_raw, default=agent.log_bundle_compact_default)
                    bundle = agent.build_logs_bundle_text(per_stream_limit=limit, compact=compact)
                    filename = f"hashwatcher-hub-logs-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%SZ')}.txt"
                    self._send_text(bundle, status=200, filename=filename)
                    return

                if parsed.path == "/api/device/logs":
                    query = parse_qs(parsed.query)
                    stream = str((query.get("stream") or ["hub"])[0]).strip().lower()
                    limit = parse_int((query.get("limit") or ["200"])[0], default=200, minimum=20, maximum=1000)
                    result = agent.get_device_logs(stream=stream, limit=limit)
                    http_status = 200 if result.get("ok") else 503
                    self._send_json(result, status=http_status)
                    return

                if parsed.path == "/api/tailscale/status":
                    self._send_json({"ok": True, **tailscale_setup.status()})
                    return

                if parsed.path == "/api/tailscale/setup-status":
                    self._send_json(tailscale_setup.get_setup_status())
                    return

                if parsed.path == "/api/wifi/status":
                    self._send_json({"ok": True, **agent.get_wifi_status()})
                    return

                if parsed.path == "/api/led/status":
                    http_status = 200 if led_status.get("ok") else 503
                    self._send_json(led_status, status=http_status)
                    return

                if parsed.path == "/api/features/status":
                    self._send_json({
                        "ok": True,
                        "featureGates": agent.feature_gates_status(),
                        "clock": agent.local_clock_status(),
                    })
                    return

                if parsed.path == "/api/fleet/status":
                    self._send_json({"ok": True, "fleetManager": agent.get_fleet_manager_status()})
                    return

                if parsed.path == "/api/miner/data":
                    result = agent.fetch_paired_miner()
                    if result:
                        normalized = agent.normalize(result["data"])
                        self._send_json({"ok": True, "raw": result["data"], "normalized": normalized})
                    else:
                        self._send_json({"ok": False, "error": "No paired miner or miner unreachable"}, status=503)
                    return

                if parsed.path.endswith(".png") and "/" not in parsed.path.lstrip("/"):
                    img_name = parsed.path.lstrip("/")
                    img_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), img_name)
                    if os.path.isfile(img_path):
                        with open(img_path, "rb") as img_f:
                            img_data = img_f.read()
                        self.send_response(200)
                        self.send_header("Content-Type", "image/png")
                        self.send_header("Content-Length", str(len(img_data)))
                        self.send_header("Cache-Control", "public, max-age=86400")
                        self.end_headers()
                        self.wfile.write(img_data)
                    else:
                        self.send_response(404)
                        self.end_headers()
                    return

                if parsed.path in ["/", "/index.html"]:
                    pi_mem = live_pi.get("memory", {}) if isinstance(live_pi, dict) else {}
                    pi_disk = live_pi.get("diskRoot", {}) if isinstance(live_pi, dict) else {}
                    wifi_ok = wifi_status.get("connected", False)
                    wifi_ssid = wifi_status.get("ssid") or "-"
                    wifi_ip = wifi_status.get("ip") or "-"
                    wifi_signal = wifi_status.get("signalDbm")
                    ts_online = ts_status.get("online", False)
                    ts_authenticated = ts_status.get("authenticated", False)
                    logs_bundle_supported = is_version_at_least(AGENT_VERSION, "1.0.9")
                    setup_done = wifi_ok and ts_authenticated
                    led_supported = led_status.get("supported", False)
                    led_name = led_status.get("name") or "ACT"
                    led_mode_label = led_status.get("modeLabel") or "Unavailable"
                    led_trigger = led_status.get("currentTrigger") or "-"
                    led_default_trigger = led_status.get("defaultTrigger") or "-"
                    led_summary = (
                        f"{led_name} LED is in {led_mode_label.lower()} mode "
                        f"(trigger: {led_trigger}, default: {led_default_trigger})."
                        if led_supported
                        else str(led_status.get("error") or "Pi activity LED control is unavailable on this device.")
                    )
                    led_action_status = json.dumps(led_status, indent=2) if led_supported else led_summary

                    soc_c = live_pi.get("socTempC")
                    soc_hot = False
                    if soc_c is not None:
                        soc_f = int(soc_c * 9 / 5 + 32)
                        soc_display = f"{int(soc_c)} °C / {soc_f} °F"
                        soc_hot = soc_c >= 65
                    else:
                        soc_display = "-"

                    uptime_s = live_pi.get("agentUptimeSeconds")
                    if uptime_s is not None:
                        _s = int(uptime_s)
                        _d, _s = divmod(_s, 86400)
                        _h, _s = divmod(_s, 3600)
                        _m, _ = divmod(_s, 60)
                        uptime_display = f"{_d}d {_h}h {_m}m" if _d > 0 else f"{_h}h {_m}m" if _h > 0 else f"{_m}m"
                    else:
                        uptime_display = "-"

                    mem_used = pi_mem.get("usedPercent", "-")
                    total_mb = live_pi.get("memory", {}).get("totalMB")
                    avail_mb = live_pi.get("memory", {}).get("availableMB")
                    if total_mb is not None and avail_mb is not None:
                        used_mb = total_mb - avail_mb
                        mem_display = f"{int(used_mb)} / {int(total_mb)} MB"
                    else:
                        mem_display = "-"

                    disk_total = pi_disk.get("totalGB") or pi_disk.get("totalGb")
                    disk_free = pi_disk.get("freeGB") or pi_disk.get("freeGb")
                    if disk_total is not None and disk_free is not None:
                        disk_used_val = round(disk_total - disk_free, 1)
                        disk_display = f"{disk_used_val} / {round(disk_total, 1)} GB"
                    else:
                        disk_display = "-"
                    disk_pct = pi_disk.get("usedPercent", "-")

                    ts_ip = str(ts_status.get("ip", "-"))
                    routes = ts_status.get("advertisedRoutes", []) or []
                    if not routes and agent.user_subnet_cidr:
                        routes = [agent.user_subnet_cidr]
                    ts_routes = ", ".join(routes or ["-"])
                    ts_status_label = "Online" if ts_online else "Offline"
                    ts_badge_class = "badge-green" if ts_online else "badge-red"

                    key_expired = ts_status.get("keyExpired", False)
                    key_expiring = ts_status.get("keyExpiringSoon", False)
                    expiry_banner = ""
                    if key_expired:
                        expiry_banner = '<div class="alert alert-red">Tailscale key expired &mdash; remote access unavailable. <a href="https://login.tailscale.com/admin/machines" target="_blank">Reauthorize &rarr;</a></div>'
                    elif key_expiring:
                        expiry_banner = '<div class="alert alert-yellow">Key expiring soon. <a href="https://login.tailscale.com/admin/machines" target="_blank">Disable key expiry &rarr;</a></div>'

                    page_html = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{agent.pi_hostname} &mdash; HashWatcher Hub Pi</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'Segoe UI', sans-serif; background: #000; color: #fff; margin: 0; padding: 16px; line-height: 1.5; min-height: 100vh; overflow-x: hidden; }}
    .bg-canvas {{ position: fixed; top: 0; left: 0; width: 100%; height: 100%; z-index: 0; pointer-events: none; overflow: hidden; }}
    .bg-orb {{ position: absolute; border-radius: 50%; filter: blur(80px); opacity: 0.35; animation: drift 20s ease-in-out infinite alternate; }}
    .bg-orb:nth-child(1) {{ width: 400px; height: 400px; background: #00cc66; top: -10%; left: -10%; animation-duration: 22s; }}
    .bg-orb:nth-child(2) {{ width: 350px; height: 350px; background: #33e680; top: 40%; right: -15%; animation-duration: 18s; animation-delay: -5s; }}
    .bg-orb:nth-child(3) {{ width: 300px; height: 300px; background: #004d26; bottom: -5%; left: 20%; animation-duration: 25s; animation-delay: -10s; }}
    @keyframes drift {{ 0% {{ transform: translate(0,0) scale(1); }} 33% {{ transform: translate(30px,-40px) scale(1.05); }} 66% {{ transform: translate(-20px,20px) scale(0.95); }} 100% {{ transform: translate(10px,-10px) scale(1.02); }} }}
    .container {{ max-width: 640px; margin: 0 auto; position: relative; z-index: 1; }}
    .card {{ background: rgba(28,28,30,0.85); backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px); border: 1px solid rgba(255,255,255,0.12); border-radius: 14px; padding: 20px; margin-bottom: 16px; }}
    h2 {{ margin: 0 0 12px; font-size: 1.4em; font-weight: 700; }}
    h3 {{ margin: 16px 0 8px; font-size: 1.1em; font-weight: 600; }}
    .grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }}
    .grid > div {{ background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.06); border-radius: 10px; padding: 10px 12px; font-size: 0.9em; color: rgba(255,255,255,0.55); }}
    .grid > div strong {{ color: #fff; }}
    .muted {{ color: rgba(255,255,255,0.55); }}
    code {{ color: #00cc66; background: rgba(0,204,102,0.1); padding: 1px 5px; border-radius: 3px; font-size: 0.9em; }}
    a {{ color: #33e680; text-decoration: none; }}
    a:hover {{ text-decoration: underline; color: #66ff99; }}
    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; font-weight: 600; }}
    .badge-green {{ background: rgba(0,204,102,0.15); color: #33e680; }}
    .badge-red {{ background: rgba(239,68,68,0.15); color: #fca5a5; }}
    .badge-yellow {{ background: rgba(245,158,11,0.15); color: #fde68a; }}
    .alert {{ padding: 12px 16px; border-radius: 12px; margin: 12px 0; font-size: 0.9em; }}
    .alert-red {{ background: rgba(239,68,68,0.1); border: 1px solid rgba(239,68,68,0.3); color: #fca5a5; }}
    .alert-red a {{ color: #fca5a5; text-decoration: underline; }}
    .alert-yellow {{ background: rgba(245,158,11,0.1); border: 1px solid rgba(245,158,11,0.3); color: #fde68a; }}
    .alert-yellow a {{ color: #fde68a; text-decoration: underline; }}
    .info-row {{ display: flex; justify-content: space-between; padding: 6px 0; border-bottom: 1px solid rgba(255,255,255,0.06); }}
    .info-row:last-child {{ border-bottom: none; }}
    .info-label {{ color: rgba(255,255,255,0.5); }}
    .divider {{ border: none; border-top: 1px solid rgba(255,255,255,0.1); margin: 16px 0; }}
    .btn {{ display: inline-block; background: #00cc66; color: #000; padding: 8px 16px; border-radius: 10px; font-weight: 600; font-size: 0.9em; text-decoration: none; border: none; cursor: pointer; }}
    .btn:hover {{ background: #33e680; text-decoration: none; }}
    .btn-warn {{ background: rgba(239,68,68,0.15); color: #fca5a5; border: 1px solid rgba(239,68,68,0.3); padding: 8px 14px; border-radius: 10px; font-weight: 600; font-size: 0.85em; cursor: pointer; }}
    .btn-warn:hover {{ background: rgba(239,68,68,0.25); }}
    .btn-secondary {{ background: rgba(255,255,255,0.08); color: rgba(255,255,255,0.8); border: 1px solid rgba(255,255,255,0.12); padding: 8px 14px; border-radius: 10px; font-weight: 600; font-size: 0.85em; cursor: pointer; }}
    .btn-secondary:hover {{ background: rgba(255,255,255,0.12); }}
    .input {{ background: rgba(255,255,255,0.06); border: 1px solid rgba(255,255,255,0.15); color: #fff; border-radius: 8px; padding: 6px 10px; font-size: 0.85em; }}
    .input:focus {{ outline: 1px solid rgba(51,230,128,0.55); border-color: rgba(51,230,128,0.55); }}
    pre {{ white-space: pre-wrap; background: rgba(0,0,0,0.4); border: 1px solid rgba(255,255,255,0.08); border-radius: 10px; padding: 10px; font-size: 0.85em; color: rgba(255,255,255,0.7); }}
    .log-pre-compact {{ max-height: 240px; overflow: auto; }}
    details.collapsible {{ margin-top: 10px; }}
    details.collapsible > summary {{ cursor: pointer; color: rgba(255,255,255,0.8); font-size: 0.85em; font-weight: 600; list-style: none; }}
    details.collapsible > summary::-webkit-details-marker {{ display: none; }}
    details.collapsible > summary::before {{ content: '>'; display: inline-block; margin-right: 8px; color: rgba(51,230,128,0.8); }}
    details.collapsible[open] > summary::before {{ content: 'v'; }}
    .brand-header {{ display: flex; align-items: center; gap: 10px; margin-bottom: 16px; font-size: 1.3em; font-weight: 700; }}
    .setup-card {{ background: rgba(0,204,102,0.08); border: 1px solid rgba(0,204,102,0.25); border-radius: 14px; padding: 20px; margin-bottom: 16px; text-align: center; }}
    .setup-card h3 {{ margin: 0 0 8px; color: #33e680; }}
    .setup-card p {{ margin: 0 0 14px; color: rgba(255,255,255,0.6); font-size: 0.9em; }}
    .mini-table {{ width: 100%; border-collapse: collapse; font-size: 0.8em; }}
    .mini-table th, .mini-table td {{ text-align: left; padding: 7px 6px; border-bottom: 1px solid rgba(255,255,255,0.08); vertical-align: top; }}
    .mini-table th {{ color: rgba(255,255,255,0.58); font-weight: 600; font-size: 0.8em; }}
    .mini-table tr:last-child td {{ border-bottom: none; }}
  </style>
</head>
<body>
  <div class="bg-canvas"><div class="bg-orb"></div><div class="bg-orb"></div><div class="bg-orb"></div></div>
  <div class="container">

    <div class="brand-header">
      <img src="/icon.png" alt="HashWatcher" style="width:36px;height:36px;border-radius:8px;">
      <span>HashWatcher Hub Pi</span>
      <span class="muted" style="font-size:0.72em;font-weight:600;">v{AGENT_VERSION} &middot; API {STATUS_API_VERSION}</span>
      <a href="https://x.com/HashWatcher" target="_blank" title="Follow @HashWatcher on X" style="margin-left:auto;display:inline-flex;align-items:center;color:rgba(255,255,255,0.6);transition:color 0.2s;">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg>
      </a>
    </div>

    {'<div class="setup-card"><h3>Finish Setup in the HashWatcher App</h3><p>Open the HashWatcher app on your iPhone to configure Wi-Fi, Tailscale, then you&#39;re done. No extra miner setup required. All of your existing IPs remain the same</p><a class="btn" href="https://www.HashWatcher.app" target="_blank">Download Free at HashWatcher.app</a></div>' if not setup_done else ''}

    <div class="card">
      <h2>Status <span class="badge {ts_badge_class}">{ts_status_label}</span></h2>

      <div class="info-row">
        <span class="info-label">Wi-Fi</span>
        <span><code>{'connected' if wifi_ok else 'disconnected'}</code>{' &middot; ' + wifi_ssid if wifi_ok else ''}</span>
      </div>
      <div class="info-row">
        <span class="info-label">Local IP</span>
        <code>{wifi_ip if wifi_ok else '-'}</code>
      </div>
      {f'<div class="info-row"><span class="info-label">Signal</span><code>{wifi_signal} dBm</code></div>' if wifi_signal is not None else ''}
      <div class="info-row">
        <span class="info-label">Tailscale IP</span>
        <code>{ts_ip}</code>
      </div>
      <div class="info-row">
        <span class="info-label">Routes</span>
        <code>{ts_routes}</code>
      </div>
      <div class="info-row">
        <span class="info-label">Hub Version</span>
        <code>v{AGENT_VERSION} &middot; API {STATUS_API_VERSION}</code>
      </div>
      {expiry_banner}

      <hr class="divider">
      <h3 style="margin:8px 0 10px;">System</h3>
      <div class="grid">
        <div>CPU <strong>{live_pi.get("cpuPercent", "-")}%</strong></div>
        <div>SoC Temp <strong style="{'color:#ff453a;' if soc_hot else ''}">{soc_display}</strong></div>
        <div>Load <strong>{live_pi.get("loadAvg1m", "-")}</strong> / {live_pi.get("loadAvg5m", "-")} / {live_pi.get("loadAvg15m", "-")}</div>
        <div>Memory <strong>{mem_display}</strong> ({mem_used}%)</div>
        <div>Disk <strong>{disk_display}</strong> ({disk_pct}%)</div>
        <div>Uptime <strong>{uptime_display}</strong></div>
      </div>
    </div>

    <div class="card">
      <h3 style="margin:0 0 12px;">Tailscale</h3>
      <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
        <button class="{'btn' if ts_online else 'btn-secondary'}" id="tsToggleBtn" onclick="toggleTailscale()" style="min-width:140px;">
          {'Turn Off' if ts_online else 'Turn On'}
        </button>
        <span class="muted" style="font-size:0.85em;" id="tsToggleStatus">{'Connected' if ts_online else 'Off'}</span>
      </div>
    </div>

    <div class="card">
      <h3 style="margin:0 0 12px;">Status LED</h3>
      <p class="muted" id="ledStatusSummary" style="margin:0 0 12px;font-size:0.9em;">{led_summary}</p>
      <div style="display:flex;gap:8px;flex-wrap:wrap;">
        <button class="btn-secondary" onclick="runLedAction('on')" {'disabled' if not led_supported else ''}>Turn On</button>
        <button class="btn-secondary" onclick="runLedAction('off')" {'disabled' if not led_supported else ''}>Turn Off</button>
        <button class="btn-secondary" onclick="runLedAction('blink')" {'disabled' if not led_supported else ''}>Blink</button>
        <button class="btn-secondary" onclick="runLedAction('restore-default')" {'disabled' if not led_supported else ''}>Restore Default</button>
      </div>
      <details class="collapsible" id="ledActionDetails">
        <summary>Show LED action logs</summary>
        <pre id="ledActionStatus" class="log-pre-compact">{led_action_status}</pre>
      </details>
    </div>

    <div class="card">
      <h3 style="margin:0 0 12px;">Maintenance</h3>
      <div style="display:flex;gap:8px;flex-wrap:wrap;">
        <button class="btn-secondary" onclick="runSelfCheck()">Run Self-Check</button>
        <button class="btn-warn" onclick="hardResetWifi()">Disconnect + Reset Wi-Fi</button>
        <button class="btn-warn" onclick="factoryReset()">Factory Reset</button>
      </div>
      <details class="collapsible" id="maintenanceActionDetails">
        <summary>Show maintenance action logs</summary>
        <pre id="actionStatus" class="log-pre-compact">No actions run yet.</pre>
      </details>
      <p class="muted" style="font-size:0.8em;margin:10px 0 0;">API: <a href="/api/status">/api/status</a> &middot; <a href="/api/wifi/status">/api/wifi/status</a> &middot; <a href="/api/led/status">/api/led/status</a> &middot; <a href="/api/self-check">/api/self-check</a> &middot; <a href="/api/device/logs?stream=hub&limit=200">/api/device/logs</a></p>
    </div>

    <div class="card">
      <h3 style="margin:0 0 8px;">Device Logs</h3>
      <p class="muted" style="margin:0 0 10px;font-size:0.85em;">Use local journal logs to troubleshoot Wi-Fi or Tailscale disconnects.</p>
      <details class="collapsible" id="deviceLogsDetails">
        <summary>Show device logs</summary>
        <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin:8px 0;">
          <label for="logStream" class="muted" style="font-size:0.85em;">Stream</label>
          <select id="logStream" class="input">
            <option value="hub">Hub Service</option>
            <option value="tailscale">Tailscale</option>
            <option value="wifi">Wi-Fi / Network</option>
            <option value="kernel">Kernel</option>
            <option value="all">All Streams</option>
          </select>
          <label for="logLimit" class="muted" style="font-size:0.85em;">Lines</label>
          <input id="logLimit" class="input" type="number" min="20" max="1000" value="200" style="width:90px;" />
          <button class="btn-secondary" id="refreshLogsBtn" onclick="loadDeviceLogs()">Refresh</button>
          <label class="muted" style="font-size:0.82em;display:inline-flex;align-items:center;gap:6px;">
            <input id="autoRefreshLogs" type="checkbox" checked />
            Auto-refresh
          </label>
          {'<a class="btn-secondary" id="downloadLogsBundleBtn" href="/api/device/logs/bundle.txt?limit=300&compact=1" target="_blank" rel="noopener">Download Bundle (.txt)</a>' if logs_bundle_supported else ''}
        </div>
        <p class="muted" id="deviceLogsMeta" style="margin:0 0 8px;font-size:0.78em;">Expand this panel and click refresh to load logs.</p>
        <pre id="deviceLogsOutput" class="log-pre-compact">Logs are collapsed. Expand this panel to load logs.</pre>
      </details>
    </div>

    <div class="card">
      <h3 style="margin:0 0 12px;">Software Update</h3>
      <p class="muted" style="margin:0 0 12px;font-size:0.85em;">Current version: <strong>{AGENT_VERSION}</strong></p>
      <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
        <button class="btn-secondary" id="checkUpdateBtn" onclick="checkForUpdate()">Check for Update</button>
        <button class="btn" id="applyUpdateBtn" onclick="applyUpdate()" style="display:none;">Install Update</button>
      </div>
      <pre id="updateStatus" style="display:none;"></pre>
    </div>

  </div>

  <script>
    let tsOn = {'true' if ts_online else 'false'};
    let deviceLogsLoading = false;
    async function toggleTailscale() {{
      const btn = document.getElementById('tsToggleBtn');
      const st = document.getElementById('tsToggleStatus');
      const endpoint = tsOn ? '/api/tailscale/down' : '/api/tailscale/up';
      btn.disabled = true;
      st.textContent = tsOn ? 'Turning off...' : 'Turning on...';
      try {{
        const r = await fetch(endpoint, {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }}, body: '{{}}' }});
        const p = await r.json();
        if (p.ok) {{
          tsOn = !tsOn;
          btn.textContent = tsOn ? 'Turn Off' : 'Turn On';
          btn.className = tsOn ? 'btn' : 'btn-secondary';
          st.textContent = tsOn ? 'Connected' : 'Off';
        }} else {{
          st.textContent = 'Error: ' + (p.error || 'unknown');
        }}
      }} catch (e) {{
        st.textContent = 'Request failed';
      }}
	      btn.disabled = false;
	    }}
	    async function runSelfCheck() {{
	      const el = document.getElementById('actionStatus');
	      el.textContent = 'Running self-check...';
      const r = await fetch('/api/self-check');
      el.textContent = JSON.stringify(await r.json(), null, 2);
    }}
    async function runLedAction(action) {{
      const statusEl = document.getElementById('ledActionStatus');
      const summaryEl = document.getElementById('ledStatusSummary');
      statusEl.textContent = 'Applying LED action: ' + action + '...';
      try {{
        const r = await fetch('/api/led/control', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify({{ action }})
        }});
        const p = await r.json();
        statusEl.textContent = JSON.stringify(p, null, 2);
        if (p.supported) {{
          summaryEl.textContent = `${{p.name || 'ACT'}} LED is in ${{(p.modeLabel || 'Unknown').toLowerCase()}} mode (trigger: ${{p.currentTrigger || '-'}}, default: ${{p.defaultTrigger || '-'}}).`;
        }} else {{
          summaryEl.textContent = p.error || 'LED control unavailable.';
        }}
      }} catch (e) {{
        statusEl.textContent = 'LED request failed.';
      }}
    }}
	    async function hardResetWifi() {{
	      if (!confirm('Disconnect + Reset Wi-Fi\\n\\nThis will force disconnect Wi-Fi now, clear saved Wi-Fi credentials, and reboot the hub. Tailscale config is kept.\\n\\nContinue?')) return;
	      const el = document.getElementById('actionStatus');
	      el.textContent = 'Disconnecting and resetting Wi-Fi...';
	      try {{
	        const r = await fetch('/api/reset-wifi-hard', {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }}, body: '{{}}' }});
	        const p = await r.json();
	        el.textContent = JSON.stringify(p, null, 2);
	      }} catch (e) {{
	        el.textContent = 'Disconnect/reset sent. Connection loss is expected.';
	      }}
	    }}
    async function factoryReset() {{
      if (!confirm('Factory Reset\\n\\nThis will clear ALL config, remove the Tailscale node key, erase Wi-Fi, clear pairing, and restart the hub.\\n\\nContinue?')) return;
      const el = document.getElementById('actionStatus');
      el.textContent = 'Factory resetting...';
      try {{
        const r = await fetch('/api/factory-reset', {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }}, body: '{{}}' }});
        const p = await r.json();
        el.textContent = JSON.stringify(p, null, 2);
        if (p.ok) el.textContent += '\\n\\nHub is restarting. Reconnect via the HashWatcher app.';
      }} catch (e) {{
        el.textContent = 'Factory reset sent. Connection lost (expected).\\nReconnect via the HashWatcher app.';
      }}
    }}
    async function loadDeviceLogs() {{
      if (deviceLogsLoading) return;
      const detailsEl = document.getElementById('deviceLogsDetails');
      if (detailsEl && !detailsEl.open) return;
      const outputEl = document.getElementById('deviceLogsOutput');
      const metaEl = document.getElementById('deviceLogsMeta');
      const streamEl = document.getElementById('logStream');
      const limitEl = document.getElementById('logLimit');
      const btn = document.getElementById('refreshLogsBtn');
      const bundleBtn = document.getElementById('downloadLogsBundleBtn');
      if (!outputEl || !metaEl || !streamEl || !limitEl || !btn) return;

      let limit = parseInt(limitEl.value || '200', 10);
      if (Number.isNaN(limit)) limit = 200;
      limit = Math.max(20, Math.min(1000, limit));
      limitEl.value = String(limit);
      const stream = streamEl.value || 'hub';
      if (bundleBtn) {{
        bundleBtn.href = '/api/device/logs/bundle.txt?limit=' + encodeURIComponent(String(limit)) + '&compact=1';
      }}

      deviceLogsLoading = true;
      btn.disabled = true;
      metaEl.textContent = 'Loading logs...';
      try {{
        const r = await fetch('/api/device/logs?stream=' + encodeURIComponent(stream) + '&limit=' + encodeURIComponent(String(limit)));
        const p = await r.json();
        if (p.ok) {{
          outputEl.textContent = p.logs || 'No logs returned.';
          let meta = (p.streamLabel || stream) + ' \u00b7 ' + (p.linesReturned || 0) + ' lines';
          if (p.generatedAtIso) meta += ' \u00b7 ' + p.generatedAtIso;
          if (p.errors && p.errors.length) meta += ' \u00b7 warnings: ' + p.errors.length;
          metaEl.textContent = meta;
        }} else {{
          outputEl.textContent = 'Failed to load logs.';
          metaEl.textContent = 'Error: ' + (p.error || 'unknown');
        }}
      }} catch (e) {{
        outputEl.textContent = 'Failed to load logs.';
        metaEl.textContent = 'Request failed.';
      }}
      btn.disabled = false;
      deviceLogsLoading = false;
    }}
    const deviceLogsDetailsEl = document.getElementById('deviceLogsDetails');
    if (deviceLogsDetailsEl) {{
      deviceLogsDetailsEl.addEventListener('toggle', () => {{
        if (deviceLogsDetailsEl.open) loadDeviceLogs();
      }});
    }}
    const logStreamEl = document.getElementById('logStream');
    if (logStreamEl) logStreamEl.addEventListener('change', loadDeviceLogs);
    const logLimitEl = document.getElementById('logLimit');
    if (logLimitEl) logLimitEl.addEventListener('change', loadDeviceLogs);
    setInterval(() => {{
      const autoEl = document.getElementById('autoRefreshLogs');
      const detailsEl = document.getElementById('deviceLogsDetails');
      if (detailsEl && detailsEl.open && autoEl && autoEl.checked) loadDeviceLogs();
    }}, 10000);
    async function checkForUpdate() {{
      const btn = document.getElementById('checkUpdateBtn');
      const applyBtn = document.getElementById('applyUpdateBtn');
      const el = document.getElementById('updateStatus');
      btn.disabled = true;
      btn.textContent = 'Checking...';
      el.style.display = 'block';
      el.textContent = 'Checking GitHub for updates...';
      try {{
        const r = await fetch('/api/update/check');
        const p = await r.json();
        if (!p.ok) {{
          el.textContent = 'Error: ' + (p.error || 'unknown');
        }} else if (p.updateAvailable) {{
          el.textContent = 'Update available: v' + p.latestVersion + '\\nPublished: ' + (p.publishedAt || 'unknown');
          if (p.debAsset) el.textContent += '\\nPackage: ' + p.debAsset.name + ' (' + Math.round(p.debAsset.size / 1024) + ' KB)';
          applyBtn.style.display = 'inline-block';
        }} else {{
          el.textContent = 'You are on the latest version (v' + p.currentVersion + ')';
          applyBtn.style.display = 'none';
        }}
      }} catch (e) {{
        el.textContent = 'Failed to check for updates.';
      }}
      btn.disabled = false;
      btn.textContent = 'Check for Update';
    }}
    async function applyUpdate() {{
      if (!confirm('Install Update\\n\\nThis will download and install the latest version from GitHub. The hub will restart afterward.\\n\\nContinue?')) return;
      const btn = document.getElementById('applyUpdateBtn');
      const el = document.getElementById('updateStatus');
      btn.disabled = true;
      btn.textContent = 'Installing...';
      el.textContent = 'Downloading and installing update...';
      try {{
        const r = await fetch('/api/update/apply', {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }}, body: '{{}}' }});
        const p = await r.json();
        el.textContent = JSON.stringify(p, null, 2);
        if (p.ok) {{
          el.textContent += '\\n\\nHub is restarting with the new version.';
          btn.style.display = 'none';
        }}
      }} catch (e) {{
        el.textContent = 'Update may have been applied. Connection lost (expected during restart).';
      }}
      btn.disabled = false;
      btn.textContent = 'Install Update';
    }}
  </script>
</body>
</html>
""".strip()
                    self._send_html(page_html)
                    return

                if parsed.path == "/api/update/check":
                    result = agent.check_for_update()
                    self._send_json(result)
                    return

                if parsed.path == "/api/update/status":
                    self._send_json({"ok": True, **agent.get_update_progress()})
                    return

                self._send_json({"ok": False, "error": "Not found"}, status=404)

            def do_POST(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                allowed_post_paths = [
                    "/api/config",
                    "/api/device/reboot",
                    "/api/reset",
                    "/api/reset-wifi",
                    "/api/reset-wifi-hard",
                    "/api/factory-reset",
                    "/api/tailscale/setup",
                    "/api/tailscale/up",
                    "/api/tailscale/down",
                    "/api/tailscale/logout",
                    "/api/led/control",
                    "/api/features/experimental",
                    "/api/fleet/schedules",
                    "/api/fleet/inventory",
                    "/api/fleet/presets",
                    "/api/fleet/apply-preset",
                    "/api/canaan/fan-pid/start",
                    "/api/canaan/fan-pid/stop",
                    "/api/miner/proxy",
                    "/api/update/apply",
                    "/api/repair",
                    "/api/repair/tailscale",
                    "/api/repair/usb-gadget",
                ]

                if parsed.path not in allowed_post_paths:
                    self._send_json({"ok": False, "error": "Not found"}, status=404)
                    return

                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    payload = json.loads(raw.decode("utf-8")) if raw else {}
                    if not isinstance(payload, dict):
                        raise ValueError("Payload must be a JSON object")
                    if parsed.path == "/api/config":
                        config = agent.update_runtime_config(payload)
                        self._send_json({"ok": True, "config": config})
                        return
                    if parsed.path == "/api/device/reboot":
                        result = agent.reboot_hub()
                        self._send_json(result)
                        return
                    if parsed.path == "/api/reset":
                        config = agent.reset_pairing()
                        self._send_json({"ok": True, "config": config})
                        return
                    if parsed.path == "/api/factory-reset":
                        result = agent.factory_reset()
                        self._send_json(result)
                        return
                    if parsed.path == "/api/reset-wifi":
                        result = agent.reset_wifi()
                        self._send_json(result)
                        return
                    if parsed.path == "/api/reset-wifi-hard":
                        result = agent.disconnect_and_reset_wifi()
                        self._send_json(result)
                        return
                    if parsed.path == "/api/tailscale/setup":
                        auth_key = str(payload.get("authKey") or "")
                        subnet_cidr = str(payload.get("subnetCIDR") or "")
                        result = tailscale_setup.setup(auth_key=auth_key, subnet_cidr=subnet_cidr or None)
                        if result.get("ok") and subnet_cidr.strip():
                            with agent.config_lock:
                                agent.user_subnet_cidr = subnet_cidr.strip()
                                agent._persist_runtime_config()
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/tailscale/up":
                        result = tailscale_setup.up()
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/tailscale/down":
                        result = tailscale_setup.down()
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/tailscale/logout":
                        result = tailscale_setup.logout()
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/led/control":
                        action = str(payload.get("action") or "").strip()
                        blink_on_ms = payload.get("blinkOnMs", 250)
                        blink_off_ms = payload.get("blinkOffMs", 250)
                        result = agent.control_led(action, blink_on_ms=blink_on_ms, blink_off_ms=blink_off_ms)
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/features/experimental":
                        result = agent.set_experimental_features_enabled(payload.get("enabled"))
                        self._send_json(result, status=200 if result.get("ok") else 400)
                        return
                    if parsed.path == "/api/fleet/schedules":
                        result = agent.set_fleet_schedules(payload.get("schedules"))
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/fleet/inventory":
                        inventory_payload = payload.get("inventory")
                        result = agent.set_fleet_inventory(inventory_payload)
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/fleet/presets":
                        result = agent.set_fleet_preset_metadata(payload)
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/fleet/apply-preset":
                        result = agent.apply_fleet_preset_now(payload)
                        http_status = 200 if result.get("ok") or result.get("partial") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/canaan/fan-pid/start":
                        result = agent.start_canaan_fan_pid_program(payload)
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/canaan/fan-pid/stop":
                        result = agent.stop_canaan_fan_pid_program(payload)
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/update/apply":
                        result = agent.apply_update()
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/miner/proxy":
                        target_ip = str(payload.get("targetIp") or agent.bitaxe_host).strip()
                        miner_path = str(payload.get("path") or "/api/system/info").strip()
                        method = str(payload.get("method") or "GET").upper()
                        body_data = payload.get("body")
                        body_bytes = json.dumps(body_data).encode("utf-8") if body_data else None
                        result = agent.proxy_miner_request(target_ip, miner_path, method=method, body=body_bytes)
                        self._send_json(result)
                        return
                    if parsed.path == "/api/repair":
                        auth_key = str(payload.get("authKey") or "")
                        ts_result = tailscale_setup.repair_tailscale(auth_key=auth_key or None)
                        usb_result = tailscale_setup.repair_usb_gadget()
                        self._send_json({
                            "ok": ts_result.get("ok", False) and usb_result.get("ok", False),
                            "tailscale": ts_result,
                            "usbGadget": usb_result,
                        })
                        return
                    if parsed.path == "/api/repair/tailscale":
                        auth_key = str(payload.get("authKey") or "")
                        result = tailscale_setup.repair_tailscale(auth_key=auth_key or None)
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/repair/usb-gadget":
                        result = tailscale_setup.repair_usb_gadget()
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    self._send_json({"ok": False, "error": "Not found"}, status=404)
                except Exception as exc:  # pylint: disable=broad-except
                    self._send_json({"ok": False, "error": str(exc)}, status=400)

        class ReuseAddrHTTPServer(ThreadingHTTPServer):
            allow_reuse_address = True

        desired_port = self.status_http_port
        for candidate in range(desired_port, desired_port + 10):
            try:
                server = ReuseAddrHTTPServer((self.status_http_bind, candidate), HubHandler)
                self.status_http_port = candidate
                break
            except OSError:
                print(f"[{now_iso()}] Port {candidate} in use, trying {candidate + 1}...", flush=True)
        else:
            raise RuntimeError(f"Could not bind to any port in range {desired_port}-{desired_port + 9}")

        port_file = os.path.join(os.path.dirname(self.runtime_config_path), "runtime_port")
        try:
            with open(port_file, "w", encoding="utf-8") as pf:
                pf.write(str(self.status_http_port))
        except Exception as exc:
            print(f"[{now_iso()}] Could not write port file: {exc}", flush=True)

        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        if self.status_http_port != desired_port:
            self._log("server", f"Port {desired_port} was in use — bound to {self.status_http_port} instead.", level="warn")
            print(
                f"[{now_iso()}] Port {desired_port} was in use — bound to {self.status_http_port} instead.",
                flush=True,
            )
        print(
            f"Gateway server listening on {self.status_http_bind}:{self.status_http_port} ({self.pi_hostname})",
            flush=True,
        )

    def _self_heal_loop(self) -> None:
        """Background loop that periodically checks health and auto-repairs what it can.

        Runs every 60s. Auto-repairs USB gadget issues (no user input needed).
        Logs Tailscale issues but can't auto-fix without an auth key.
        """
        HEAL_INTERVAL = 60
        time.sleep(30)  # let services settle after boot
        while True:
            try:
                diag = tailscale_setup.diagnose()
                if diag.get("healthy"):
                    time.sleep(HEAL_INTERVAL)
                    continue

                for finding in diag.get("findings", []):
                    component = finding.get("component", "")
                    issue = finding.get("issue", "")
                    repairable = finding.get("autoRepairable", False)

                    if component == "usb_gadget" and repairable:
                        retry_key = f"{component}:{issue}:{finding.get('repairAction', '')}"
                        retry_after = self._self_heal_retry_after.get(retry_key, 0)
                        if retry_after > time.time():
                            continue

                        self._log("self-heal", f"Auto-repairing USB gadget: {issue}", level="info")
                        result = tailscale_setup.repair_usb_gadget()
                        if result.get("ok"):
                            self._self_heal_retry_after.pop(retry_key, None)
                            self._log("self-heal", f"USB gadget repair succeeded: {result.get('actionsTaken', [])}", level="info")
                        else:
                            self._self_heal_retry_after[retry_key] = time.time() + 3600
                            self._log("self-heal", f"USB gadget repair had errors: {result.get('errors', [])}", level="warn")

                    elif component == "tailscale":
                        self._log("self-heal", f"Tailscale issue detected: {issue} — {finding.get('message', '')}", level="warn")

            except Exception as exc:  # pylint: disable=broad-except
                self._log("self-heal", f"Self-heal loop error: {exc}")

            time.sleep(HEAL_INTERVAL)

    def _systemd_watchdog_loop(self) -> None:
        """Feed the systemd watchdog while the main poll loop is alive.

        Only pings WATCHDOG=1 when the main loop heartbeat is fresh, so a
        deadlocked poll loop stops the pings and systemd restarts the service
        (WatchdogSec in the unit file). Exits immediately when systemd did not
        configure a watchdog.
        """
        try:
            watchdog_usec = int(os.environ.get("WATCHDOG_USEC", "0"))
        except (TypeError, ValueError):
            watchdog_usec = 0
        if watchdog_usec <= 0:
            return
        interval_seconds = watchdog_usec / 1_000_000
        ping_seconds = max(1.0, interval_seconds / 3.0)
        # Fleet actions can legitimately hold the main loop for a while
        # (per-device HTTP timeouts), so only treat multi-minute silence as a hang.
        stale_after_seconds = max(interval_seconds * 2.0, 300.0)
        self._log(
            "watchdog",
            f"systemd watchdog active: interval={interval_seconds:.0f}s ping={ping_seconds:.0f}s staleAfter={stale_after_seconds:.0f}s",
            level="info",
        )
        stale_reported = False
        while True:
            age = time.time() - self._main_loop_heartbeat
            if age < stale_after_seconds:
                sd_notify("WATCHDOG=1")
                stale_reported = False
            elif not stale_reported:
                self._log(
                    "watchdog",
                    f"Main loop heartbeat stale for {int(age)}s; withholding watchdog ping so systemd can restart the agent.",
                    level="warn",
                )
                stale_reported = True
            time.sleep(ping_seconds)

    def _canaan_pid_loop(self) -> None:
        """Run Canaan fan PID independently from the hub poll interval."""
        while True:
            try:
                self._run_canaan_fan_pid_if_needed()
            except Exception as exc:  # pylint: disable=broad-except
                self._log("canaan-pid", f"PID background loop error: {exc}", level="warn")
            time.sleep(self.canaan_pid_loop_seconds)

    def run(self) -> None:
        print(
            f"Starting gateway agent='{self.agent_id}' piHostname='{self.pi_hostname}' bitaxe='{self.bitaxe_host}' poll={self.poll_seconds}s pidLoop={self.canaan_pid_loop_seconds}s",
            flush=True,
        )
        self.start_status_server()
        sd_notify("READY=1")

        threading.Thread(target=self._self_heal_loop, daemon=True, name="self-heal").start()
        threading.Thread(target=self._canaan_pid_loop, daemon=True, name="canaan-pid").start()
        threading.Thread(target=self._systemd_watchdog_loop, daemon=True, name="sd-watchdog").start()

        while True:
            self._main_loop_heartbeat = time.time()
            try:
                if self.paired and self.bitaxe_host.strip():
                    result = self.fetch_paired_miner()
                    if result:
                        normalized = self.normalize(result["data"])
                        self.state.set_poll_success({"normalized": normalized, "raw": result["data"]})
                    else:
                        msg = f"Miner {self.bitaxe_host} unreachable"
                        self.state.set_poll_error(msg)
                        self._log("poll", msg, level="warn")

                self._run_fleet_manager_if_needed()
            except Exception as exc:  # pylint: disable=broad-except
                self.state.set_poll_error(str(exc))
                self._run_fleet_manager_if_needed()
                self._log("poll", str(exc))

            time.sleep(self.poll_seconds)


if __name__ == "__main__":
    HubAgent().run()
