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
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv

import hashlib

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

HUB_AUTOMATION_PROFILES: Dict[str, Dict[str, Any]] = {
    "monitor_only": {
        "label": "Monitor Only",
        "description": "Track miner health and alerts only. No automatic recovery actions.",
        "autoRecoveryEnabled": False,
        "failureThreshold": None,
        "cooldownSeconds": None,
    },
    "auto_recover": {
        "label": "Auto Recover",
        "description": "Restart the paired miner when repeated network failures are detected.",
        "autoRecoveryEnabled": True,
        "failureThreshold": 3,
        "cooldownSeconds": 300,
    },
    "aggressive_recover": {
        "label": "Aggressive Recover",
        "description": "Faster recovery attempts for unstable networks with shorter trigger and cooldown windows.",
        "autoRecoveryEnabled": True,
        "failureThreshold": 2,
        "cooldownSeconds": 120,
    },
}
DEFAULT_AUTOMATION_MODE = "monitor_only"

BITAXE_TUNE_PRESETS: Dict[str, Dict[str, Any]] = {
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


def env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value else default


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

        self.pi_hostname = env_str("PI_HOSTNAME", "HashWatcherHub")
        self.bitaxe_host = env_str("BITAXE_HOST", "")
        self.bitaxe_scheme = env_str("BITAXE_SCHEME", "http")
        self.endpoints = parse_endpoints(env_str("BITAXE_ENDPOINTS", "/system/info,/api/system/info"))
        self.poll_seconds = max(5, env_int("POLL_SECONDS", 10))
        self.http_timeout_seconds = max(2, env_int("HTTP_TIMEOUT_SECONDS", 5))

        self.status_http_bind = env_str("STATUS_HTTP_BIND", "0.0.0.0")
        self.status_http_port = max(1, env_int("STATUS_HTTP_PORT", 8787))
        self.runtime_config_path = env_str("RUNTIME_CONFIG_PATH", "/opt/hashwatcher-hub-pi/runtime_config.json")

        self.agent_id = env_str("AGENT_ID", socket.gethostname())
        self.paired_device_type = ""
        self.paired_miner_mac = ""
        self.paired_miner_hostname = ""
        self.paired = bool(self.bitaxe_host.strip())
        self.user_subnet_cidr = ""
        self.automation_mode = DEFAULT_AUTOMATION_MODE
        self.experimental_features_enabled = False
        self.fleet_schedules: List[Dict[str, Any]] = []
        self.fleet_inventory: List[Dict[str, Any]] = []
        self.bitaxe_tune_by_mac: Dict[str, str] = {}

        self.config_lock = threading.Lock()
        self._automation_lock = threading.Lock()
        self._fleet_lock = threading.Lock()
        self._load_runtime_config()
        self._cpu_prev_total = 0
        self._cpu_prev_idle = 0
        total, idle = self._read_cpu_totals()
        if total is not None and idle is not None:
            self._cpu_prev_total = total
            self._cpu_prev_idle = idle

        self.session = requests.Session()
        self.state = HubState()
        self._update_progress: Dict[str, Any] = {"stage": "idle"}
        self._led_lock = threading.Lock()
        self._led_default_trigger: Optional[str] = None

        self._error_log: List[Dict[str, Any]] = []
        self._log_lock = threading.Lock()
        self._max_log_entries = 200
        self._consecutive_poll_failures = 0
        self._last_recovery_attempt_at_iso: Optional[str] = None
        self._last_recovery_attempt_at_ts: Optional[float] = None
        self._last_recovery_result: Optional[Dict[str, Any]] = None

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
            automation_mode = str(cfg.get("automationMode", DEFAULT_AUTOMATION_MODE)).strip()
            experimental_features_enabled = bool(cfg.get("experimentalFeaturesEnabled", False))
            fleet_schedules = cfg.get("fleetSchedules", [])
            fleet_inventory = cfg.get("fleetInventory", [])
            bitaxe_tune_by_mac = cfg.get("bitaxeTuneByMac", {})

            if bitaxe_host:
                self.bitaxe_host = bitaxe_host
            if isinstance(endpoints, list) and endpoints:
                self.endpoints = parse_endpoints(",".join(str(item) for item in endpoints if isinstance(item, str)))
            if isinstance(poll_seconds, int):
                self.poll_seconds = max(5, poll_seconds)
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
            self.automation_mode = self._normalize_automation_mode(automation_mode)
            self.experimental_features_enabled = bool(experimental_features_enabled)
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
                    if mac and tune in BITAXE_TUNE_PRESETS:
                        normalized_tunes[mac] = tune
                self.bitaxe_tune_by_mac = normalized_tunes
            self._hydrate_inventory_with_saved_bitaxe_tunes(self.fleet_inventory)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[{now_iso()}] WARNING: failed to load runtime config: {exc}", flush=True)

    def _persist_runtime_config(self) -> None:
        cfg = {
            "bitaxeHost": self.bitaxe_host,
            "endpoints": self.endpoints,
            "pollSeconds": self.poll_seconds,
            "deviceType": self.paired_device_type,
            "minerMac": self.paired_miner_mac,
            "minerHostname": self.paired_miner_hostname,
            "paired": self.paired,
            "userSubnetCIDR": self.user_subnet_cidr,
            "automationMode": self.automation_mode,
            "experimentalFeaturesEnabled": self.experimental_features_enabled,
            "fleetSchedules": self.fleet_schedules,
            "fleetInventory": self.fleet_inventory,
            "bitaxeTuneByMac": self.bitaxe_tune_by_mac,
            "updatedAtIso": now_iso(),
        }
        os.makedirs(os.path.dirname(self.runtime_config_path), exist_ok=True)
        with open(self.runtime_config_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f)
        try:
            os.chmod(self.runtime_config_path, 0o600)
        except Exception:
            pass

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

        step_num = 1
        step_name = "Connect to Wi-Fi"
        step_detail = "Join your network via BLE provisioning in the app."
        if not wifi_connected:
            pass
        elif not ts_authenticated:
            step_num = 2
            step_name = "Set up Tailscale"
            step_detail = f"Wi-Fi connected ({wifi.get('ssid', '?')}). Provide a Tailscale auth key to enable remote access. Generate one at login.tailscale.com \u2192 Settings \u2192 Keys."
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
                "automationMode": self.automation_mode,
                "experimentalFeaturesEnabled": self.experimental_features_enabled,
                "fleetSchedules": self.fleet_schedules,
                "fleetInventory": self.fleet_inventory,
                "bitaxeTuneByMac": self.bitaxe_tune_by_mac,
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
                    self.poll_seconds = max(5, int(updates["pollSeconds"]))
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
            if "automationMode" in updates:
                self.automation_mode = self._normalize_automation_mode(updates.get("automationMode"))

            self._persist_runtime_config()
            return self.get_runtime_config()

    def _normalize_automation_mode(self, value: Any) -> str:
        mode = str(value or "").strip().lower()
        if mode in HUB_AUTOMATION_PROFILES:
            return mode
        return DEFAULT_AUTOMATION_MODE

    def _automation_profile(self, mode: Optional[str] = None) -> Dict[str, Any]:
        mode_value = self._normalize_automation_mode(mode or self.automation_mode)
        return HUB_AUTOMATION_PROFILES[mode_value]

    def get_automation_status(self) -> Dict[str, Any]:
        with self.config_lock:
            mode = self._normalize_automation_mode(self.automation_mode)
            profile = self._automation_profile(mode)
        with self._automation_lock:
            failures = self._consecutive_poll_failures
            last_attempt_iso = self._last_recovery_attempt_at_iso
            last_result = dict(self._last_recovery_result) if isinstance(self._last_recovery_result, dict) else None
        return {
            "mode": mode,
            "modeLabel": profile["label"],
            "description": profile["description"],
            "autoRecoveryEnabled": profile["autoRecoveryEnabled"],
            "failureThreshold": profile["failureThreshold"],
            "cooldownSeconds": profile["cooldownSeconds"],
            "consecutiveFailures": failures,
            "lastRecoveryAttemptAtIso": last_attempt_iso,
            "lastRecovery": last_result,
            "supportedModes": [
                {
                    "id": mode_id,
                    "label": cfg["label"],
                    "description": cfg["description"],
                }
                for mode_id, cfg in HUB_AUTOMATION_PROFILES.items()
            ],
        }

    def set_automation_mode(self, mode: Any) -> Dict[str, Any]:
        if not self.experimental_features_enabled:
            return {
                "ok": False,
                "error": "Experimental features are disabled.",
            }
        requested = str(mode or "").strip().lower()
        if requested not in HUB_AUTOMATION_PROFILES:
            return {
                "ok": False,
                "error": f"Invalid automation mode '{requested or 'empty'}'.",
                "supportedModes": list(HUB_AUTOMATION_PROFILES.keys()),
            }
        normalized = requested
        with self.config_lock:
            self.automation_mode = normalized
            self._persist_runtime_config()
        with self._automation_lock:
            self._consecutive_poll_failures = 0
        status = self.get_automation_status()
        return {
            "ok": True,
            "message": f"Automation mode set to {status['modeLabel']}.",
            "automation": status,
        }

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
        if device_type not in {"bitaxe", "canaan"}:
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
        if saved_tune not in BITAXE_TUNE_PRESETS:
            saved_tune = self.bitaxe_tune_by_mac.get(mac, "") if mac else ""
        if saved_tune not in BITAXE_TUNE_PRESETS:
            saved_tune = ""

        supported_modes = (
            self._normalize_canaan_mode_options(raw.get("supportedWorkModes"), subtype)
            if device_type == "canaan"
            else []
        )

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
            if saved in BITAXE_TUNE_PRESETS:
                item["savedBitaxeTune"] = saved

    def _send_cgminer_json_command(self, target_ip: str, command: str, port: int = 4028) -> Dict[str, Any]:
        sock = None
        try:
            sock = socket.create_connection((target_ip, port), timeout=max(2.0, float(self.http_timeout_seconds)))
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

    def _probe_canaan_capabilities(self, target_ip: str) -> Dict[str, Any]:
        result = self._send_cgminer_json_command(target_ip, "version")
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
        if not self.experimental_features_enabled:
            return {"ok": False, "error": "Experimental features are disabled."}
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
                    if tune in BITAXE_TUNE_PRESETS:
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
        if action_type not in {"bitaxe_tune", "canaan_work_mode"}:
            return None

        mode_value_raw = raw.get("modeValue")
        mode_value = str(mode_value_raw).strip().lower()
        if action_type == "bitaxe_tune":
            if mode_value not in BITAXE_TUNE_PRESETS:
                return None
        else:
            if not self._is_valid_canaan_mode_value(mode_value):
                return None

        device_type = self._normalize_fleet_device_type(raw.get("deviceType"))
        if not device_type:
            device_type = "bitaxe" if action_type == "bitaxe_tune" else "canaan"

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

    def fleet_presets(self) -> Dict[str, Any]:
        return {
            "bitaxeTunes": [
                {
                    "id": key,
                    "label": preset["label"],
                    "frequency": preset["frequency"],
                    "coreVoltage": preset["coreVoltage"],
                }
                for key, preset in BITAXE_TUNE_PRESETS.items()
            ],
            "canaanWorkModes": [
                {"id": key, "label": label}
                for key, label in CANAAN_WORK_MODE_OPTIONS.items()
            ],
        }

    def get_fleet_manager_status(self) -> Dict[str, Any]:
        with self._fleet_lock:
            schedules = [dict(item) for item in self.fleet_schedules]
            inventory = [dict(item) for item in self.fleet_inventory]
            bitaxe_tune_by_mac = dict(self.bitaxe_tune_by_mac)
        return {
            "clock": self.local_clock_status(),
            "schedules": schedules,
            "presets": self.fleet_presets(),
            "inventory": inventory,
            "bitaxeTuneByMac": bitaxe_tune_by_mac,
        }

    def set_fleet_schedules(self, schedules: Any) -> Dict[str, Any]:
        if not self.experimental_features_enabled:
            return {"ok": False, "error": "Experimental features are disabled."}
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
                if target_mac and mode_value in BITAXE_TUNE_PRESETS:
                    self.bitaxe_tune_by_mac[target_mac] = mode_value
            self._hydrate_inventory_with_saved_bitaxe_tunes(self.fleet_inventory)

        with self.config_lock:
            self._persist_runtime_config()

        return {
            "ok": True,
            "message": f"Saved {len(sanitized)} fleet schedule(s).",
            "fleetManager": self.get_fleet_manager_status(),
        }

    def _schedule_includes_weekday(self, days_mask: int, weekday_index: int) -> bool:
        return (days_mask & (1 << weekday_index)) != 0

    def _apply_bitaxe_tune(self, target_ip: str, tune_id: str, target_mac: str = "") -> Dict[str, Any]:
        preset = BITAXE_TUNE_PRESETS.get(tune_id)
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
                "targetMac": normalize_mac(target_mac) or None,
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
        return {"ok": False, "error": f"Unsupported action type '{action_type}'."}

    def _run_fleet_manager_if_needed(self) -> None:
        if not self.experimental_features_enabled:
            return

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

                if result.get("ok") and schedule.get("actionType") == "bitaxe_tune" and resolved_mac:
                    applied_tune = str(result.get("appliedTuneId") or schedule.get("modeValue") or "").strip().lower()
                    if applied_tune in BITAXE_TUNE_PRESETS:
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

    def _restart_paired_miner(self) -> Dict[str, Any]:
        host = self.bitaxe_host.strip()
        if not host:
            return {"ok": False, "error": "No paired miner host configured."}

        restart_paths = ["/api/system/restart", "/system/restart"]
        last_error = "Unable to restart paired miner."
        for path in restart_paths:
            url = f"{self.bitaxe_scheme}://{host}{path}"
            try:
                response = self.session.post(url, timeout=max(3.0, float(self.http_timeout_seconds)))
                if response.ok:
                    return {
                        "ok": True,
                        "action": "restart_paired_miner",
                        "host": host,
                        "endpoint": path,
                        "statusCode": response.status_code,
                        "message": "Restart command sent to paired miner.",
                    }
                last_error = f"Restart endpoint returned HTTP {response.status_code}."
            except Exception as exc:  # pylint: disable=broad-except
                last_error = str(exc)
        return {
            "ok": False,
            "action": "restart_paired_miner",
            "host": host,
            "error": last_error,
        }

    def _run_automation_if_needed(self) -> None:
        with self.config_lock:
            mode = self._normalize_automation_mode(self.automation_mode)
            profile = self._automation_profile(mode)
        if not profile.get("autoRecoveryEnabled"):
            return

        if not self.paired or not self.bitaxe_host.strip():
            return

        failure_threshold = parse_int(profile.get("failureThreshold"), default=3, minimum=1)
        cooldown_seconds = parse_int(profile.get("cooldownSeconds"), default=300, minimum=30)
        now_ts = time.time()

        with self._automation_lock:
            failures = self._consecutive_poll_failures
            last_ts = self._last_recovery_attempt_at_ts

        if failures < failure_threshold:
            return
        if last_ts is not None and (now_ts - last_ts) < cooldown_seconds:
            return

        result = self._restart_paired_miner()
        attempt_iso = now_iso()
        with self._automation_lock:
            self._last_recovery_attempt_at_ts = now_ts
            self._last_recovery_attempt_at_iso = attempt_iso
            self._last_recovery_result = result

        if result.get("ok"):
            self._log(
                "automation",
                f"{mode}: recovery triggered after {failures} consecutive failures.",
                level="info",
            )
        else:
            detail = str(result.get("error") or "unknown error")
            self._log(
                "automation",
                f"{mode}: recovery attempt failed after {failures} consecutive failures: {detail}",
                level="warn",
            )

    def reset_pairing(self) -> Dict[str, Any]:
        with self.config_lock:
            self.bitaxe_host = ""
            self.paired = False
            self.paired_device_type = ""
            self.paired_miner_mac = ""
            self.paired_miner_hostname = ""
            self._persist_runtime_config()
            return self.get_runtime_config()

    def _purge_all_wifi_credentials(self) -> None:
        """Remove Wi-Fi credentials from all common Pi network config sources."""
        wifi_creds_path = os.getenv("LAST_WIFI_PATH", "/opt/hashwatcher-hub-pi/last_wifi_credentials.json")
        try:
            if os.path.exists(wifi_creds_path):
                os.remove(wifi_creds_path)
        except Exception:
            pass

        script = r"""
set -e

# Remove all known NetworkManager wireless profiles.
nmcli -t -f NAME,TYPE connection show 2>/dev/null | while IFS=: read -r name type; do
    if echo "$type" | grep -q wireless; then
        nmcli connection delete "$name" 2>/dev/null || true
    fi
done

# Clear runtime NM connection files as belt-and-suspenders.
rm -f /run/NetworkManager/system-connections/*wlan*.nmconnection 2>/dev/null || true
rm -f /run/NetworkManager/system-connections/*wireless*.nmconnection 2>/dev/null || true

# Remove netplan files containing Wi-Fi config.
for yf in /etc/netplan/*.yaml; do
    [ -f "$yf" ] || continue
    if grep -qE 'wifis:|access-points:' "$yf" 2>/dev/null; then
        rm -f "$yf"
    fi
done

# Strip the wifi stanza from /boot/firmware/network-config if present.
BOOT_CFG="/boot/firmware/network-config"
if [ -f "$BOOT_CFG" ]; then
    python3 -c "
import pathlib
p = pathlib.Path('$BOOT_CFG')
lines = p.read_text(encoding='utf-8', errors='ignore').splitlines(keepends=True)
out = []
skip = False
for line in lines:
    stripped = line.lstrip()
    if stripped.startswith('wifis:'):
        skip = True
        continue
    if skip:
        if line[:1] in (' ', '\t') or stripped == '':
            continue
        skip = False
    out.append(line)
p.write_text(''.join(out), encoding='utf-8')
"
fi

# Prevent cloud-init from re-writing network config on next boot.
mkdir -p /etc/cloud/cloud.cfg.d
echo "network: {config: disabled}" > /etc/cloud/cloud.cfg.d/99-disable-network-config.cfg

# Disconnect wlan0 and apply netplan if available.
nmcli device disconnect wlan0 2>/dev/null || true
netplan apply 2>/dev/null || true
"""
        try:
            subprocess.run(
                ["sudo", "bash", "-c", script],
                capture_output=True, text=True, timeout=30,
            )
        except Exception:
            pass

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

    def _schedule_sudo_command(self, command: List[str], delay_seconds: float, source: str) -> None:
        def _runner() -> None:
            time.sleep(delay_seconds)
            try:
                result = subprocess.run(command, capture_output=True, text=True, timeout=15)
                if result.returncode != 0:
                    detail = (result.stderr or result.stdout or "unknown error").strip()
                    self._log(source, f"Command failed ({' '.join(command)}): {detail}", level="error")
            except Exception as exc:  # pylint: disable=broad-except
                self._log(source, f"Command failed ({' '.join(command)}): {exc}", level="error")

        threading.Thread(target=_runner, daemon=True).start()

    def reboot_hub(self) -> Dict[str, Any]:
        self._schedule_sudo_command(["sudo", "-n", "reboot"], delay_seconds=2, source="device-action")
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
        self._purge_all_wifi_credentials()
        self._schedule_sudo_command(["sudo", "-n", "reboot"], delay_seconds=3, source="wifi-reset")

        return {"ok": True, "message": "Wi-Fi disconnected and credentials cleared. Hub will reboot in ~3 seconds. Re-provision via BLE."}

    def disconnect_and_reset_wifi(self) -> Dict[str, Any]:
        """Force disconnect wlan0 immediately, then clear all Wi-Fi credentials and reboot."""
        self._force_disconnect_wifi()
        self._purge_all_wifi_credentials()
        self._schedule_sudo_command(["sudo", "-n", "reboot"], delay_seconds=2, source="wifi-reset")

        return {
            "ok": True,
            "message": "Wi-Fi force-disconnected. Credentials cleared. Hub will reboot in ~2 seconds.",
        }

    def factory_reset(self) -> Dict[str, Any]:
        """Wipe all config, disconnect Tailscale, drop Wi-Fi, and reboot.

        Mimics a fresh-from-box power-on so the full onboarding can be tested.
        """
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

        # Disconnect Tailscale
        try:
            tailscale_setup.logout()
        except Exception:
            pass

        self._purge_all_wifi_credentials()
        self._schedule_sudo_command(["sudo", "-n", "reboot"], delay_seconds=3, source="factory-reset")

        return {"ok": True, "message": "Factory reset complete. Hub will reboot in ~3 seconds."}

    def self_check(self) -> Dict[str, Any]:
        ts_status = tailscale_setup.status()
        diag = tailscale_setup.diagnose()
        usb_diag = diag.get("usbGadget", {})

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
        }

        healthy = all([
            checks["wifiConnected"],
            checks["tailscaleAuthenticated"],
            not checks["tailscaleNodeNotFound"],
        ])
        return {
            "ok": healthy,
            "checks": checks,
            "findings": diag.get("findings", []),
            "checkedAtIso": now_iso(),
        }

    def check_for_update(self) -> Dict[str, Any]:
        """Query GitHub releases API for a newer version."""
        url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
        try:
            resp = requests.get(url, timeout=15, headers={"Accept": "application/vnd.github+json"})
            if resp.status_code == 404:
                return {"ok": True, "updateAvailable": False, "reason": "no releases published yet", "currentVersion": AGENT_VERSION}
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
        return dict(self._update_progress)

    def apply_update(self) -> Dict[str, Any]:
        """Start background download and install of the latest .deb from GitHub."""
        if self._update_progress.get("stage") in ("downloading", "installing", "restarting"):
            return {"ok": True, "message": "Update already in progress.", **self._update_progress}

        check = self.check_for_update()
        if not check.get("ok"):
            return check
        if not check.get("updateAvailable"):
            return {"ok": True, "message": f"Already on latest version ({AGENT_VERSION})"}

        deb_info = check.get("debAsset")
        if not deb_info or not deb_info.get("downloadUrl"):
            return {"ok": False, "error": "No .deb asset found in the latest release"}

        self._update_progress = {
            "stage": "downloading",
            "percent": 0,
            "version": check["latestVersion"],
            "message": "Starting download...",
        }

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
                            self._update_progress = {
                                "stage": "downloading",
                                "percent": min(pct, 100),
                                "version": check["latestVersion"],
                                "message": f"Downloaded {downloaded // 1024} KB" + (f" / {total_size // 1024} KB" if total_size else ""),
                            }
            except Exception as exc:
                self._update_progress = {"stage": "failed", "percent": 0, "error": f"Download failed: {exc}"}
                return

            self._update_progress = {
                "stage": "installing",
                "percent": 100,
                "version": check["latestVersion"],
                "message": "Installing package...",
            }

            try:
                result = subprocess.run(
                    ["sudo", "dpkg", "-i", deb_path],
                    capture_output=True, text=True, timeout=60,
                )
                if result.returncode != 0:
                    stderr = result.stderr.strip() or result.stdout.strip()
                    self._update_progress = {"stage": "failed", "percent": 0, "error": f"dpkg install failed: {stderr}"}
                    return
            except Exception as exc:
                self._update_progress = {"stage": "failed", "percent": 0, "error": f"Install failed: {exc}"}
                return

            self._update_progress = {
                "stage": "restarting",
                "percent": 100,
                "version": check["latestVersion"],
                "message": f"Updated to {check['latestVersion']}. Restarting...",
                "previousVersion": AGENT_VERSION,
                "sha256": sha.hexdigest(),
            }

            time.sleep(2)
            try:
                subprocess.run(["sudo", "systemctl", "restart", "hashwatcher-hub-pi"],
                               capture_output=True, text=True, timeout=15)
            except Exception:
                pass

        threading.Thread(target=_run_update, daemon=True).start()
        return {"ok": True, "message": "Update started.", **self._update_progress}

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
        try:
            result = subprocess.run(
                ["vcgencmd", *args],
                capture_output=True,
                text=True,
                timeout=3,
                check=False,
            )
        except Exception:
            return None

        if result.returncode != 0:
            return None
        output = (result.stdout or "").strip()
        return output or None

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
                fleet_manager = agent.get_fleet_manager_status() if agent.experimental_features_enabled else None

                status = {
                    "ok": True,
                    "agentId": agent.agent_id,
                    "piHostname": agent.pi_hostname,
                    "agentVersion": AGENT_VERSION,
                    "hubVersion": AGENT_VERSION,
                    "statusApiVersion": STATUS_API_VERSION,
                    "bitaxeHost": agent.bitaxe_host,
                    "isPaired": agent.paired,
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
                    "automation": agent.get_automation_status() if agent.experimental_features_enabled else None,
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
                    with agent._log_lock:
                        entries = list(agent._error_log)
                    if level_filter:
                        entries = [e for e in entries if e.get("level") == level_filter]
                    self._send_json({
                        "ok": True,
                        "count": len(entries),
                        "entries": entries[-limit:],
                    })
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

                if parsed.path == "/api/automation/status":
                    if not agent.experimental_features_enabled:
                        self._send_json({"ok": False, "error": "Experimental features are disabled."}, status=403)
                        return
                    self._send_json({"ok": True, "automation": agent.get_automation_status()})
                    return

                if parsed.path == "/api/features/status":
                    self._send_json({
                        "ok": True,
                        "featureGates": agent.feature_gates_status(),
                        "clock": agent.local_clock_status(),
                    })
                    return

                if parsed.path == "/api/fleet/status":
                    if not agent.experimental_features_enabled:
                        self._send_json({"ok": False, "error": "Experimental features are disabled."}, status=403)
                        return
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

                    html = f"""
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
    pre {{ white-space: pre-wrap; background: rgba(0,0,0,0.4); border: 1px solid rgba(255,255,255,0.08); border-radius: 10px; padding: 10px; font-size: 0.85em; color: rgba(255,255,255,0.7); }}
    .brand-header {{ display: flex; align-items: center; gap: 10px; margin-bottom: 16px; font-size: 1.3em; font-weight: 700; }}
    .setup-card {{ background: rgba(0,204,102,0.08); border: 1px solid rgba(0,204,102,0.25); border-radius: 14px; padding: 20px; margin-bottom: 16px; text-align: center; }}
    .setup-card h3 {{ margin: 0 0 8px; color: #33e680; }}
    .setup-card p {{ margin: 0 0 14px; color: rgba(255,255,255,0.6); font-size: 0.9em; }}
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
      <pre id="ledActionStatus">{led_action_status}</pre>
    </div>

	    <div class="card">
	      <h3 style="margin:0 0 12px;">Maintenance</h3>
	      <div style="display:flex;gap:8px;flex-wrap:wrap;">
	        <button class="btn-secondary" onclick="runSelfCheck()">Run Self-Check</button>
	        <button class="btn-warn" onclick="hardResetWifi()">Disconnect + Reset Wi-Fi</button>
	        <button class="btn-warn" onclick="factoryReset()">Factory Reset</button>
	      </div>
      <pre id="actionStatus">No actions run yet.</pre>
      <p class="muted" style="font-size:0.8em;margin:10px 0 0;">API: <a href="/api/status">/api/status</a> &middot; <a href="/api/wifi/status">/api/wifi/status</a> &middot; <a href="/api/led/status">/api/led/status</a> &middot; <a href="/api/self-check">/api/self-check</a></p>
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
      if (!confirm('Factory Reset\\n\\nThis will clear ALL config (Wi-Fi, Tailscale, pairing) and restart the hub.\\n\\nContinue?')) return;
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
                    self._send_html(html)
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
                    "/api/automation/mode",
                    "/api/fleet/schedules",
                    "/api/fleet/inventory",
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
                    if parsed.path == "/api/automation/mode":
                        if not agent.experimental_features_enabled:
                            self._send_json({"ok": False, "error": "Experimental features are disabled."}, status=403)
                            return
                        result = agent.set_automation_mode(payload.get("mode"))
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/fleet/schedules":
                        if not agent.experimental_features_enabled:
                            self._send_json({"ok": False, "error": "Experimental features are disabled."}, status=403)
                            return
                        result = agent.set_fleet_schedules(payload.get("schedules"))
                        http_status = 200 if result.get("ok") else 400
                        self._send_json(result, status=http_status)
                        return
                    if parsed.path == "/api/fleet/inventory":
                        if not agent.experimental_features_enabled:
                            self._send_json({"ok": False, "error": "Experimental features are disabled."}, status=403)
                            return
                        inventory_payload = payload.get("inventory")
                        result = agent.set_fleet_inventory(inventory_payload)
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
                        self._log("self-heal", f"Auto-repairing USB gadget: {issue}", level="info")
                        result = tailscale_setup.repair_usb_gadget()
                        if result.get("ok"):
                            self._log("self-heal", f"USB gadget repair succeeded: {result.get('actionsTaken', [])}", level="info")
                        else:
                            self._log("self-heal", f"USB gadget repair had errors: {result.get('errors', [])}", level="warn")

                    elif component == "tailscale":
                        self._log("self-heal", f"Tailscale issue detected: {issue} — {finding.get('message', '')}", level="warn")

            except Exception as exc:  # pylint: disable=broad-except
                self._log("self-heal", f"Self-heal loop error: {exc}")

            time.sleep(HEAL_INTERVAL)

    def run(self) -> None:
        print(
            f"Starting gateway agent='{self.agent_id}' piHostname='{self.pi_hostname}' bitaxe='{self.bitaxe_host}' poll={self.poll_seconds}s",
            flush=True,
        )
        self.start_status_server()

        threading.Thread(target=self._self_heal_loop, daemon=True, name="self-heal").start()

        while True:
            try:
                if self.paired and self.bitaxe_host.strip():
                    result = self.fetch_paired_miner()
                    if result:
                        normalized = self.normalize(result["data"])
                        self.state.set_poll_success({"normalized": normalized, "raw": result["data"]})
                        with self._automation_lock:
                            self._consecutive_poll_failures = 0
                    else:
                        msg = f"Miner {self.bitaxe_host} unreachable"
                        self.state.set_poll_error(msg)
                        with self._automation_lock:
                            self._consecutive_poll_failures += 1
                        self._run_automation_if_needed()
                        self._log("poll", msg, level="warn")
                else:
                    with self._automation_lock:
                        self._consecutive_poll_failures = 0

                self._run_fleet_manager_if_needed()
            except Exception as exc:  # pylint: disable=broad-except
                self.state.set_poll_error(str(exc))
                with self._automation_lock:
                    self._consecutive_poll_failures += 1
                self._run_automation_if_needed()
                self._run_fleet_manager_if_needed()
                self._log("poll", str(exc))

            time.sleep(self.poll_seconds)


if __name__ == "__main__":
    HubAgent().run()
