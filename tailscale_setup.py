#!/usr/bin/env python3
"""Tailscale setup helpers for the Hashwatcher Pi gateway.

Provides functions to authenticate with a Tailscale auth key, enable subnet
routing, query connection status, and tear down the Tailscale session.
"""

import json
import os
import re
import subprocess
from typing import Any, Dict, List, Optional


def _run(cmd: List[str], timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=timeout)


def is_installed() -> bool:
    result = _run(["which", "tailscale"])
    return result.returncode == 0


def _sudo_run(cmd: List[str], timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(["sudo"] + cmd, check=False, capture_output=True, text=True, timeout=timeout)


def _ensure_ip_forwarding() -> None:
    """Enable IPv4/IPv6 forwarding at runtime (persistent config handled by install script)."""
    _sudo_run(["sysctl", "-w", "net.ipv4.ip_forward=1"])
    _sudo_run(["sysctl", "-w", "net.ipv6.conf.all.forwarding=1"])


def detect_subnet(interface: str = "wlan0") -> Optional[str]:
    """Auto-detect the local subnet CIDR from a network interface.

    Falls back to a UDP-socket heuristic if the interface lookup fails.
    """
    result = _run(["ip", "-4", "-o", "addr", "show", interface])
    if result.returncode == 0 and result.stdout.strip():
        match = re.search(r"inet\s+(\d+\.\d+\.\d+\.\d+/\d+)", result.stdout)
        if match:
            cidr = match.group(1)
            parts = cidr.split("/")
            prefix = int(parts[1])
            octets = parts[0].split(".")
            if prefix <= 24:
                return f"{octets[0]}.{octets[1]}.{octets[2]}.0/24"
            return cidr

    # Fallback: determine local IP via UDP socket
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        sock.close()
        octets = ip.split(".")
        return f"{octets[0]}.{octets[1]}.{octets[2]}.0/24"
    except Exception:
        return None


def setup(auth_key: str, subnet_cidr: Optional[str] = None) -> Dict[str, Any]:
    """Authenticate Tailscale and enable subnet routing.

    Args:
        auth_key: A Tailscale auth key (tskey-auth-...).
        subnet_cidr: Optional subnet to advertise. Auto-detected if omitted.

    Returns:
        Dict with ok, ip, hostname, advertisedRoutes, and any error.
    """
    if not is_installed():
        return {"ok": False, "error": "tailscale is not installed on this gateway"}

    auth_key = auth_key.strip()
    if not auth_key:
        return {"ok": False, "error": "authKey is required"}
    if not auth_key.startswith("tskey-"):
        return {"ok": False, "error": "authKey must start with 'tskey-'"}

    _ensure_ip_forwarding()

    # Ensure tailscaled is running
    _sudo_run(["systemctl", "start", "tailscaled"])

    resolved_cidr = (subnet_cidr or "").strip() or detect_subnet()
    if not resolved_cidr:
        return {
            "ok": False,
            "error": 'Could not detect local subnet. Enter your LAN subnet explicitly (e.g. 192.168.1.0/24 or 10.51.127.0/24).',
        }

    ts_hostname = os.getenv("PI_HOSTNAME", "HashWatcherHub")

    cmd = [
        "tailscale", "up",
        f"--authkey={auth_key}",
        f"--advertise-routes={resolved_cidr}",
        f"--hostname={ts_hostname}",
        "--accept-routes",
        "--reset",
    ]
    result = _sudo_run(cmd, timeout=60)
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        return {"ok": False, "error": f"tailscale up failed: {stderr}"}

    import time
    for i in range(15):
        time.sleep(2 if i < 5 else 3)
        s = status()
        if s.get("authenticated") and s.get("ip"):
            return {"ok": True, "advertisedRoutes": [resolved_cidr], "ip": s["ip"], "hostname": s.get("hostname")}

    s = status()
    return {
        "ok": True,
        "advertisedRoutes": [resolved_cidr],
        "ip": s.get("ip"),
        "hostname": s.get("hostname") or ts_hostname,
        "note": "Tailscale connected but IP may still be propagating. Refresh in a few seconds.",
    }


def status() -> Dict[str, Any]:
    """Return current Tailscale connection status including key expiry."""
    from datetime import datetime, timezone

    info: Dict[str, Any] = {
        "installed": is_installed(),
        "running": False,
        "authenticated": False,
        "ip": None,
        "hostname": None,
        "advertisedRoutes": [],
        "online": False,
        "keyExpiry": None,
        "keyExpired": False,
        "keyExpiringSoon": False,
    }

    if not info["installed"]:
        return info

    result = _run(["tailscale", "status", "--json"])
    if result.returncode != 0:
        return info

    try:
        data = json.loads(result.stdout)
    except (json.JSONDecodeError, ValueError):
        return info

    backend_state = data.get("BackendState", "")
    info["running"] = backend_state != "Stopped"
    info["authenticated"] = backend_state == "Running"
    info["online"] = backend_state == "Running"

    self_node = data.get("Self", {})
    tailscale_ips = self_node.get("TailscaleIPs", [])
    if tailscale_ips:
        info["ip"] = tailscale_ips[0]
    info["hostname"] = self_node.get("HostName")

    key_expiry_raw = self_node.get("KeyExpiry")
    if key_expiry_raw:
        info["keyExpiry"] = key_expiry_raw
        try:
            expiry_str = key_expiry_raw.replace("Z", "+00:00")
            expiry_dt = datetime.fromisoformat(expiry_str)
            now = datetime.now(timezone.utc)
            info["keyExpired"] = expiry_dt <= now
            seven_days = 7 * 24 * 3600
            info["keyExpiringSoon"] = (not info["keyExpired"]
                                       and (expiry_dt - now).total_seconds() < seven_days)
        except (ValueError, TypeError):
            pass

    prefs = _get_prefs()
    if prefs:
        info["advertisedRoutes"] = prefs.get("AdvertiseRoutes", []) or []

    allowed_ips = self_node.get("AllowedIPs", [])
    advertised = info["advertisedRoutes"]
    if advertised and info["authenticated"]:
        approved = [r for r in advertised if r in allowed_ips]
        info["routesApproved"] = len(approved) == len(advertised)
        info["routesPending"] = len(approved) < len(advertised)
    else:
        info["routesApproved"] = False
        info["routesPending"] = False

    return info


def down() -> Dict[str, Any]:
    """Turn Tailscale off (stays authenticated; can turn back on without re-auth)."""
    if not is_installed():
        return {"ok": False, "error": "tailscale is not installed"}
    result = _sudo_run(["tailscale", "down"], timeout=15)
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "").strip()
        if "not connected" in stderr.lower() or "not running" in stderr.lower():
            return {"ok": True, "note": "Already off"}
        return {"ok": False, "error": stderr or "tailscale down failed"}
    return {"ok": True, "note": "Tailscale turned off. You can turn it back on anytime."}


def up() -> Dict[str, Any]:
    """Turn Tailscale back on using existing auth (no auth key needed).

    If Tailscale has been logged out, returns an error instead of hanging
    on interactive browser auth.
    """
    if not is_installed():
        return {"ok": False, "error": "tailscale is not installed"}

    current = status()
    if current.get("authenticated"):
        return {"ok": True, "note": "Already connected", "ip": current.get("ip"), "hostname": current.get("hostname")}

    # Check backend state — if NeedsLogin, there's no saved auth to resume
    state_result = _run(["tailscale", "status", "--json"])
    if state_result.returncode == 0:
        try:
            state_data = json.loads(state_result.stdout)
            backend = state_data.get("BackendState", "")
            if backend == "NeedsLogin":
                return {"ok": False, "error": "Not authenticated. Set up Tailscale with an auth key first (use the HashWatcher app)."}
        except (json.JSONDecodeError, ValueError):
            pass

    prefs = _get_prefs()
    routes = prefs.get("AdvertiseRoutes", []) or [] if prefs else []
    ts_hostname = os.getenv("PI_HOSTNAME", "HashWatcherHub")
    routes_str = ",".join(routes) if routes else ""
    _ensure_ip_forwarding()
    cmd = ["tailscale", "up", f"--hostname={ts_hostname}", "--accept-routes"]
    if routes_str:
        cmd.append(f"--advertise-routes={routes_str}")
    result = _sudo_run(cmd, timeout=30)
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "").strip()
        return {"ok": False, "error": stderr or "tailscale up failed"}
    s = status()
    return {"ok": True, "ip": s.get("ip"), "hostname": s.get("hostname")}


def logout() -> Dict[str, Any]:
    """Disconnect and deauthorize Tailscale on this gateway."""
    if not is_installed():
        return {"ok": False, "error": "tailscale is not installed"}

    _sudo_run(["tailscale", "down"])
    result = _sudo_run(["tailscale", "logout"], timeout=15)
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        if "not logged in" not in stderr.lower():
            return {"ok": False, "error": f"tailscale logout failed: {stderr}"}

    return {"ok": True}


def _status_fields() -> Dict[str, Any]:
    """Extract ip and hostname from tailscale status --json."""
    result = _run(["tailscale", "status", "--json"])
    if result.returncode != 0:
        return {}
    try:
        data = json.loads(result.stdout)
    except (json.JSONDecodeError, ValueError):
        return {}

    self_node = data.get("Self", {})
    tailscale_ips = self_node.get("TailscaleIPs", [])
    return {
        "ip": tailscale_ips[0] if tailscale_ips else None,
        "hostname": self_node.get("HostName"),
    }


def _get_prefs() -> Optional[Dict[str, Any]]:
    """Read tailscale debug prefs for advertised routes."""
    result = _run(["tailscale", "debug", "prefs"])
    if result.returncode != 0:
        return None
    try:
        return json.loads(result.stdout)
    except (json.JSONDecodeError, ValueError):
        return None
