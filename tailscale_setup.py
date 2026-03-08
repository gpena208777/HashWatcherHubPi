#!/usr/bin/env python3
"""Tailscale setup helpers for the Hashwatcher Pi gateway.

Provides functions to authenticate with a Tailscale auth key, enable subnet
routing, query connection status, and tear down the Tailscale session.
"""

import json
import os
import re
import shutil
import subprocess
from typing import Any, Dict, List, Optional


def _run(cmd: List[str], timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=timeout)


def is_installed() -> bool:
    # Avoid relying on `which`, which may be missing or PATH-dependent under systemd.
    if shutil.which("tailscale"):
        return True
    return any(
        os.path.exists(path)
        for path in (
            "/usr/bin/tailscale",
            "/usr/sbin/tailscale",
            "/bin/tailscale",
            "/sbin/tailscale",
        )
    )


def _tailscale_bin() -> str:
    resolved = shutil.which("tailscale")
    if resolved:
        return resolved
    for path in ("/usr/bin/tailscale", "/usr/sbin/tailscale", "/bin/tailscale", "/sbin/tailscale"):
        if os.path.exists(path):
            return path
    return "tailscale"


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


_setup_status: Dict[str, Any] = {"stage": "idle"}
_setup_lock = __import__("threading").Lock()


def _tailscale_up_cmd(
    *,
    hostname: str,
    advertise_routes: Optional[str] = None,
    auth_key: Optional[str] = None,
    reset: bool = False,
) -> List[str]:
    """Build a tailscale up command that preserves local LAN reachability.

    The hub acts as a subnet router; it should advertise its local subnet, but
    it should not accept routes from the tailnet because that can override the
    Pi's local routing and make the hub disappear from the LAN during onboarding.
    """
    cmd = [
        _tailscale_bin(),
        "up",
        f"--hostname={hostname}",
        "--accept-routes=false",
    ]
    if auth_key:
        cmd.append(f"--authkey={auth_key}")
    if advertise_routes:
        cmd.append(f"--advertise-routes={advertise_routes}")
    if reset:
        cmd.append("--reset")
    return cmd


def setup(auth_key: str, subnet_cidr: Optional[str] = None) -> Dict[str, Any]:
    """Authenticate Tailscale and enable subnet routing.

    Launches `tailscale up` in a background thread so the HTTP response returns
    immediately (< 2s) instead of blocking for 60-90s. The app can poll
    /api/tailscale/status or /api/tailscale/setup-status to track progress.

    Args:
        auth_key: A Tailscale auth key (tskey-auth-...).
        subnet_cidr: Optional subnet to advertise. Auto-detected if omitted.

    Returns:
        Dict with ok and immediate validation result. Actual connection
        happens asynchronously.
    """
    global _setup_status

    if not is_installed():
        return {"ok": False, "error": "tailscale is not installed on this gateway"}

    auth_key = auth_key.strip()
    if not auth_key:
        return {"ok": False, "error": "authKey is required"}
    if not auth_key.startswith("tskey-"):
        return {"ok": False, "error": "authKey must start with 'tskey-'"}

    resolved_cidr = (subnet_cidr or "").strip() or detect_subnet()
    if not resolved_cidr:
        return {
            "ok": False,
            "error": 'Could not detect local subnet. Enter your LAN subnet explicitly (e.g. 192.168.1.0/24 or 10.51.127.0/24).',
        }

    with _setup_lock:
        if _setup_status.get("stage") == "connecting":
            return {"ok": True, "message": "Tailscale setup already in progress.", "stage": "connecting"}

    import threading as _th

    def _do_setup() -> None:
        global _setup_status
        try:
            with _setup_lock:
                _setup_status = {"stage": "connecting", "startedAt": __import__("datetime").datetime.now().isoformat()}

            _ensure_ip_forwarding()
            _sudo_run(["systemctl", "start", "tailscaled"])

            ts_hostname = os.getenv("PI_HOSTNAME", "HashWatcherHub")

            # Keep the hub reachable on the local LAN while enabling subnet routing.
            # Accepting remote routes on the hub can steal local traffic and make the
            # onboarding app lose contact with the Pi mid-setup.
            cmd = _tailscale_up_cmd(
                hostname=ts_hostname,
                advertise_routes=resolved_cidr,
                auth_key=auth_key,
            )
            result = _sudo_run(cmd, timeout=90)
            if result.returncode != 0:
                stderr = result.stderr.strip() or result.stdout.strip()
                with _setup_lock:
                    _setup_status = {"stage": "error", "error": f"tailscale up failed: {stderr}"}
                return

            import time
            for i in range(10):
                time.sleep(2)
                s = status()
                if s.get("authenticated") and s.get("ip"):
                    with _setup_lock:
                        _setup_status = {
                            "stage": "connected",
                            "ip": s["ip"],
                            "hostname": s.get("hostname"),
                            "advertisedRoutes": [resolved_cidr],
                        }
                    return

            s = status()
            with _setup_lock:
                _setup_status = {
                    "stage": "connected" if s.get("authenticated") else "timeout",
                    "ip": s.get("ip"),
                    "hostname": s.get("hostname") or ts_hostname,
                    "advertisedRoutes": [resolved_cidr],
                    "note": "Tailscale may still be propagating. Check status in a few seconds.",
                }
        except Exception as exc:
            with _setup_lock:
                _setup_status = {"stage": "error", "error": str(exc)}

    _th.Thread(target=_do_setup, daemon=True, name="tailscale-setup").start()

    return {
        "ok": True,
        "message": "Tailscale setup started. Poll /api/tailscale/status for progress.",
        "stage": "connecting",
        "advertisedRoutes": [resolved_cidr],
    }


def get_setup_status() -> Dict[str, Any]:
    """Return the current async setup progress."""
    with _setup_lock:
        return {"ok": True, **_setup_status}


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

    result = _run([_tailscale_bin(), "status", "--json"])
    if result.returncode != 0:
        return info

    try:
        data = json.loads(result.stdout)
    except (json.JSONDecodeError, ValueError):
        return info

    backend_state = data.get("BackendState", "")
    health_messages = [str(item).strip() for item in (data.get("Health") or []) if str(item).strip()]
    health_text = " ".join(health_messages).lower()
    auth_url = str(data.get("AuthURL") or "").strip()
    info["running"] = backend_state != "Stopped"

    self_node = data.get("Self", {})
    self_online = bool(self_node.get("Online"))
    self_active = bool(self_node.get("Active"))
    tailscale_ips = self_node.get("TailscaleIPs", [])
    if tailscale_ips:
        info["ip"] = tailscale_ips[0]
    info["hostname"] = self_node.get("HostName")

    logged_out = (
        backend_state in {"NeedsLogin", "NoState"}
        or bool(auth_url)
        or "logged out" in health_text
        or "invalid key" in health_text
        or "not valid" in health_text
        or "requires authentication" in health_text
    )
    info["authenticated"] = info["running"] and not logged_out and bool(info["ip"])
    info["online"] = info["authenticated"] and (self_online or self_active)

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
    result = _sudo_run([_tailscale_bin(), "down"], timeout=15)
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
    state_result = _run([_tailscale_bin(), "status", "--json"])
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
    cmd = _tailscale_up_cmd(
        hostname=ts_hostname,
        advertise_routes=routes_str or None,
    )
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

    _sudo_run([_tailscale_bin(), "down"])
    result = _sudo_run([_tailscale_bin(), "logout"], timeout=15)
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        if "not logged in" not in stderr.lower():
            return {"ok": False, "error": f"tailscale logout failed: {stderr}"}

    return {"ok": True}

def _get_prefs() -> Optional[Dict[str, Any]]:
    """Read tailscale debug prefs for advertised routes."""
    result = _run([_tailscale_bin(), "debug", "prefs"])
    if result.returncode != 0:
        return None
    try:
        return json.loads(result.stdout)
    except (json.JSONDecodeError, ValueError):
        return None


# ── Diagnostics & Self-Repair ──────────────────────────────────────────

def _read_journalctl_tailscale(lines: int = 200) -> str:
    """Read recent tailscaled journal output."""
    result = _run(["journalctl", "-u", "tailscaled", "--no-pager", "-n", str(lines)], timeout=10)
    return result.stdout if result.returncode == 0 else ""


def _is_node_not_found() -> bool:
    """Detect the 'node not found' loop where the coordination server has forgotten this node."""
    logs = _read_journalctl_tailscale(100)
    if not logs:
        return False
    recent = logs.strip().splitlines()[-20:]
    hits = sum(1 for line in recent if "node not found" in line)
    return hits >= 3


def _is_needs_login() -> bool:
    result = _run([_tailscale_bin(), "status", "--json"])
    if result.returncode != 0:
        return False
    try:
        data = json.loads(result.stdout)
        return data.get("BackendState") == "NeedsLogin"
    except (json.JSONDecodeError, ValueError):
        return False


def diagnose() -> Dict[str, Any]:
    """Run diagnostics on Tailscale and USB gadget, returning actionable findings.

    Called by /api/diagnostics and the background self-heal loop.
    """
    findings: list = []
    ts = status()

    # Tailscale not installed
    if not ts.get("installed"):
        findings.append({
            "component": "tailscale",
            "severity": "critical",
            "issue": "not_installed",
            "message": "Tailscale is not installed.",
            "autoRepairable": False,
        })
        return {"ok": True, "healthy": False, "findings": findings, "tailscale": ts}

    # Tailscale node-not-found (stale node key — needs re-auth)
    node_not_found = _is_node_not_found()
    if node_not_found:
        findings.append({
            "component": "tailscale",
            "severity": "critical",
            "issue": "node_not_found",
            "message": (
                "Tailscale coordination server returns 'node not found'. "
                "The node was likely removed from the tailnet. "
                "Re-authenticate with a fresh auth key."
            ),
            "autoRepairable": True,
            "repairAction": "tailscale_reset",
        })

    # Tailscale key expired
    if ts.get("keyExpired"):
        findings.append({
            "component": "tailscale",
            "severity": "critical",
            "issue": "key_expired",
            "message": "Tailscale key has expired. Re-authorize the node or disable key expiry.",
            "autoRepairable": True,
            "repairAction": "tailscale_reset",
        })

    # Tailscale needs login
    if _is_needs_login() and not node_not_found:
        findings.append({
            "component": "tailscale",
            "severity": "critical",
            "issue": "needs_login",
            "message": "Tailscale requires authentication. Provide an auth key.",
            "autoRepairable": False,
        })

    # Tailscale not authenticated but no specific error
    if not ts.get("authenticated") and not findings:
        findings.append({
            "component": "tailscale",
            "severity": "warning",
            "issue": "not_authenticated",
            "message": "Tailscale is installed but not authenticated.",
            "autoRepairable": False,
        })

    # USB gadget
    usb_diag = diagnose_usb_gadget()
    findings.extend(usb_diag.get("findings", []))

    healthy = not any(f["severity"] == "critical" for f in findings)
    return {"ok": True, "healthy": healthy, "findings": findings, "tailscale": ts, "usbGadget": usb_diag}


def repair_tailscale(auth_key: Optional[str] = None) -> Dict[str, Any]:
    """Attempt to repair a broken Tailscale connection.

    For 'node not found': logout + re-auth with a new key.
    For expired keys: re-auth with a new key.
    If no auth_key is provided, can only do a logout+reset to clear stale state.
    """
    if not is_installed():
        return {"ok": False, "error": "Tailscale is not installed"}

    diag = diagnose()
    repairable = [f for f in diag.get("findings", []) if f.get("autoRepairable")]
    if not repairable:
        return {"ok": True, "message": "No repairable Tailscale issues found.", "diagnosis": diag}

    _sudo_run([_tailscale_bin(), "down"], timeout=10)
    _sudo_run([_tailscale_bin(), "logout"], timeout=10)

    if auth_key and auth_key.strip().startswith("tskey-"):
        result = setup(auth_key=auth_key.strip())
        return {
            "ok": result.get("ok", False),
            "action": "logout_and_reauth",
            "message": "Cleared stale state and started re-authentication. Poll /api/tailscale/status for progress.",
            "setupResult": result,
        }

    return {
        "ok": True,
        "action": "logout_only",
        "message": (
            "Cleared stale Tailscale state (logout). "
            "Provide an auth key via /api/tailscale/setup or the app to re-authenticate."
        ),
    }


# ── USB Gadget Diagnostics & Repair ───────────────────────────────────

def _usb0_exists() -> bool:
    result = _run(["ip", "link", "show", "usb0"])
    return result.returncode == 0


def _usb0_has_ip() -> bool:
    result = _run(["ip", "-4", "addr", "show", "usb0"])
    if result.returncode != 0:
        return False
    return "inet " in result.stdout


def _usb0_is_up() -> bool:
    result = _run(["ip", "link", "show", "usb0"])
    if result.returncode != 0:
        return False
    return "state UP" in result.stdout or ",UP" in result.stdout


def _gadget_service_active() -> bool:
    result = _run(["systemctl", "is-active", "usb0-gadget.service"])
    return result.returncode == 0 and "active" in result.stdout.strip()


def _gadget_service_exists() -> bool:
    return os.path.exists("/etc/systemd/system/usb0-gadget.service")


def diagnose_usb_gadget() -> Dict[str, Any]:
    """Check USB gadget mode health."""
    findings: list = []
    info: Dict[str, Any] = {
        "usb0Exists": _usb0_exists(),
        "usb0HasIp": _usb0_has_ip(),
        "usb0IsUp": _usb0_is_up(),
        "serviceExists": _gadget_service_exists(),
        "serviceActive": _gadget_service_active(),
    }

    if not info["usb0Exists"]:
        findings.append({
            "component": "usb_gadget",
            "severity": "warning",
            "issue": "usb0_missing",
            "message": (
                "USB gadget interface usb0 not found. "
                "Ensure dtoverlay=dwc2 is in config.txt and modules-load=dwc2,g_ether is in cmdline.txt, then reboot."
            ),
            "autoRepairable": False,
        })
    else:
        if not info["serviceExists"]:
            findings.append({
                "component": "usb_gadget",
                "severity": "warning",
                "issue": "service_missing",
                "message": "usb0-gadget.service not installed. USB gadget IP won't persist across reboots.",
                "autoRepairable": True,
                "repairAction": "install_usb_gadget_service",
            })
        elif not info["serviceActive"]:
            findings.append({
                "component": "usb_gadget",
                "severity": "warning",
                "issue": "service_inactive",
                "message": "usb0-gadget.service exists but is not active.",
                "autoRepairable": True,
                "repairAction": "start_usb_gadget_service",
            })

        if not info["usb0HasIp"]:
            findings.append({
                "component": "usb_gadget",
                "severity": "warning",
                "issue": "usb0_no_ip",
                "message": "usb0 exists but has no IP address. Mac cannot reach Pi over USB.",
                "autoRepairable": True,
                "repairAction": "assign_usb0_ip",
            })

    # Check for conflicting config.txt entry
    for base in ("/boot/firmware", "/boot"):
        cfg_path = os.path.join(base, "config.txt")
        if os.path.isfile(cfg_path):
            try:
                with open(cfg_path, "r") as f:
                    content = f.read()
                if "dtoverlay=dwc2,dr_mode=host" in content:
                    findings.append({
                        "component": "usb_gadget",
                        "severity": "warning",
                        "issue": "conflicting_dtoverlay",
                        "message": f"config.txt has 'dtoverlay=dwc2,dr_mode=host' which conflicts with gadget mode.",
                        "autoRepairable": True,
                        "repairAction": "fix_config_txt",
                    })
            except OSError:
                pass
            break

    info["findings"] = findings
    return info


USB_GADGET_IP = "169.254.75.1"
USB_GADGET_SERVICE = """\
[Unit]
Description=Bring up USB gadget ethernet (usb0) with fixed link-local IP
After=network-pre.target
Before=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/sh -c 'ip link set usb0 up 2>/dev/null; ip addr add 169.254.75.1/16 dev usb0 2>/dev/null || true'
ExecStop=/bin/sh -c 'ip addr flush dev usb0 2>/dev/null; ip link set usb0 down 2>/dev/null || true'

[Install]
WantedBy=multi-user.target
"""


def repair_usb_gadget() -> Dict[str, Any]:
    """Attempt to repair USB gadget mode issues found by diagnose_usb_gadget()."""
    diag = diagnose_usb_gadget()
    actions_taken: list = []
    errors: list = []

    for finding in diag.get("findings", []):
        if not finding.get("autoRepairable"):
            continue

        action = finding.get("repairAction")

        if action == "fix_config_txt":
            for base in ("/boot/firmware", "/boot"):
                cfg_path = os.path.join(base, "config.txt")
                if os.path.isfile(cfg_path):
                    try:
                        with open(cfg_path, "r") as f:
                            lines = f.readlines()
                        new_lines = [l for l in lines if "dtoverlay=dwc2,dr_mode=host" not in l]
                        result = _sudo_run(["tee", cfg_path], timeout=5)
                        _sudo_run(["bash", "-c", f"printf '%s' '{''.join(new_lines)}' > {cfg_path}"], timeout=5)
                        actions_taken.append("Removed conflicting dtoverlay=dwc2,dr_mode=host from config.txt (reboot needed)")
                    except Exception as exc:
                        errors.append(f"Failed to fix config.txt: {exc}")
                    break

        if action == "install_usb_gadget_service":
            try:
                svc_path = "/etc/systemd/system/usb0-gadget.service"
                proc = _sudo_run(["bash", "-c", f"cat > {svc_path} << 'SVCEOF'\n{USB_GADGET_SERVICE}SVCEOF"], timeout=5)
                if proc.returncode != 0:
                    errors.append(f"Failed to write service file: {proc.stderr}")
                else:
                    _sudo_run(["systemctl", "daemon-reload"])
                    _sudo_run(["systemctl", "enable", "usb0-gadget.service"])
                    _sudo_run(["systemctl", "start", "usb0-gadget.service"])
                    actions_taken.append("Installed and started usb0-gadget.service")
            except Exception as exc:
                errors.append(f"Failed to install gadget service: {exc}")

        if action == "start_usb_gadget_service":
            _sudo_run(["systemctl", "start", "usb0-gadget.service"])
            actions_taken.append("Started usb0-gadget.service")

        if action == "assign_usb0_ip":
            _sudo_run(["ip", "link", "set", "usb0", "up"])
            _sudo_run(["ip", "addr", "add", f"{USB_GADGET_IP}/16", "dev", "usb0"])
            actions_taken.append(f"Assigned {USB_GADGET_IP}/16 to usb0")

    return {
        "ok": len(errors) == 0,
        "actionsTaken": actions_taken,
        "errors": errors,
        "postRepairStatus": diagnose_usb_gadget(),
    }
