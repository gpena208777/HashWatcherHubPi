#!/usr/bin/env python3
import json
import os
import re
import socket
import subprocess
import threading
import time
from typing import Optional

from bluezero import adapter
from bluezero import peripheral


SERVICE_UUID = "A8F0C001-2D4F-4B2A-8A9E-000000000001"
WIFI_CHAR_UUID = "A8F0C001-2D4F-4B2A-8A9E-000000000002"
IP_STATUS_CHAR_UUID = "A8F0C001-2D4F-4B2A-8A9E-000000000003"
PAIR_STATUS_CHAR_UUID = "A8F0C001-2D4F-4B2A-8A9E-000000000004"
DETAIL_STATUS_CHAR_UUID = "A8F0C001-2D4F-4B2A-8A9E-000000000005"
IP_ONLY_CHAR_UUID = "A8F0C001-2D4F-4B2A-8A9E-000000000006"

# Keep one fixed BLE name so a single generic image works for all shipped hubs.
DEVICE_NAME = "HashWatcherHub"

# Pair status codes sent over BLE for iOS HubOnboardingView to display
PAIR_STATUS_CREDS_RECEIVED = "creds-received"
PAIR_STATUS_WIFI_CONNECTING = "wifi-connecting"
PAIR_STATUS_WIFI_CONNECTED = "wifi-connected"
PAIR_STATUS_HUB_READY = "hub-ready"
PAIR_STATUS_WIFI_FAILED = "wifi-failed"
LAST_WIFI_PATH = os.getenv("LAST_WIFI_PATH", "/opt/hashwatcher-hub-pi/last_wifi_credentials.json")
RUNTIME_PORT_PATH = os.getenv("RUNTIME_PORT_PATH", "/opt/hashwatcher-hub-pi/runtime_port")
DEFAULT_HUB_PORT = 8787
WIFI_INTERFACE = os.getenv("WIFI_INTERFACE", "wlan0")

_ble_peripheral: Optional[peripheral.Peripheral] = None
_ip_status_value: list[int] = list(b"waiting")
_pair_status_value: list[int] = list(b"idle")
_detail_status_value: list[int] = list(b'{"state":"idle"}')
_ip_only_value: list[int] = list(b"no-ip")


def get_local_ip() -> Optional[str]:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        sock.close()
        return ip
    except Exception:
        return None


def ip_status_read_callback() -> list[int]:
    return _ip_status_value


def pair_status_read_callback() -> list[int]:
    return _pair_status_value


def detail_status_read_callback() -> list[int]:
    return _detail_status_value


def ip_only_read_callback() -> list[int]:
    return _ip_only_value


def _notify_characteristic(chr_id: int, label: str) -> None:
    if not _ble_peripheral:
        return

    # Bluezero localGATT uses Characteristic.set_value(), not Peripheral.update_value().
    try:
        target = None
        for ch in getattr(_ble_peripheral, "characteristics", []):
            if getattr(ch, "path", "").endswith(f"char{chr_id:04d}"):
                target = ch
                break
        if not target:
            return

        if chr_id == 2:
            target.set_value(_ip_status_value)
        elif chr_id == 3:
            target.set_value(_pair_status_value)
        elif chr_id == 4:
            target.set_value(_detail_status_value)
        elif chr_id == 5:
            target.set_value(_ip_only_value)
    except Exception as exc:
        print(f"[{now_iso()}] BLE {label} notify failed: {exc}", flush=True)


def emit_pair_status(status: str) -> None:
    """Send a pair-progress status over BLE for iOS HubOnboardingView to display."""
    global _pair_status_value
    _pair_status_value = list(status.encode("utf-8"))
    _notify_characteristic(chr_id=3, label="pair-status")


def emit_detail_status(
    state: str,
    *,
    note: Optional[str] = None,
    ssid: Optional[str] = None,
    ip: Optional[str] = None,
    port: Optional[int] = None,
) -> None:
    """Publish richer onboarding telemetry over BLE for future app use."""
    global _detail_status_value
    payload = {
        "state": state,
        "updatedAtIso": now_iso(),
        "hostname": get_hostname(),
        "ip": ip,
        "port": port,
        "wifiInterface": WIFI_INTERFACE,
    }
    if note:
        payload["note"] = note
    if ssid:
        payload["ssid"] = ssid
    _detail_status_value = list(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    _notify_characteristic(chr_id=4, label="detail-status")


def get_hostname() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return DEVICE_NAME


def update_ip_status(ip: Optional[str], port: Optional[int] = None) -> None:
    global _ip_status_value, _ip_only_value
    if ip:
        hostname = get_hostname()
        port_str = f":{port}" if port and port != DEFAULT_HUB_PORT else ""
        value = f"{hostname}|{ip}{port_str}"
        _ip_status_value = list(value.encode("utf-8"))
        _ip_only_value = list(f"{ip}{port_str}".encode("utf-8"))
    else:
        _ip_status_value = list(b"no-ip")
        _ip_only_value = list(b"no-ip")
    _notify_characteristic(chr_id=2, label="ip-status")
    _notify_characteristic(chr_id=5, label="ip-only")


def _read_runtime_port() -> int:
    """Read the hub agent's actual bound port from the shared file."""
    try:
        with open(RUNTIME_PORT_PATH, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return DEFAULT_HUB_PORT


def _hub_port_ready(ip: str, port: int, timeout: float = 1.0) -> bool:
    """Return True if the hub HTTP server is accepting connections."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((ip, port))
        sock.close()
        return True
    except Exception:
        return False


def _wait_for_ip_and_notify() -> None:
    """Poll for a local IP *and* hub HTTP readiness for up to 45 seconds."""
    ip = None
    emit_pair_status(PAIR_STATUS_WIFI_CONNECTING)
    emit_detail_status("wifi-connecting")
    for tick in range(45):
        time.sleep(1)
        if ip is None:
            ip = get_local_ip()
            if ip:
                print(f"[{now_iso()}] Wi-Fi IP acquired: {ip}", flush=True)
                emit_pair_status(PAIR_STATUS_WIFI_CONNECTED)
                emit_detail_status("wifi-connected", ip=ip)
        if ip:
            port = _read_runtime_port()
            if _hub_port_ready(ip, port):
                print(f"[{now_iso()}] Hub HTTP ready on {ip}:{port}", flush=True)
                emit_pair_status(PAIR_STATUS_HUB_READY)
                emit_detail_status("hub-ready", ip=ip, port=port)
                update_ip_status(ip, port)
                _retry_ble_notify(retries=3, delay=0.5)
                return
            if tick > 5 and tick % 5 == 0:
                emit_detail_status("hub-waiting-http", ip=ip, port=port, note="ip-acquired-http-pending")
    if ip:
        port = _read_runtime_port()
        print(f"[{now_iso()}] IP acquired ({ip}) but hub HTTP not ready after 45s — notifying anyway", flush=True)
        # Do not claim the hub is ready until HTTP is actually reachable; the app
        # should keep treating this as an in-progress state instead of chasing a
        # stale or half-ready IP.
        emit_pair_status(PAIR_STATUS_WIFI_CONNECTED)
        emit_detail_status("hub-waiting-http", ip=ip, port=port, note="ip-acquired-http-pending")
        update_ip_status(ip, port)
        _retry_ble_notify(retries=3, delay=0.5)
    else:
        print(f"[{now_iso()}] No IP acquired after 45s", flush=True)
        emit_pair_status(PAIR_STATUS_WIFI_FAILED)
        emit_detail_status("wifi-failed", note="no-ip-after-timeout")
        update_ip_status(None)


def _retry_ble_notify(retries: int = 3, delay: float = 0.5) -> None:
    """Re-send the BLE notify in case the first one was missed."""
    for i in range(retries):
        time.sleep(delay)
        try:
            _notify_characteristic(chr_id=2, label=f"re-notify {i+1}/{retries}")
            _notify_characteristic(chr_id=5, label=f"ip-only re-notify {i+1}/{retries}")
        except Exception as exc:
            print(f"[{now_iso()}] BLE re-notify {i+1}/{retries} failed: {exc}", flush=True)


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def safe_shell(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def has_cmd(name: str) -> bool:
    return safe_shell(["bash", "-lc", f"command -v {name} >/dev/null 2>&1"]).returncode == 0


def _short_err(result: subprocess.CompletedProcess, limit: int = 160) -> str:
    msg = (result.stderr or result.stdout or "").strip()
    if not msg:
        return "unknown"
    msg = " ".join(msg.split())
    return msg[:limit]


def save_wifi_marker(ssid: str, password: str) -> None:
    payload = {
        "ssid": ssid,
        "password": password,
        "updatedAtIso": now_iso(),
    }
    os.makedirs(os.path.dirname(LAST_WIFI_PATH), exist_ok=True)
    with open(LAST_WIFI_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    os.chmod(LAST_WIFI_PATH, 0o600)


def load_wifi_marker() -> Optional[tuple[str, str]]:
    try:
        with open(LAST_WIFI_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return None
        ssid = str(data.get("ssid") or "").strip()
        password = str(data.get("password") or "")
        if not ssid:
            return None
        return ssid, password
    except Exception:
        return None


def apply_wifi_nmcli(ssid: str, password: str) -> tuple[bool, Optional[str]]:
    if not has_cmd("nmcli"):
        return False, "nmcli-not-installed"

    # Reset flow can leave Wi-Fi radio off; force it on before attempting connect.
    safe_shell(["nmcli", "radio", "wifi", "on"])
    safe_shell(["ip", "link", "set", WIFI_INTERFACE, "up"])
    safe_shell(["nmcli", "device", "set", WIFI_INTERFACE, "managed", "yes"])
    safe_shell(["nmcli", "device", "wifi", "rescan", "ifname", WIFI_INTERFACE])

    # Remove any existing profile for this SSID so credentials are always fresh.
    safe_shell(["nmcli", "connection", "delete", ssid])
    cmd = ["nmcli", "--wait", "25", "dev", "wifi", "connect", ssid]
    if password:
        cmd += ["password", password]
    cmd += ["ifname", WIFI_INTERFACE]
    result = safe_shell(cmd)
    if result.returncode == 0:
        return True, None

    fallback_cmd = ["nmcli", "--wait", "25", "dev", "wifi", "connect", ssid]
    if password:
        fallback_cmd += ["password", password]
    fallback = safe_shell(fallback_cmd)
    if fallback.returncode == 0:
        return True, None

    # Some NM versions create an invalid security block via "device wifi connect".
    # Build an explicit profile with key-mgmt set, then activate it.
    conn_name = f"hashwatcher-{ssid}"
    safe_shell(["nmcli", "connection", "delete", conn_name])
    add_cmd = [
        "nmcli",
        "connection",
        "add",
        "type",
        "wifi",
        "ifname",
        WIFI_INTERFACE,
        "con-name",
        conn_name,
        "ssid",
        ssid,
    ]
    add_result = safe_shell(add_cmd)
    if add_result.returncode != 0:
        return False, f"nmcli:{_short_err(add_result)}"

    if password:
        mod_result = safe_shell([
            "nmcli",
            "connection",
            "modify",
            conn_name,
            "wifi-sec.key-mgmt",
            "wpa-psk",
            "wifi-sec.psk",
            password,
        ])
    else:
        mod_result = safe_shell([
            "nmcli",
            "connection",
            "modify",
            conn_name,
            "wifi-sec.key-mgmt",
            "none",
        ])
    if mod_result.returncode != 0:
        return False, f"nmcli:{_short_err(mod_result)}"

    up_result = safe_shell(["nmcli", "--wait", "25", "connection", "up", conn_name])
    if up_result.returncode == 0:
        return True, None

    return False, f"nmcli:{_short_err(up_result)}"


def apply_wifi_wpa_supplicant(ssid: str, password: str) -> tuple[bool, Optional[str]]:
    wpa_conf = "/etc/wpa_supplicant/wpa_supplicant.conf"
    if not os.path.exists(wpa_conf):
        return False, "wpa-conf-missing"

    escaped_ssid = ssid.replace("\\", "\\\\").replace('"', '\\"')
    escaped_psk = password.replace("\\", "\\\\").replace('"', '\\"')

    network_block = (
        "\nnetwork={\n"
        + f'    ssid="{escaped_ssid}"\n'
        + (f'    psk="{escaped_psk}"\n' if password else "    key_mgmt=NONE\n")
        + "}\n"
    )

    with open(wpa_conf, "r", encoding="utf-8") as f:
        existing = f.read()

    if escaped_ssid in existing:
        # Remove the old network block for this SSID so we can write the updated one
        cleaned = re.sub(
            r'\nnetwork=\{[^}]*ssid="' + re.escape(escaped_ssid) + r'"[^}]*\}\n?',
            "",
            existing,
        )
        with open(wpa_conf, "w", encoding="utf-8") as f:
            f.write(cleaned)

    with open(wpa_conf, "a", encoding="utf-8") as f:
        f.write(network_block)

    if has_cmd("wpa_cli"):
        result = safe_shell(["wpa_cli", "-i", WIFI_INTERFACE, "reconfigure"])
        if result.returncode == 0:
            return True, None
        return False, f"wpa-cli:{_short_err(result)}"

    if has_cmd("systemctl"):
        result = safe_shell(["systemctl", "restart", "wpa_supplicant"])
        if result.returncode == 0:
            return True, None
        return False, f"wpa-systemctl:{_short_err(result)}"

    return False, "wpa-no-apply-method"


def apply_wifi_credentials(ssid: str, password: str) -> tuple[bool, Optional[str]]:
    ok, reason = apply_wifi_nmcli(ssid, password)
    if ok:
        return True, None
    ok2, reason2 = apply_wifi_wpa_supplicant(ssid, password)
    if ok2:
        return True, None
    if reason and reason2:
        return False, f"{reason}; {reason2}"
    return False, (reason or reason2 or "apply-failed")


def parse_payload(value: list[int]) -> Optional[tuple[str, str]]:
    try:
        raw = bytes(value).decode("utf-8", errors="strict")
        payload = json.loads(raw)
    except Exception:
        return None

    if not isinstance(payload, dict):
        return None

    ssid = str(payload.get("ssid") or "").strip()
    password = str(payload.get("password") or "")
    if not ssid:
        return None
    if len(ssid) > 64 or len(password) > 128:
        return None
    if any((ord(ch) < 0x20 or ord(ch) == 0x7F) for ch in ssid):
        return None
    return ssid, password


def _handle_command(raw: str) -> bool:
    """Handle special UTF-8 commands written to the Wi-Fi characteristic.
    Returns True if the value was a command (and was handled)."""
    cmd = raw.strip().lower()
    if cmd == "reboot":
        print(f"[{now_iso()}] BLE command: reboot", flush=True)
        emit_pair_status("rebooting")
        emit_detail_status("command-reboot")
        threading.Thread(target=_do_reboot, daemon=True).start()
        return True
    if cmd == "restart-services":
        print(f"[{now_iso()}] BLE command: restart-services", flush=True)
        emit_pair_status("restarting")
        emit_detail_status("command-restart-services")
        safe_shell(["sudo", "systemctl", "restart", "hashwatcher-hub-pi"])
        safe_shell(["sudo", "systemctl", "restart", "hashwatcher-ble-provisioner"])
        return True
    if cmd == "ping":
        print(f"[{now_iso()}] BLE command: ping", flush=True)
        emit_pair_status("pong")
        emit_detail_status("command-ping")
        return True
    return False


def _do_reboot() -> None:
    time.sleep(2)
    safe_shell(["sudo", "reboot"])


def on_wifi_write(value: list[int], _options: dict) -> None:
    try:
        raw = bytes(value).decode("utf-8", errors="strict")
    except Exception:
        print(f"[{now_iso()}] BLE write: decode failed", flush=True)
        emit_pair_status("payload-invalid")
        emit_detail_status("payload-invalid", note="utf8-decode-failed")
        return

    if _handle_command(raw):
        return

    parsed = parse_payload(value)
    if not parsed:
        print(f"[{now_iso()}] BLE Wi-Fi payload invalid", flush=True)
        emit_pair_status("payload-invalid")
        emit_detail_status("payload-invalid", note="json-schema-invalid")
        return

    ssid, password = parsed
    print(f"[{now_iso()}] BLE Wi-Fi provisioning received for SSID '{ssid}'", flush=True)
    emit_pair_status(PAIR_STATUS_CREDS_RECEIVED)
    emit_detail_status("creds-received", ssid=ssid)
    save_wifi_marker(ssid, password)

    update_ip_status(None)
    emit_pair_status("wifi-applying")
    emit_detail_status("wifi-applying", ssid=ssid)

    ok, reason = apply_wifi_credentials(ssid, password)
    if ok:
        print(f"[{now_iso()}] Wi-Fi apply succeeded for SSID '{ssid}'", flush=True)
        emit_detail_status("wifi-apply-succeeded", ssid=ssid)
        threading.Thread(target=_wait_for_ip_and_notify, daemon=True).start()
    else:
        reason_note = (reason or "apply-failed")
        print(f"[{now_iso()}] Wi-Fi apply failed for SSID '{ssid}': {reason_note}", flush=True)
        emit_pair_status(PAIR_STATUS_WIFI_FAILED)
        emit_detail_status("wifi-failed", ssid=ssid, note=reason_note)
        update_ip_status(None)


def find_adapter_address() -> str:
    adapters = list(adapter.Adapter.available())
    if not adapters:
        raise RuntimeError("No Bluetooth adapter found")
    return adapters[0].address


def ensure_adapter_powered(adapter_addr: str) -> None:
    """Power on adapter if needed. ExecStartPre does rfkill/hciconfig; this handles BlueZ state."""
    adapters = list(adapter.Adapter.available())
    for a in adapters:
        if a.address == adapter_addr:
            try:
                if not a.powered:
                    a.powered = True
                    time.sleep(1)
            except Exception:
                pass  # Adapter may already be up from ExecStartPre
            return
    raise RuntimeError(f"Adapter {adapter_addr} not found")


def main() -> None:
    adapter_addr = find_adapter_address()
    ensure_adapter_powered(adapter_addr)
    print(
        f"[{now_iso()}] Starting BLE provisioner on adapter={adapter_addr} name={DEVICE_NAME}",
        flush=True,
    )

    global _ble_peripheral
    ble = peripheral.Peripheral(adapter_address=adapter_addr, local_name=DEVICE_NAME, appearance=0)
    ble.add_service(srv_id=1, uuid=SERVICE_UUID, primary=True)
    ble.add_characteristic(
        srv_id=1,
        chr_id=1,
        uuid=WIFI_CHAR_UUID,
        value=[],
        notifying=False,
        flags=["write", "write-without-response"],
        write_callback=on_wifi_write,
    )
    ble.add_characteristic(
        srv_id=1,
        chr_id=2,
        uuid=IP_STATUS_CHAR_UUID,
        value=_ip_status_value,
        notifying=False,
        flags=["read", "notify"],
        read_callback=ip_status_read_callback,
    )
    ble.add_characteristic(
        srv_id=1,
        chr_id=3,
        uuid=PAIR_STATUS_CHAR_UUID,
        value=_pair_status_value,
        notifying=False,
        flags=["read", "notify"],
        read_callback=pair_status_read_callback,
    )
    ble.add_characteristic(
        srv_id=1,
        chr_id=4,
        uuid=DETAIL_STATUS_CHAR_UUID,
        value=_detail_status_value,
        notifying=False,
        flags=["read", "notify"],
        read_callback=detail_status_read_callback,
    )
    ble.add_characteristic(
        srv_id=1,
        chr_id=5,
        uuid=IP_ONLY_CHAR_UUID,
        value=_ip_only_value,
        notifying=False,
        flags=["read", "notify"],
        read_callback=ip_only_read_callback,
    )
    _ble_peripheral = ble
    ble.publish()
    emit_detail_status("ble-ready")

    def _reconnect_saved_wifi() -> None:
        """On boot, re-apply last known credentials so power cycles keep Wi-Fi working."""
        time.sleep(2)
        if get_local_ip():
            return
        saved = load_wifi_marker()
        if not saved:
            return
        ssid, password = saved
        print(f"[{now_iso()}] Boot reconnect using saved Wi-Fi SSID '{ssid}'", flush=True)
        emit_detail_status("wifi-reconnecting", ssid=ssid, note="boot-reconnect")
        ok, reason = apply_wifi_credentials(ssid, password)
        if ok:
            print(f"[{now_iso()}] Boot reconnect apply succeeded for SSID '{ssid}'", flush=True)
            emit_detail_status("wifi-apply-succeeded", ssid=ssid, note="boot-reconnect")
            threading.Thread(target=_wait_for_ip_and_notify, daemon=True).start()
        else:
            reason_note = reason or "boot-reconnect-failed"
            print(f"[{now_iso()}] Boot reconnect apply failed for SSID '{ssid}': {reason_note}", flush=True)
            emit_detail_status("wifi-failed", ssid=ssid, note=reason_note)

    threading.Thread(target=_reconnect_saved_wifi, daemon=True).start()

    def _advertise_existing_ip() -> None:
        """If the hub already has Wi-Fi, broadcast hostname + IP over BLE immediately
        so the app can skip the Wi-Fi provisioning step entirely."""
        for tick in range(60):
            time.sleep(1)
            ip = get_local_ip()
            if ip:
                port = _read_runtime_port()
                if _hub_port_ready(ip, port):
                    print(f"[{now_iso()}] Advertising existing IP: {get_hostname()}|{ip}:{port}", flush=True)
                    update_ip_status(ip, port)
                    emit_pair_status(PAIR_STATUS_HUB_READY)
                    emit_detail_status("hub-ready", ip=ip, port=port, note="existing-ip")
                    _retry_ble_notify(retries=3, delay=0.5)
                    return
                elif tick >= 10:
                    print(f"[{now_iso()}] IP available ({ip}) but hub HTTP not ready yet", flush=True)
                    update_ip_status(ip, port)
                    emit_detail_status("hub-waiting-http", ip=ip, port=port, note="existing-ip-http-pending")
                    _retry_ble_notify(retries=2, delay=0.3)
                    return

    threading.Thread(target=_advertise_existing_ip, daemon=True).start()

    # Re-broadcast IP every 30s so late-connecting apps always get the current address
    def _periodic_ip_broadcast() -> None:
        time.sleep(70)  # Wait for initial broadcast to finish
        while True:
            ip = get_local_ip()
            if ip:
                port = _read_runtime_port()
                update_ip_status(ip, port)
                emit_detail_status("periodic-ip", ip=ip, port=port)
            time.sleep(30)

    threading.Thread(target=_periodic_ip_broadcast, daemon=True).start()

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
