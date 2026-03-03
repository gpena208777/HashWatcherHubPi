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


def emit_pair_status(status: str) -> None:
    """Send a pair-progress status over BLE for iOS HubOnboardingView to display."""
    global _pair_status_value
    _pair_status_value = list(status.encode("utf-8"))
    if _ble_peripheral:
        try:
            _ble_peripheral.update_value(srv_id=1, chr_id=3)
        except Exception as exc:
            print(f"[{now_iso()}] BLE pair-status notify failed: {exc}", flush=True)


def update_ip_status(ip: Optional[str], port: Optional[int] = None) -> None:
    global _ip_status_value
    if ip:
        value = f"{ip}:{port}" if port and port != DEFAULT_HUB_PORT else ip
        _ip_status_value = list(value.encode("utf-8"))
    else:
        _ip_status_value = list(b"no-ip")
    if _ble_peripheral:
        try:
            _ble_peripheral.update_value(srv_id=1, chr_id=2)
        except Exception as exc:
            print(f"[{now_iso()}] BLE notify failed: {exc}", flush=True)


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
    for tick in range(45):
        time.sleep(1)
        if ip is None:
            ip = get_local_ip()
            if ip:
                print(f"[{now_iso()}] Wi-Fi IP acquired: {ip}", flush=True)
                emit_pair_status(PAIR_STATUS_WIFI_CONNECTED)
        if ip:
            port = _read_runtime_port()
            if _hub_port_ready(ip, port):
                print(f"[{now_iso()}] Hub HTTP ready on {ip}:{port}", flush=True)
                emit_pair_status(PAIR_STATUS_HUB_READY)
                update_ip_status(ip, port)
                _retry_ble_notify(retries=3, delay=0.5)
                return
    if ip:
        port = _read_runtime_port()
        print(f"[{now_iso()}] IP acquired ({ip}) but hub HTTP not ready after 45s — notifying anyway", flush=True)
        emit_pair_status(PAIR_STATUS_HUB_READY)
        update_ip_status(ip, port)
        _retry_ble_notify(retries=3, delay=0.5)
    else:
        print(f"[{now_iso()}] No IP acquired after 45s", flush=True)
        emit_pair_status(PAIR_STATUS_WIFI_FAILED)
        update_ip_status(None)


def _retry_ble_notify(retries: int = 3, delay: float = 0.5) -> None:
    """Re-send the BLE notify in case the first one was missed."""
    for i in range(retries):
        time.sleep(delay)
        if _ble_peripheral:
            try:
                _ble_peripheral.update_value(srv_id=1, chr_id=2)
            except Exception as exc:
                print(f"[{now_iso()}] BLE re-notify {i+1}/{retries} failed: {exc}", flush=True)


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def safe_shell(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def has_cmd(name: str) -> bool:
    return safe_shell(["bash", "-lc", f"command -v {name} >/dev/null 2>&1"]).returncode == 0


def save_wifi_marker(ssid: str) -> None:
    payload = {
        "ssid": ssid,
        "updatedAtIso": now_iso(),
    }
    os.makedirs(os.path.dirname(LAST_WIFI_PATH), exist_ok=True)
    with open(LAST_WIFI_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    os.chmod(LAST_WIFI_PATH, 0o600)


def apply_wifi_nmcli(ssid: str, password: str) -> bool:
    if not has_cmd("nmcli"):
        return False
    result = safe_shell(["nmcli", "dev", "wifi", "connect", ssid, "password", password, "ifname", WIFI_INTERFACE])
    if result.returncode == 0:
        return True
    # fallback without explicit interface
    result = safe_shell(["nmcli", "dev", "wifi", "connect", ssid, "password", password])
    return result.returncode == 0


def apply_wifi_wpa_supplicant(ssid: str, password: str) -> bool:
    wpa_conf = "/etc/wpa_supplicant/wpa_supplicant.conf"
    if not os.path.exists(wpa_conf):
        return False

    escaped_ssid = ssid.replace("\\", "\\\\").replace('"', '\\"')
    escaped_psk = password.replace("\\", "\\\\").replace('"', '\\"')

    network_block = (
        "\nnetwork={\n"
        f'    ssid="{escaped_ssid}"\n'
        f'    psk="{escaped_psk}"\n'
        "}\n"
    )

    with open(wpa_conf, "r", encoding="utf-8") as f:
        existing = f.read()

    if escaped_ssid not in existing:
        with open(wpa_conf, "a", encoding="utf-8") as f:
            f.write(network_block)

    if has_cmd("wpa_cli"):
        result = safe_shell(["wpa_cli", "-i", WIFI_INTERFACE, "reconfigure"])
        return result.returncode == 0

    if has_cmd("systemctl"):
        result = safe_shell(["systemctl", "restart", "wpa_supplicant"])
        return result.returncode == 0

    return False


def apply_wifi_credentials(ssid: str, password: str) -> bool:
    if apply_wifi_nmcli(ssid, password):
        return True
    return apply_wifi_wpa_supplicant(ssid, password)


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
    if not re.match(r"^[\x20-\x7E]+$", ssid):
        return None
    return ssid, password


def on_wifi_write(value: list[int], _options: dict) -> None:
    parsed = parse_payload(value)
    if not parsed:
        print(f"[{now_iso()}] BLE Wi-Fi payload invalid", flush=True)
        return

    ssid, password = parsed
    print(f"[{now_iso()}] BLE Wi-Fi provisioning received for SSID '{ssid}'", flush=True)
    emit_pair_status(PAIR_STATUS_CREDS_RECEIVED)
    save_wifi_marker(ssid)

    update_ip_status(None)

    ok = apply_wifi_credentials(ssid, password)
    if ok:
        print(f"[{now_iso()}] Wi-Fi apply succeeded for SSID '{ssid}'", flush=True)
        threading.Thread(target=_wait_for_ip_and_notify, daemon=True).start()
    else:
        print(f"[{now_iso()}] Wi-Fi apply failed for SSID '{ssid}'", flush=True)
        emit_pair_status(PAIR_STATUS_WIFI_FAILED)
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
    _ble_peripheral = ble
    ble.publish()
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
