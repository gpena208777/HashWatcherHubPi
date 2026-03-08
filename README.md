# HashWatcher Hub Pi

Turn a Raspberry Pi into a dedicated mining hub that monitors your ASIC rigs and makes them accessible from anywhere via Tailscale. Pair it with the free [HashWatcher app](https://www.HashWatcher.app) for iOS, macOS, and Android.

Follow us on [X/Twitter](https://x.com/HashWatcher).

---

## Install on a Raspberry Pi

### What You Need

- A **Raspberry Pi** (Pi Zero 2 W, Pi 4, or Pi 5)
- A **microSD card** (8 GB or larger)
- **Power supply** for your Pi
- Your Pi on the **same local network** as your miners

### Step 1: Flash Raspberry Pi OS

1. Download [Raspberry Pi Imager](https://www.raspberrypi.com/software/) on your computer
2. Insert your microSD card
3. Open Raspberry Pi Imager and choose:
   - **Device:** your Pi model
   - **OS:** Raspberry Pi OS Lite (64-bit)
   - **Storage:** your microSD card
4. Click the **gear icon** (or "Edit Settings") before writing and configure:
   - Set hostname: `HashWatcherHub`
   - Enable SSH: Use password authentication
   - Set username: `pi`
   - Set password: (choose your own)
   - Configure Wi-Fi: enter your SSID and password
   - Set locale: your timezone
5. Click **Write** and wait for it to finish

### Step 2: Boot the Pi

1. Insert the microSD card into your Pi
2. Plug in the power supply
3. Wait about 60 seconds for it to boot and connect to Wi-Fi

### Step 3: Install HashWatcher Hub Pi

From your computer (on the same network), open a terminal and run:

```bash
ssh pi@HashWatcherHub.local
```

Enter the password you set in Step 1. Then run the canonical installer:

```bash
curl -fsSL https://install.hashwatcher.app | sudo bash
```

This is the same installer used for both self-install and manual/SSH installs. It checks what is already present on the Pi and only installs what is missing, then installs or updates the hub app and services.

### Step 4: Set Up Tailscale

1. Open the hub dashboard in your browser: `http://HashWatcherHub.local:8787`
2. Follow the on-screen setup guide to connect Tailscale:
   - Get a free auth key from [login.tailscale.com/admin/settings/keys](https://login.tailscale.com/admin/settings/keys)
   - Enter it in the dashboard
   - Approve subnet routes in the [Tailscale Machines page](https://login.tailscale.com/admin/machines)
3. Install Tailscale on your phone from [tailscale.com/download](https://tailscale.com/download)
4. Disable key expiry (recommended) so the gateway stays connected permanently

### Step 5: Connect the HashWatcher App

1. Download the [HashWatcher app](https://www.HashWatcher.app) on your iPhone, Mac, or Android
2. The app will discover your hub automatically, or you can enter the IP manually
3. Your miners are now accessible from anywhere

---

## Installation Model

- Start from stock Raspberry Pi OS Lite flashed with Raspberry Pi Imager
- Run the single installer from GitHub
- Re-run that same installer later for updates
- If you install over SSH from another machine, that path should still invoke the same installer with a local source bundle

The installer is idempotent:

- it checks for required OS packages and only installs missing ones
- it checks whether Tailscale is already installed before installing it
- it reuses the existing Python virtual environment when possible
- it only reinstalls Python dependencies when `requirements.txt` changed
- it preserves your existing `hub.env`

## What Gets Installed

The installer sets up the following on your Pi:

| Component | Description |
|-----------|-------------|
| **hashwatcher-hub-pi** service | Main agent — polls miners, serves the web dashboard and API on port 8787 |
| **hashwatcher-ble-provisioner** service | BLE advertising — lets the HashWatcher app discover and configure the hub over Bluetooth |
| **Tailscale** | VPN tunnel for secure remote access (installed via official Tailscale installer) |
| **Python virtual environment** | Isolated Python environment at `/opt/hashwatcher-hub-pi/.venv` |
| **Config** | Environment config at `/etc/hashwatcher-hub-pi/hub.env` |

---

## What It Does

- **Miner polling** — fetches hashrate, temperature, power, and efficiency from your local miners every 10 seconds
- **Miner discovery** — scans your local subnet to find miners automatically
- **Web dashboard** — status page with Tailscale controls and guided setup at port 8787
- **REST API** — JSON API for the HashWatcher app
- **BLE provisioning** — the Pi advertises as `HashWatcherHub` over Bluetooth so the app can discover it and configure Wi-Fi
- **Built-in Tailscale** — secure remote access with subnet routing, no port forwarding needed
- **Key expiry monitoring** — warns you when your Tailscale key is about to expire
- **OTA updates** — optional update agent that checks for new versions automatically

---

## Frequently Asked Questions

### I already have Tailscale running on this Pi. Will the installer break it?

No. The installer detects that Tailscale is already installed and skips the Tailscale installation step. It uses the existing `tailscaled` service. However, when you set up the hub through the dashboard, it will run `tailscale up` with subnet routing and the hostname `HashWatcherHub`, which will modify your existing Tailscale session. If you're already using Tailscale on this Pi for something else, the hub will add subnet route advertising to your existing connection.

### What miners are supported?

Any miner with an HTTP API, including:
- **BitAxe** (all variants: Supra, Ultra, Gamma, Hex, etc.)
- **NerdQAxe / NerdAxe**
- **Canaan Avalon** (via CGMiner TCP protocol)
- **Any miner** reachable via HTTP on your local network

### Can I use a Pi Zero 2 W?

Yes. The Pi Zero 2 W has Wi-Fi and Bluetooth built in, making it a great low-power, low-cost hub. The installer works on any Pi running Raspberry Pi OS Lite (64-bit).

### Do I need to open any ports on my router?

No. Tailscale creates an encrypted peer-to-peer tunnel. No port forwarding or dynamic DNS needed.

### How do I update the hub?

SSH into the Pi and re-run the installer. It preserves your existing config:

```bash
ssh pi@HashWatcherHub.local
curl -fsSL https://install.hashwatcher.app | sudo bash
```

### How do I check the logs?

SSH into the Pi using the hostname (mDNS) or the IP you got from BLE/onboarding (use the username you set in Imager):

```bash
# By hostname (same network)
ssh hashwatcherhub@HashWatcherHub.local

# Or by IP (e.g. the IP the app discovered via BLE or showed in onboarding)
ssh hashwatcherhub@<hub-ip>
```

Then run:

```bash
# Hub agent logs (API, /api/status, Tailscale setup) — use this when iOS onboarding says "Could not reach the hub" on the Connect Tailscale step
journalctl -u hashwatcher-hub-pi -n 200 --no-pager

# Follow hub logs live
journalctl -u hashwatcher-hub-pi -f

# BLE provisioner logs
journalctl -u hashwatcher-ble-provisioner -f

# Tailscale daemon logs
journalctl -u tailscaled -n 100 --no-pager
tailscale status
```

If you see **`PollNetMap: initial fetch failed 404: node not found`** in `tailscaled` logs, the Pi’s node is no longer valid (e.g. key expired or node removed). Fix: on the Pi run `tailscale logout`, then in the app or dashboard run Tailscale setup again with a **new auth key** from [Tailscale admin](https://login.tailscale.com/admin/settings/keys) (disable key expiry for the hub).

### How do I restart the hub?

```bash
sudo systemctl restart hashwatcher-hub-pi
```

### How do I uninstall?

```bash
sudo systemctl stop hashwatcher-hub-pi hashwatcher-ble-provisioner
sudo systemctl disable hashwatcher-hub-pi hashwatcher-ble-provisioner
sudo rm -rf /opt/hashwatcher-hub-pi /etc/hashwatcher-hub-pi
sudo rm /etc/systemd/system/hashwatcher-hub-pi.service
sudo rm /etc/systemd/system/hashwatcher-ble-provisioner.service
sudo rm /etc/sudoers.d/hashwatcher-hub-pi
sudo systemctl daemon-reload
```

This does not uninstall Tailscale. To remove Tailscale: `sudo apt remove tailscale tailscaled`.

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/status` | Full gateway status (miner data, Tailscale, telemetry) |
| GET | `/api/config` | Current runtime config |
| POST | `/api/config` | Update miner pairing config |
| POST | `/api/reset` | Reset miner pairing |
| GET | `/api/discover` | Scan local subnet for miners |
| GET | `/api/discover?cidr=192.168.1.0/24` | Scan a specific subnet |
| GET | `/api/tailscale/status` | Tailscale connection info |
| POST | `/api/tailscale/setup` | Connect Tailscale with auth key |
| POST | `/api/tailscale/up` | Turn Tailscale on |
| POST | `/api/tailscale/down` | Turn Tailscale off |
| POST | `/api/tailscale/logout` | Disconnect and deauthorize |
| GET | `/api/network` | Local IP and subnet info |
| GET | `/api/miner/data` | Latest paired miner data |
| POST | `/api/miner/proxy` | Proxy a request to any miner by IP |

---

## BLE Provisioning

The hub advertises over Bluetooth as `HashWatcherHub` so the HashWatcher app can discover it without knowing the IP address.

| Property | Value |
|----------|-------|
| BLE device name | `HashWatcherHub` |
| Service UUID | `A8F0C001-2D4F-4B2A-8A9E-000000000001` |
| Wi-Fi characteristic UUID | `A8F0C001-2D4F-4B2A-8A9E-000000000002` |
| IP status characteristic UUID | `A8F0C001-2D4F-4B2A-8A9E-000000000003` |
| Pair status characteristic UUID | `A8F0C001-2D4F-4B2A-8A9E-000000000004` |
| Detailed status characteristic UUID | `A8F0C001-2D4F-4B2A-8A9E-000000000005` |
| Dedicated IP characteristic UUID | `A8F0C001-2D4F-4B2A-8A9E-000000000006` |

The app sends Wi-Fi credentials over BLE, and the hub connects to the specified network using NetworkManager (`nmcli`) or `wpa_cli`.

---

## File Layout on the Pi

```
/opt/hashwatcher-hub-pi/
├── hashwatcher_hub_agent.py   # Main hub agent
├── hub_ble_provisioner.py         # BLE Wi-Fi provisioner
├── tailscale_setup.py             # Tailscale CLI wrappers
├── requirements.txt               # Python dependencies
├── runtime_config.json            # Miner pairing state (auto-generated)
├── last_wifi_credentials.json     # Last Wi-Fi SSID (auto-generated)
└── .venv/                         # Python virtual environment

/etc/hashwatcher-hub-pi/
└── hub.env                        # Environment configuration
```

---

## Development

### Deploy changes to a Pi

```bash
cd pi-agent
./install_to_pi.sh 192.168.0.51 pi
```

### Quick file-only update (no full reinstall)

```bash
scp pi-agent/hashwatcher_hub_agent.py pi@HashWatcherHubPi.local:~/
ssh pi@HashWatcherHub.local 'sudo cp ~/hashwatcher_hub_agent.py /opt/hashwatcher-hub-pi/ && sudo systemctl restart hashwatcher-hub-pi'
```

### OTA Updates

Hub software updates are managed by the built-in update API in `hashwatcher_hub_agent.py`
(`GET /api/update/check`, `POST /api/update/apply`, `GET /api/update/status`).
