# HashWatcher Hub Pi

Turn any Raspberry Pi into a mining hub that monitors your BitAxe, BitDSK, NerdQ, and other miners — with remote access via the [HashWatcher iOS app](https://www.hashwatcher.app).

## What it does

- **Discovers and monitors** miners on your local network
- **BLE Wi-Fi provisioning** — configure your Pi's Wi-Fi from the app (no monitor/keyboard needed)
- **Tailscale subnet routing** — access your miners remotely from anywhere
- **Web dashboard** on port 8787
- **OTA updates** — receives updates automatically

## Quick Install

Flash **Raspberry Pi OS Lite (64-bit)** onto your Pi using [Raspberry Pi Imager](https://www.raspberrypi.com/software/), then:

```bash
ssh pi@HashWatcherHub.local
curl -fsSL https://raw.githubusercontent.com/gpena208777/HashWatcherHubPi/main/install.sh | sudo bash
```

That's it. The installer handles everything: system dependencies, Tailscale, BLE provisioning, Python environment, and systemd services.

## Install via .deb package

Download the latest `.deb` from [Releases](https://github.com/gpena208777/HashWatcherHubPi/releases):

```bash
wget https://github.com/gpena208777/HashWatcherHubPi/releases/latest/download/hashwatcher-hub-pi_1.0.0_all.deb
sudo dpkg -i hashwatcher-hub-pi_1.0.0_all.deb
```

## After installation

1. Download the **HashWatcher** app from the App Store
2. The app will discover your hub via BLE (advertises as `HashWatcherHub`)
3. Pair your miners from the app
4. Set up Tailscale for remote access

## Services

| Service | Description |
|---------|-------------|
| `hashwatcher-hub-pi` | Main agent — miner polling, web dashboard, API |
| `hashwatcher-ble-provisioner` | BLE Wi-Fi provisioning (no keyboard needed) |
| `hashwatcher-update-agent` | OTA updates from HashWatcher backend |

```bash
# Check status
sudo systemctl status hashwatcher-hub-pi

# View logs
journalctl -u hashwatcher-hub-pi -f

# Restart
sudo systemctl restart hashwatcher-hub-pi
```

## Requirements

- Raspberry Pi 4 or 5 (64-bit OS)
- Raspberry Pi OS Lite (Bookworm recommended)
- Network connection (Ethernet or Wi-Fi)
- Bluetooth (built-in on Pi 4/5) for BLE provisioning

## Support

- App: [hashwatcher.app](https://www.hashwatcher.app)
- Issues: [GitHub Issues](https://github.com/gpena208777/HashWatcherHubPi/issues)
