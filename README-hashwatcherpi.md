# HashWatcher Hub Pi

Turn any Raspberry Pi into a mining hub that monitors your BitAxe, BitDSK, NerdQ, and other miners — with remote access via the [HashWatcher iOS app](https://www.hashwatcher.app).

## What it does

- **Discovers and monitors** miners on your local network
- **BLE Wi-Fi provisioning** — configure your Pi's Wi-Fi from the app (no monitor/keyboard needed)
- **Tailscale subnet routing** — access your miners remotely from anywhere
- **Web dashboard** on port 8787
- **OTA updates** — receives updates automatically

## Quick Install

Flash **Raspberry Pi OS Lite (64-bit)** onto your Pi using [Raspberry Pi Imager](https://www.raspberrypi.com/software/). Configure hostname `HashWatcherHub`, enable SSH, and set your user/password. Then:

```bash
ssh pi@HashWatcherHub.local
# Or, for pre-configured images: ssh hashwatcherhub@HashWatcherHub.local (password: 90218)

curl -fsSL https://raw.githubusercontent.com/gpena208777/HashWatcherHubPi/main/install.sh | sudo bash
```

The installer puts the hub software on the Pi. It is the same canonical installer used for self-install and manual SSH installs, and it only installs missing prerequisites before updating the hub app. **You still need the HashWatcher app to commission it**: connect over BLE, send Wi‑Fi credentials, add your Tailscale auth key, and pair miners. The app guides you through the full setup.

## Install via .deb package

Download the latest `.deb` from [Releases](https://github.com/gpena208777/HashWatcherHubPi/releases):

```bash
wget https://github.com/gpena208777/HashWatcherHubPi/releases/latest/download/hashwatcher-hub-pi_1.0.0_all.deb
sudo dpkg -i hashwatcher-hub-pi_1.0.0_all.deb
```

## After installation

The installer only gets the software onto the Pi. **Commissioning is done in the HashWatcher app:**

1. Download the **HashWatcher** app from the App Store
2. Open the app → Hub setup. It discovers your hub via BLE (advertises as `HashWatcherHub`)
3. Send Wi‑Fi credentials over BLE (no keyboard needed)
4. Add your Tailscale auth key for remote access
5. Pair your miners from the app

## Services

| Service | Description |
|---------|-------------|
| `hashwatcher-hub-pi` | Main agent — miner polling, web dashboard, API |
| `hashwatcher-ble-provisioner` | BLE Wi-Fi provisioning (no keyboard needed) |

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
