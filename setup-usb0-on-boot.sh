#!/bin/sh
# Run from cloud-init or first boot to assign 169.254.75.1 to usb0 (USB gadget).
# Copy this file to the SD card boot partition as setup-usb0-on-boot.sh, then
# add to user-data runcmd: - /bin/sh /boot/firmware/setup-usb0-on-boot.sh

set -e
SERVICE_FILE="/etc/systemd/system/usb0-gadget.service"
mkdir -p "$(dirname "$SERVICE_FILE")"
cat > "$SERVICE_FILE" << 'EOF'
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
EOF
systemctl daemon-reload
systemctl enable usb0-gadget.service
systemctl start usb0-gadget.service
# Bring up now in case we're running from cloud-init before network.target
ip link set usb0 up 2>/dev/null || true
ip addr add 169.254.75.1/16 dev usb0 2>/dev/null || true
