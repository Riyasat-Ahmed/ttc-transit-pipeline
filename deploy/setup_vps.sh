#!/usr/bin/env bash
# One-time setup for a fresh Ubuntu 24.04 VPS. Run as root:
#   curl -fsSL <raw url of this file> | bash      (or scp it over and run it)
set -euo pipefail

apt-get update
apt-get install -y ca-certificates curl git ufw unattended-upgrades

# Docker Engine + Compose plugin (official convenience script)
curl -fsSL https://get.docker.com | sh
systemctl enable --now docker   # Docker starts on boot; containers use restart: unless-stopped

# Firewall: SSH and HTTPS (plus 80 for the Let's Encrypt challenge) only.
ufw default deny incoming
ufw default allow outgoing
ufw allow OpenSSH
ufw allow 80/tcp
ufw allow 443/tcp
ufw --force enable

# 2 GB swap: headroom for the static GTFS load on a 4 GB box.
if ! swapon --show | grep -q /swapfile; then
  fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

echo "Done. Next: clone the repo, create .env, then"
echo "  docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build"
