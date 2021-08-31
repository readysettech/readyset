#!/bin/sh

sudo install -o root -g root -m 644 \
  /tmp/tailscale-subnet-router/etc_sysctl.d_99-tailscale.conf
  /etc/sysctl.d/99-tailscale.conf

systemctl restart systemd-sysctl

UBUNTU_CODENAME=$(lsb_release -cs)
export UBUNTU_CODENAME
# Configure Tailscale Apt Repository
curl -fsSL "https://pkgs.tailscale.com/stable/ubuntu/${UBUNTU_CODENAME}.gpg" |\
  sudo gpg --dearmor -o /usr/share/keyrings/tailscale-keyring.gpg
echo \
  "deb [arch=amd64 signed-by=/usr/share/keyrings/tailscale-keyring.gpg]\
  https://pkgs.tailscale.com/stable/ubuntu ${UBUNTU_CODENAME} main" |\
  sudo tee /etc/apt/sources.list.d/tailscale.list > /dev/null

sudo DEBIAN_FRONTEND=noninteractive apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install tailscale
sudo systemctl enable --now tailscaled

sudo install -o root -g root -m 755 \
  /tmp/tailscale-subnet-router/usr_local_bin_tailscale-autoconnect.sh \
  /usr/local/bin/tailscale-autoconnect.sh

sudo install -o root -g root -m 644 \
  /tmp/tailscale-subnet-router/etc_systemd_system_tailscale-autoconnect.service \
  /etc/systemd/system/tailscale-autoconnect.service

systemd-analyze verify tailscale-autoconnect.service
sudo systemctl enable tailscale-autoconnect.service
