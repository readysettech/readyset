#!/bin/sh
set -eux

if [ -z "$HOSTNAME_PREFIX" ]; then
  echo "No hostname prefix set"
  exit 1
fi

sudo mkdir -p /usr/local/etc
echo "${HOSTNAME_PREFIX}" | sudo tee /usr/local/etc/hostname-prefix

sudo install -o root -g root -m 755 \
  /tmp/internal-base/usr_local_bin_set-hostname.sh \
  /usr/local/bin/set-hostname.sh

sudo install -o root -g root -m 644 \
  /tmp/internal-base/etc_systemd_system_set-hostname.service \
  /etc/systemd/system/set-hostname.service

systemd-analyze verify set-hostname.service
sudo systemctl enable set-hostname.service
