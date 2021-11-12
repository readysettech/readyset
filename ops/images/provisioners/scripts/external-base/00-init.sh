#!/bin/sh
set -eux

sudo DEBIAN_FRONTEND=noninteractive apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get upgrade -y

# Get absolute basics from the Ubuntu repository
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  apt-transport-https \
  ca-certificates \
  curl \
  gnupg \
  jq \
  lsb-release \
  software-properties-common \
  unattended-upgrades \
  mariadb-client-core-10.3 \
  unzip

# Enable auto upgrading packages for security updates
sudo install -o root -g root -m 644 \
  /tmp/external-base/etc_apt_apt.conf.d_20-auto-upgrades \
  /etc/apt/apt.conf.d/20-auto-upgrades

