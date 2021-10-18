#!/bin/sh
set -eux

cd /tmp

sudo useradd --system --shell /usr/sbin/nologin vector
sudo usermod -aG systemd-journal vector

sudo install -D -o vector -g vector -m 644 \
  /tmp/vector-agent/etc_vector.d_vector.toml \
  /etc/vector.d/vector.toml

sudo install -o vector -g vector -m 755 \
  /tmp/vector-agent/usr_local_bin_setup-vector.sh \
  /usr/local/bin/setup-vector.sh

sudo bash -c "echo 'PROMETHEUS_PORT=${PROMETHEUS_PORT}' > /etc/vector.d/env"
