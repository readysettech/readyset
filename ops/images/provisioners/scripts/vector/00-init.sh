#!/bin/sh
set -eux

export VECTOR_VERSION="0.16.1"
cd /tmp
curl -fSsLO "https://packages.timber.io/vector/${VECTOR_VERSION}/vector-${VECTOR_VERSION}-x86_64-unknown-linux-gnu.tar.gz"
tar zxvf vector-${VECTOR_VERSION}-x86_64-unknown-linux-gnu.tar.gz

sudo install -o root -g root -m 755 \
  vector-x86_64-unknown-linux-gnu/bin/vector \
  /usr/local/bin/vector

sudo install -o root -g root -m 755 \
  /tmp/vector/usr_local_bin_start-vector.sh \
  /usr/local/bin/start-vector.sh

sudo install -o root -g root -m 644 \
  /tmp/vector/etc_systemd_system_vector.service \
  /etc/systemd/system/vector.service

sudo install -d -o vector -g vector -m 644 \
  -p /var/lib/vector

systemd-analyze verify vector.service
sudo systemctl enable vector.service
