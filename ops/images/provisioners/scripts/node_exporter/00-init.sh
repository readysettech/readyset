#!/bin/sh
set -eux

export NODE_EXPORTER_VERSION="1.1.2"
cd /tmp
curl -fSsLO "https://github.com/prometheus/node_exporter/releases/download/v${NODE_EXPORTER_VERSION}/node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64.tar.gz"
curl -fSsLo node_exporter_sha256sums.txt "https://github.com/prometheus/node_exporter/releases/download/v${NODE_EXPORTER_VERSION}/sha256sums.txt"
sed -i '/.*linux-amd64.tar.gz/!d' node_exporter_sha256sums.txt
sha256sum -c node_exporter_sha256sums.txt
tar zxvf node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64.tar.gz

sudo install -o root -g root -m 755 \
  node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64/node_exporter \
  /usr/local/bin/node_exporter

sudo useradd --system --shell /usr/sbin/nologin node_exporter

sudo install -o root -g root -m 644 \
  /tmp/node_exporter/etc_systemd_system_node_exporter.service \
  /etc/systemd/system/node_exporter.service

systemd-analyze verify node_exporter.service
sudo systemctl enable node_exporter.service
