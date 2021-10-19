#!/bin/sh
set -eux

export PROMETHEUS_VERSION="2.30.3"
export PROMETHEUS_RELEASE="https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_VERSION}"

cd /tmp
curl -fSsLO "${PROMETHEUS_RELEASE}/prometheus-${PROMETHEUS_VERSION}.linux-amd64.tar.gz"

sed -i '/.*linux-amd64.tar.gz/!d' /tmp/prometheus/sha256sums.txt
sha256sum -c /tmp/prometheus/sha256sums.txt

tar zxvf prometheus-${PROMETHEUS_VERSION}.linux-amd64.tar.gz

sudo useradd --system --shell /usr/sbin/nologin prometheus

sudo install -o root -g root -m 755 \
  prometheus-${PROMETHEUS_VERSION}.linux-amd64/prometheus \
  /usr/local/bin/prometheus

sudo install -o root -g root -m 644 \
  /tmp/prometheus/etc_systemd_system_prometheus.service \
  /etc/systemd/system/prometheus.service

sudo install -o prometheus -g prometheus -m 755 \
  /tmp/prometheus/usr_local_bin_start-prometheus.sh \
  /usr/local/bin/start-prometheus.sh

sudo install -D -o prometheus -g prometheus -m 644 \
  /tmp/prometheus/etc_prometheus.d_prometheus.yaml \
  /etc/prometheus.d/prometheus.yaml

sudo install -d -o prometheus -g prometheus -m 755 \
  -p /var/lib/prometheus

systemd-analyze verify prometheus.service
sudo systemctl enable prometheus.service
