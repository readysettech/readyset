#!/bin/sh
set -eux

sudo install -o root -g root -m 755 \
  /tmp/metrics-aggregator-binary \
  /usr/local/bin/metrics-aggregator

metrics-aggregator --help # ensure it runs

sudo install -o root -g root -m 644 \
  /tmp/metrics-aggregator/etc_systemd_system_metrics-aggregator.service \
  /etc/systemd/system/metrics-aggregator.service

systemd-analyze verify metrics-aggregator.service
