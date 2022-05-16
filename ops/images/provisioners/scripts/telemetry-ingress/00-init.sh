#!/usr/bin/env bash
set -eux -o pipefail

sudo install -o root -g root -m 755 \
    /tmp/telemetry-ingress-binary \
    /usr/local/bin/telemetry-ingress

telemetry-ingress --help

sudo install -o root -g root -m 644 \
    /tmp/telemetry-ingress/etc_systemd_system_telemetry-ingress.service \
    /etc/systemd/system/telemetry-ingress.service

systemd-analyze verify telemetry-ingress.service

sudo systemctl enable telemetry-ingress
