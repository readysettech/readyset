#!/usr/bin/env bash
set -euxo pipefail

sudo apt-get install -y nvme-cli

sudo install -o root -g root -m 755 \
    /tmp/ensure-ebs-volume-binary \
    /usr/local/bin/ensure-ebs-volume

ensure-ebs-volume --help # ensure it runs

sudo install -o root -g root -m 644 \
    /tmp/ensure-ebs-volume/etc_systemd_system_ensure-ebs-volume.service \
    /etc/systemd/system/ensure-ebs-volume.service

systemd-analyze verify ensure-ebs-volume.service
