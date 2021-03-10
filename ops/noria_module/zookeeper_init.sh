#!/usr/bin/env bash
set -euo pipefail

block_device=/dev/$(curl http://169.254.169.254/latest/meta-data/block-device-mapping/ebs1)
if [ ! -f "$block_device" ]; then
    block_device="${block_device/\/s/\/xv}"
fi
mkfs.ext4 "$block_device"
mount "$block_device" /var/lib/zookeeper
echo "$block_device /var/lib/zookeeper ext4 rw,discard,x-systemd.growfs 0 0" >> /etc/fstab
systemctl restart zookeeper
