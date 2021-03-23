#!/usr/bin/env bash
set -euo pipefail

find_nvme_device() {
    local block_dev="$1"
    apt-get -y update >&2
    apt-get -y install nvme-cli jq >&2
    nvme_devices=$(nvme list -o json | jq -r '.Devices | map(.DevicePath) | .[]')
    for nvme_dev in $nvme_devices; do
        ebs_block_dev=$(nvme id-ctrl -vb "$nvme_dev" 2>/dev/null | tr '\0' ' ' | cut -b 3073-3075)
        if [ "$ebs_block_dev" = "$block_dev" ]; then
            echo "$nvme_dev"
            return 0
        fi
    done
    >&2 echo "Could not find nvme device corresponding to block device $block_dev"
    return 1
}

systemctl stop zookeeper

block_device_name=$(curl http://169.254.169.254/latest/meta-data/block-device-mapping/ebs1)
if [ -f "/dev/$block_device_name" ]; then
    block_device="/dev/$block_device_name"
else
    with_xv="$${block_device_name/s/xv}"
    if [ -f "/dev/$with_xv" ]; then
        block_device="/dev/$with_xv"
    else
        block_device="$(find_nvme_device "$block_device_name")"
    fi
fi

mkfs.ext4 "$block_device"
mount "$block_device" /var/lib/zookeeper
echo "$block_device /var/lib/zookeeper ext4 rw,discard,x-systemd.growfs 0 0" >> /etc/fstab

systemctl reset-failed
systemctl restart zookeeper
