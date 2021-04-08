#!/usr/bin/env bash
set -euo pipefail

find_nvme_device() {
    local block_dev="$1"
    apt-get -y update >&2
    apt-get -y install nvme-cli jq >&2
    nvme_devices=$(nvme list -o json | jq -r '.Devices | map(.DevicePath) | .[]')
    for nvme_dev in $nvme_devices; do
        # https://github.com/transferwise/ansible-ebs-automatic-nvme-mapping
        ebs_block_dev=$(nvme id-ctrl -vb "$nvme_dev" 2>/dev/null | cut -c3073-3104 | tr -s ' ' | sed 's/ $//g')
        if [ "$ebs_block_dev" = "$block_dev" ]; then
            echo "$nvme_dev"
            return 0
        fi
    done
    >&2 echo "Could not find nvme device corresponding to block device $block_dev"
    return 1
}

systemctl stop noria

block_device_name="${device_name}"
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

sudo tee /etc/default/noria > /dev/null <<EOF
NORIA_DEPLOYMENT=${deployment}
ZOOKEEPER_URL=${zookeeper_ip}:2181
NORIA_MEMORY_BYTES=${noria_memory_bytes}
NORIA_QUORUM=${quorum}
NORIA_SHARDS=${shards}
EOF

echo "Found NVME device at: $block_device"

mkfs.ext4 "$block_device"
mkdir -p /var/lib/noria
mount "$block_device" /var/lib/noria
UUID=$(lsblk -J -f $block_device | jq -r '.blockdevices[] | .uuid')
echo "UUID of NVME device at $block_device is $UUID"
echo "$UUID /var/lib/noria ext4 rw,discard,x-systemd.growfs 0 0" >> /etc/fstab

systemctl reset-failed
systemctl start noria
