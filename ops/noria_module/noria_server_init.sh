#!/usr/bin/env bash
set -eo pipefail

sudo tee /etc/default/noria > /dev/null <<EOF
NORIA_DEPLOYMENT=${deployment}
ZOOKEEPER_URL=${zookeeper_ip}:2181
NORIA_MEMORY_BYTES=${noria_memory_bytes}
NORIA_QUORUM=${quorum}
NORIA_SHARDS=${shards}
EOF

block_device=/dev/$(curl http://169.254.169.254/latest/meta-data/block-device-mapping/ebs1)
if [ ! -f "$block_device" ]; then
    block_device="$${block_device/\/s/\/xv}"
fi
mkfs.ext4 "$block_device"
mount "$block_device" /var/lib/noria
echo "$block_device /var/lib/noria ext4 rw,discard,x-systemd.growfs 0 0" >> /etc/fstab

systemctl restart noria
