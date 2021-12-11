#!/usr/bin/env bash
set -eux -o pipefail

sudo install -o root -g root -m 755 \
    /tmp/readyset-bastion/aws/usr_local_bin_user-data-init.sh \
    /usr/local/bin/user-data-init.sh

sudo install -o root -g root -m 744 \
    /tmp/readyset-bastion/aws/etc_fstab \
    /etc/fstab



sudo mount -o remount,rw,hidepid=2 /proc
sudo awk '!/proc/' /etc/fstab | sudo tee temp && sudo mv temp /etc/fstab
echo "proc /proc proc defaults,hidepid=2 0 0" >> sudo tee -a /etc/fstab