#!/bin/sh
set -eux

sudo install -o root -g root -m 755 \
    /tmp/readyset-server-binary \
    /usr/local/bin/readyset-server

readyset-server --help # ensure it runs

sudo install -o root -g root -m 644 \
    /tmp/readyset-server/etc_systemd_system_readyset-server.service \
    /etc/systemd/system/readyset-server.service

sudo install -o root -g root -m 755 \
    /tmp/readyset-server/usr_local_bin_health-check.sh \
    /usr/local/bin/health-check.sh

sudo install -o root -g root -m 644 \
    /tmp/readyset-server/etc_crond_health_check \
    /etc/cron.d/health_check 

# needed to get ec2 instance name
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y cloud-utils

systemd-analyze verify readyset-server.service
