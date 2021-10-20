#!/bin/sh
set -eux

sudo install -o root -g root -m 755 \
    /tmp/readyset-server-binary \
    /usr/local/bin/readyset-server

readyset-server --help # ensure it runs

sudo install -o root -g root -m 644 \
    /tmp/readyset-server/etc_systemd_system_readyset-server.service \
    /etc/systemd/system/readyset-server.service

systemd-analyze verify readyset-server.service
