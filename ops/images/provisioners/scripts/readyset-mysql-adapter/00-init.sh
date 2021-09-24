#!/bin/sh
set -eux

sudo install -o root -g root -m 755 \
  /tmp/readyset-mysql-adapter-binary \
  /usr/local/bin/readyset-mysql-adapter

sudo install -o root -g root -m 644 \
  /tmp/readyset-mysql-adapter/etc_systemd_system_readyset-mysql-adapter.service \
  /etc/systemd/system/readyset-mysql-adapter.service

systemd-analyze verify readyset-mysql-adapter.service
