#!/bin/sh
set -eux

sudo install -o root -g root -m 755 \
  /tmp/readyset-psql-adapter-binary \
  /usr/local/bin/readyset-psql-adapter

readyset-psql-adapter --help # ensure it runs

sudo install -o root -g root -m 644 \
  /tmp/readyset-psql-adapter/etc_systemd_system_readyset-psql-adapter.service \
  /etc/systemd/system/readyset-psql-adapter.service

systemd-analyze verify readyset-psql-adapter.service
