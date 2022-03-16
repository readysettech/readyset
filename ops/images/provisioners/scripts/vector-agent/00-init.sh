#!/bin/sh
set -eux

sudo install -D -o vector -g vector -m 644 \
  /tmp/vector-agent/etc_vector_vector.toml \
  /etc/vector/vector.toml

sudo install -D -o root -g root -m 755 \
  /tmp/vector-agent/usr_local_bin_configure-vector-agent.sh \
  /usr/local/bin/configure-vector-agent.sh
