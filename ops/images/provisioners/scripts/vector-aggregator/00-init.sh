#!/bin/sh
set -eux

cd /tmp

sudo useradd --system --shell /usr/sbin/nologin vector
sudo install -D -o vector -g vector -m 644 \
  /tmp/vector-aggregator/etc_vector.d_vector.toml \
  /etc/vector.d/vector.toml

sudo install -D -o vector -g vector -m 644 \
  /tmp/vector-aggregator/etc_vector.d_vector.hcl \
  /etc/vector.d/vector.hcl

sudo install -o vector -g vector -m 755 \
  /tmp/vector-aggregator/usr_local_bin_setup-vector.sh \
  /usr/local/bin/setup-vector.sh

sudo install -o vector -g vector -m 755 \
  /tmp/vector-aggregator/usr_local_bin_check-vector.sh \
  /usr/local/bin/check-vector.sh
