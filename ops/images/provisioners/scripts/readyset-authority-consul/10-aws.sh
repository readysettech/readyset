#!/bin/sh
set -eux

sudo install -D -o vector -g vector -m 644 \
    /tmp/readyset-authority-consul/aws/etc_vector_vector.toml \
    /etc/vector/vector.toml

sudo install -o root -g root -m 755 \
  /tmp/readyset-authority-consul/aws/usr_local_bin_user-data-init.sh \
  /usr/local/bin/user-data-init.sh
