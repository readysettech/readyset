#!/bin/sh
set -eux

sudo install -o root -g root -m 755 \
  /tmp/readyset-server/aws/usr_local_bin_user-data-init.sh \
  /usr/local/bin/user-data-init.sh
