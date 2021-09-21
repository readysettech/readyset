#!/bin/sh
set -eux

sudo install -o root -g root -m 755 \
  /tmp/readyset-mysql-adapter/aws/usr_local_bin_cfn-init-wrapper.sh \
  /usr/local/bin/cfn-init-wrapper.sh

sudo install -o root -g root -m 755 \
  /tmp/readyset-mysql-adapter/aws/usr_local_bin_cfn-signal-wrapper.sh \
  /usr/local/bin/cfn-signal-wrapper.sh

sudo install -o root -g root -m 755 \
  /tmp/readyset-mysql-adapter/aws/usr_local_bin_user-data-init.sh \
  /usr/local/bin/user-data-init.sh
