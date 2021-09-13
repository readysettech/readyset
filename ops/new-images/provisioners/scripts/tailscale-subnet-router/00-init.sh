#!/bin/sh

sudo install -o root -g root -m 644 \
  /tmp/tailscale-subnet-router/etc_sysctl.d_99-tailscale-subnet-router.conf \
  /etc/sysctl.d/99-tailscale-subnet-router.conf

sudo systemctl restart systemd-sysctl
