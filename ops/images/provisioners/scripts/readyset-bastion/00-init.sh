#!/bin/sh

# Repository install
sudo DEBIAN_FRONTEND=noninteractive apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get upgrade -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y auditd

# Configuration
sudo install -o root -g root -m 000550 \
  /tmp/readyset-bastion/etc_audit_audit.rules \
  /etc/audit/rules.d/audit.rules
