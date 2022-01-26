#!/bin/bash
set -eux

# Configure Hashicorp Apt Repository
curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-keyring.gpg
echo \
  "deb [arch=amd64 signed-by=/usr/share/keyrings/hashicorp-keyring.gpg] https://apt.releases.hashicorp.com \
  $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list > /dev/null

# Install Consul
sudo DEBIAN_FRONTEND=noninteractive apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y consul=1.11.2

sudo install -o consul -g consul -m 644 /tmp/consul-client/etc_consul.d_consul.hcl /etc/consul.d/consul.hcl
sudo install -o root -g root -m 644 /tmp/consul-client/lib_systemd_system_consul.service /lib/systemd/system/consul.service

sudo systemctl enable consul.service
