#!/bin/sh
set -eux

# Configure Docker Apt Repository
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker
sudo DEBIAN_FRONTEND=noninteractive apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y docker-ce docker-ce-cli containerd.io

# Configure to use journald logging and expose a metrics endpoint
sudo install -o root -g root -m 644 /tmp/docker/etc_docker_daemon.json /etc/docker/daemon.json
sudo systemctl enable --now docker.service
sudo systemctl enable --now containerd.service

# Enable the default user to use docker without sudo
sudo usermod -aG docker "$USER"
newgrp docker
