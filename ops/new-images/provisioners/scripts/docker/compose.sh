#!/bin/sh
set -eux

export DOCKER_COMPOSE_VERSION="1.29.2"

cd /tmp
curl -fSsLO "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)"
curl -fSsLO "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m).sha256"
sha256sum -c "docker-compose-$(uname -s)-$(uname -m).sha256"
sudo install -o root -g root -m 755 \
  "docker-compose-$(uname -s)-$(uname -m)" \
  /usr/local/bin/docker-compose

sudo install -o root -g root -m 644 \
  /tmp/docker/etc_systemd_system_docker-compose.service \
  /etc/systemd/system/docker-compose.service \

systemd-analyze verify docker-compose.service
