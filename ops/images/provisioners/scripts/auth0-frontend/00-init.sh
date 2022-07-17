#!/usr/bin/env bash
set -eux -o pipefail

curl -fsSL https://deb.nodesource.com/setup_17.x | sudo -E bash -
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y nodejs

sudo cp -r \
    /tmp/auth0-frontend-source \
    /opt/auth0-frontend
sudo chown -R root:root /opt/auth0-frontend

test -f /opt/auth0-frontend/package.json
sudo rm /opt/auth0-frontend/package-lock.json

pushd /opt/auth0-frontend
sudo npm install
sudo npm run build
popd

sudo install -o root -g root -m 644 \
    /tmp/auth0-frontend/etc_systemd_system_auth0-frontend.service \
    /etc/systemd/system/auth0-frontend.service

systemd-analyze verify auth0-frontend.service

sudo systemctl enable auth0-frontend