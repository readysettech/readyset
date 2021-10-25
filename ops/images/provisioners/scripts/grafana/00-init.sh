#!/bin/sh
set -eux

sudo apt-get install -y apt-transport-https
sudo apt-get install -y software-properties-common wget
wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add -

echo "deb https://packages.grafana.com/enterprise/deb stable main" | sudo tee -a /etc/apt/sources.list.d/grafana.list

sudo DEBIAN_FRONTEND=noninteractive apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y grafana-enterprise

sudo install -o root -g root -m 644 \
  /tmp/grafana/etc_systemd_system_grafana-server.service \
  /etc/systemd/system/grafana-server.service

sudo install -o grafana -g grafana -m 644 \
  /tmp/grafana/etc_grafana_grafana.ini \
  /etc/grafana/grafana.ini

sudo cp -r /tmp/grafana/provisioning \
           /etc/grafana
sudo cp -r /tmp/grafana/dashboards \
           /etc/grafana

sudo systemctl daemon-reload
sudo systemctl start grafana-server.service
