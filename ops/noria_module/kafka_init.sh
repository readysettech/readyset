#!/usr/bin/env bash
set -euo pipefail

systemctl stop kafka

sed -i \
    's/zookeeper.connect=localhost:2181/zookeeper.connect=${zookeeper_ip}:2181/' \
    /opt/kafka/config/server.properties

private_ip="$(curl http://169.254.169.254/latest/meta-data/local-ipv4)"

echo >> /opt/kafka/config/server.properties
echo "advertised.listeners=PLAINTEXT://$private_ip:9092" >> /opt/kafka/config/server.properties

systemctl reset-failed
systemctl restart kafka
