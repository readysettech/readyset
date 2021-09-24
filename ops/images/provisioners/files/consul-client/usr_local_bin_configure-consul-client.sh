#!/usr/bin/env bash
set -euo pipefail

cat > /etc/consul.d/consul.hcl <<EOF
data_dir = "/opt/consul"
client_addr = "127.0.0.1"
server = false
retry_join = ["provider=aws tag_key=${CONSUL_TAG_KEY:-consul-server} tag_value=${CONSUL_TAG_VALUE}"]
EOF

systemctl reset-failed
systemctl enable consul.service
systemctl restart consul.service
